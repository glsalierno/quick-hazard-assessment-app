"""
Unified chemical assessment: typed CAS/name, example dict, or SDS PDF → same DB pipeline.

The Streamlit UI still uses session keys (`query`, `result_data`); this module centralizes
fetch logic so all paths share one implementation.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, List, Optional, Union

import streamlit as st

from utils import cas_validator, chemical_db, dsstox_local, pubchem_client, toxvaldb_client
from utils.sds_parser import get_sds_parser

try:
    from utils import carcinogenic_potency_client
except ImportError:
    carcinogenic_potency_client = None  # type: ignore[misc, assignment]


class InputSource(Enum):
    TYPED_CAS = "typed_cas"
    TYPED_NAME = "typed_name"
    SDS_SINGLE = "sds_single"
    SDS_MULTI = "sds_multi"
    EXAMPLE = "example"


@dataclass
class ChemicalIdentity:
    cas: str
    chemical_name: Optional[str] = None
    dtxsid: Optional[str] = None
    source: InputSource = InputSource.TYPED_CAS
    source_details: Optional[str] = None
    sds_context: Optional[dict[str, Any]] = None
    confidence: float = 1.0
    validated: bool = False
    warnings: List[str] = field(default_factory=list)


@dataclass
class AssessmentResult:
    identity: ChemicalIdentity
    pubchem_data: dict[str, Any]
    dsstox_info: Optional[dict[str, Any]] = None
    toxval_data: Optional[dict[str, Any]] = None
    carc_potency_data: Optional[dict[str, Any]] = None
    assessment_time: float = 0.0
    has_multiple_components: bool = False
    all_components: List["AssessmentResult"] = field(default_factory=list)


class ChemicalAssessmentService:
    """
    Single service for database-backed hazard data assembly (PubChem + DSSTox + ToxVal + CPDB).
    """

    def __init__(self) -> None:
        self._sds_parser = get_sds_parser()
        self.db_stats = chemical_db.get_db_stats()
        self.use_sqlite_dsstox = self.db_stats.get("dsstox", {}).get("exists", False)
        self.use_sqlite_toxval = self.db_stats.get("toxvaldb", {}).get("exists", False)
        self._dsstox_data = None if self.use_sqlite_dsstox else dsstox_local.load_dsstox_enhanced()

    def to_result_data(self, result: AssessmentResult) -> dict[str, Any]:
        """Shape expected by existing `app.py` hazard / P2OASys display (`result_data`)."""
        ident = result.identity
        preferred = ident.chemical_name or (result.dsstox_info or {}).get("preferred_name")
        dtxsid = ident.dtxsid or (result.dsstox_info or {}).get("dtxsid")
        return {
            "pubchem": result.pubchem_data,
            "dsstox_info": result.dsstox_info,
            "dtxsid": dtxsid,
            "preferred_name": preferred,
            "clean_cas": ident.cas,
            "toxval_data": result.toxval_data,
            "carc_potency_data": result.carc_potency_data,
        }

    def assess(
        self,
        input_data: Any,
        source_hint: Optional[str] = None,
        *,
        assess_all_sds_components: bool = False,
    ) -> Union[AssessmentResult, List[AssessmentResult]]:
        """
        Route any supported input to assessment output(s).

        - ``str``: typed CAS or name (after normalize)
        - ``dict`` with ``cas``: example shortcut
        - File-like with ``getvalue()`` / ``read()`` + ``name``: SDS PDF

        If ``assess_all_sds_components`` is True and the PDF has multiple CAS rows, returns
        a list of ``AssessmentResult`` (one per component). Default False: caller should pick
        one CAS for SDS (matches current UI) or set True for batch use.
        """
        start = time.time()
        identified = self._identify_chemical(input_data, source_hint)

        if isinstance(identified, list):
            if len(identified) == 0:
                raise ValueError("No chemical identities resolved from input")
            if len(identified) == 1:
                r = self._assess_single_identity(identified[0])
                r.assessment_time = time.time() - start
                return r
            if assess_all_sds_components:
                out = [self._assess_single_identity(x) for x in identified]
                for r in out:
                    r.assessment_time = time.time() - start
                return out
            parent = AssessmentResult(
                identity=ChemicalIdentity(
                    cas="MIXTURE",
                    chemical_name=f"Mixture ({len(identified)} components)",
                    source=InputSource.SDS_MULTI,
                    source_details=identified[0].source_details,
                ),
                pubchem_data={},
                has_multiple_components=True,
                all_components=[self._assess_single_identity(x) for x in identified],
                assessment_time=time.time() - start,
            )
            return parent

        r = self._assess_single_identity(identified)
        r.assessment_time = time.time() - start
        return r

    def assess_identity(self, identity: ChemicalIdentity) -> AssessmentResult:
        """Assess when you already have a resolved ``ChemicalIdentity`` (e.g. SDS row)."""
        t0 = time.time()
        r = self._assess_single_identity(identity)
        r.assessment_time = time.time() - t0
        return r

    def identify_from_sds(self, pdf_file: Any) -> List[ChemicalIdentity]:
        """Extract chemical identities from an SDS PDF (no PubChem/DSSTox fetch)."""
        return self._identify_from_sds(pdf_file)

    def _identify_chemical(self, input_data: Any, source_hint: Optional[str]) -> Union[ChemicalIdentity, List[ChemicalIdentity]]:
        if isinstance(input_data, ChemicalIdentity):
            return input_data

        if isinstance(input_data, dict) and input_data.get("cas"):
            cas = str(input_data["cas"]).strip()
            norm = cas_validator.normalize_cas_input(cas) or cas
            return ChemicalIdentity(
                cas=norm,
                chemical_name=input_data.get("name"),
                source=InputSource.EXAMPLE,
                source_details=str(input_data.get("label", "Example")),
                validated=cas_validator.is_valid_cas_format(norm),
                confidence=1.0,
            )

        if isinstance(input_data, str):
            return self._identify_from_string(input_data, source_hint)

        if hasattr(input_data, "getvalue") or hasattr(input_data, "read"):
            return self._identify_from_sds(input_data)

        raise TypeError(f"Unsupported input type: {type(input_data)}")

    def _identify_from_string(self, text: str, _hint: Optional[str]) -> ChemicalIdentity:
        raw = (text or "").strip()
        if not raw:
            raise ValueError("Empty chemical query")
        norm = cas_validator.normalize_cas_input(raw) or raw

        if cas_validator.is_valid_cas_format(norm):
            return ChemicalIdentity(
                cas=norm,
                source=InputSource.TYPED_CAS,
                source_details=f"Typed: {raw}",
                validated=True,
                confidence=1.0,
            )

        if self.use_sqlite_dsstox:
            dsstox_info = chemical_db.get_dsstox_by_name(raw)
            if dsstox_info and dsstox_info.get("cas"):
                cas = str(dsstox_info["cas"]).strip()
                n2 = cas_validator.normalize_cas_input(cas) or cas
                return ChemicalIdentity(
                    cas=n2,
                    chemical_name=raw,
                    dtxsid=dsstox_info.get("dtxsid"),
                    source=InputSource.TYPED_NAME,
                    source_details=f"Name → DSSTox: {raw}",
                    validated=cas_validator.is_valid_cas_format(n2),
                    confidence=0.9,
                )

        return ChemicalIdentity(
            cas=norm,
            source=InputSource.TYPED_NAME,
            source_details=f"Name (PubChem resolve): {raw}",
            validated=False,
            confidence=0.55,
            warnings=["Name not found in local DSSTox; resolving via PubChem"],
        )

    def _identify_from_sds(self, pdf_file: Any) -> List[ChemicalIdentity]:
        pdf_bytes = pdf_file.getvalue() if hasattr(pdf_file, "getvalue") else pdf_file.read()
        if hasattr(pdf_file, "seek"):
            try:
                pdf_file.seek(0)
            except Exception:
                pass
        fname = getattr(pdf_file, "name", None) or "SDS.pdf"
        result = self._sds_parser.parse_pdf(pdf_bytes)
        if not result or not result.cas_numbers:
            raise ValueError("No CAS numbers found in SDS")
        multi = len(result.cas_numbers) > 1
        src = InputSource.SDS_MULTI if multi else InputSource.SDS_SINGLE
        identities: list[ChemicalIdentity] = []
        for ext in result.cas_numbers:
            cas = (ext.cas or "").strip()
            if not cas:
                continue
            identities.append(
                ChemicalIdentity(
                    cas=cas_validator.normalize_cas_input(cas) or cas,
                    chemical_name=ext.chemical_name,
                    source=src,
                    source_details=f"SDS: {fname}",
                    sds_context={
                        "concentration": ext.concentration,
                        "section": ext.section,
                        "method": ext.method,
                        "context": ext.context,
                    },
                    confidence=float(ext.confidence) if ext.confidence is not None else 0.85,
                    validated=bool(ext.validated),
                    warnings=list(ext.warnings or []),
                )
            )
        if not identities:
            raise ValueError("No CAS numbers found in SDS")
        return identities

    def _comptox_api_key(self) -> Optional[str]:
        try:
            if hasattr(st, "secrets"):
                k = st.secrets.get("COMPTOX_API_KEY")
                if k:
                    return str(k)
        except Exception:
            pass
        return os.environ.get("COMPTOX_API_KEY")

    def _assess_single_identity(self, identity: ChemicalIdentity) -> AssessmentResult:
        cas = identity.cas
        if self.use_sqlite_dsstox:
            dsstox_info = chemical_db.get_dsstox_by_cas(cas)
        else:
            dsstox_info = dsstox_local.get_dsstox_info(cas, self._dsstox_data) if self._dsstox_data else None

        dtxsid = (dsstox_info or {}).get("dtxsid")
        if dsstox_info:
            identity.dtxsid = dtxsid
            if not identity.chemical_name:
                identity.chemical_name = dsstox_info.get("preferred_name")

        if cas_validator.is_valid_cas_format(cas):
            input_type = "cas"
        else:
            input_type = "name"
        pubchem_data = pubchem_client.get_compound_data(cas, input_type=input_type)

        toxval_data = None
        if dtxsid and self.use_sqlite_toxval:
            recs = chemical_db.get_toxicity_by_dtxsid(dtxsid, numeric_only=False)
            toxval_data = {}
            for rec in recs:
                cat = (rec.get("study_type") or "other").strip() or "other"
                toxval_data.setdefault(cat, []).append(
                    {
                        "value": rec.get("toxval_numeric"),
                        "units": rec.get("toxval_units", ""),
                        "species": rec.get("species", ""),
                        "route": rec.get("exposure_route", ""),
                        "study_type": rec.get("study_type", ""),
                    }
                )
        elif dtxsid:
            api_key = self._comptox_api_key()
            if api_key:
                try:
                    toxval_data = toxvaldb_client.fetch_toxval_data(dtxsid, api_key)
                except Exception:
                    toxval_data = None

        carc = None
        if carcinogenic_potency_client and carcinogenic_potency_client.is_available():
            carc = carcinogenic_potency_client.get_data_by_cas(cas)

        return AssessmentResult(
            identity=identity,
            pubchem_data=pubchem_data,
            dsstox_info=dsstox_info,
            toxval_data=toxval_data,
            carc_potency_data=carc,
        )


@st.cache_resource
def get_assessment_service() -> ChemicalAssessmentService:
    return ChemicalAssessmentService()
