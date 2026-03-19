"""
Hugging Face Transformers CAS extractor for SDS PDFs.
Runs 100% locally with zero external API calls.
See docs/HF_TRANSFORMERS_CAS_EXTRACTION_PROMPT.md.
"""

from __future__ import annotations

import hashlib
import json
import re
import warnings
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any

# Optional: heavy deps so main app can run without them
try:
    import torch
    from transformers import (
        AutoModelForCausalLM,
        AutoTokenizer,
        pipeline,
        logging as transformers_logging,
    )
    _HF_AVAILABLE = True
except ImportError:
    torch = None
    AutoModelForCausalLM = AutoTokenizer = pipeline = transformers_logging = None
    _HF_AVAILABLE = False

try:
    from transformers import BitsAndBytesConfig
    _BITSANDBYTES_AVAILABLE = True
except ImportError:
    BitsAndBytesConfig = None
    _BITSANDBYTES_AVAILABLE = False

try:
    import pdfplumber
    _PDFPLUMBER_AVAILABLE = True
except ImportError:
    pdfplumber = None
    _PDFPLUMBER_AVAILABLE = False

try:
    import psutil
except ImportError:
    psutil = None

if _HF_AVAILABLE and transformers_logging is not None:
    transformers_logging.set_verbosity_error()
warnings.filterwarnings("ignore", category=UserWarning)

# --- Prompt 10: Helper functions ---
CAS_PATTERN = re.compile(r"\b(\d{1,7})-(\d{2})-(\d)\b")


def clean_cas(cas_str: Any) -> str | None:
    """Extract and normalize CAS string."""
    if not cas_str:
        return None
    s = str(cas_str).strip()
    m = re.search(r"(\d{1,7}-\d{2}-\d)", s)
    return m.group(1) if m else None


def validate_cas(cas: str | None) -> bool:
    """Validate CAS format and checksum."""
    if not cas or not isinstance(cas, str):
        return False
    if not re.match(r"^\d{1,7}-\d{2}-\d$", cas.strip()):
        return False
    try:
        parts = cas.strip().split("-")
        main = parts[0] + parts[1]
        check = int(parts[2])
        total = sum(int(d) * (i + 1) for i, d in enumerate(reversed(main)))
        return total % 10 == check
    except Exception:
        return False


# --- Prompt 1: Architecture + Prompts 2–9 in class ---
class HFCASExtractor:
    """Extract CAS numbers from SDS PDFs using Hugging Face Transformers. 100% local."""

    def __init__(self) -> None:
        self.model = None
        self.tokenizer = None
        self.pipeline = None
        self.model_loaded = False
        self.model_name: str | None = None
        self.device = self._get_optimal_device() if _HF_AVAILABLE and torch is not None else "cpu"
        self.cas_pattern = CAS_PATTERN

    def _get_optimal_device(self) -> str:
        if torch is None:
            return "cpu"
        if torch.cuda.is_available():
            return "cuda"
        if getattr(torch.backends, "mps", None) and torch.backends.mps.is_available():
            return "mps"
        return "cpu"

    # --- Prompt 2: Model loading ---
    def load_model(self, model_id: str = "microsoft/phi-2", quantization: str = "none") -> bool:
        if not _HF_AVAILABLE or torch is None:
            return False
        quantization_config = None
        if quantization == "4bit" and self.device == "cuda" and _BITSANDBYTES_AVAILABLE and BitsAndBytesConfig:
            quantization_config = BitsAndBytesConfig(
                load_in_4bit=True,
                bnb_4bit_quant_type="nf4",
                bnb_4bit_compute_dtype=torch.float16,
                bnb_4bit_use_double_quant=True,
            )
        elif quantization == "8bit" and self.device == "cuda" and _BITSANDBYTES_AVAILABLE and BitsAndBytesConfig:
            quantization_config = BitsAndBytesConfig(load_in_8bit=True)

        try:
            self.tokenizer = AutoTokenizer.from_pretrained(
                model_id, trust_remote_code=True, padding_side="left"
            )
            if self.tokenizer.pad_token is None:
                self.tokenizer.pad_token = self.tokenizer.eos_token

            kwargs = {
                "trust_remote_code": True,
                "low_cpu_mem_usage": True,
            }
            if quantization_config:
                kwargs["quantization_config"] = quantization_config
            else:
                kwargs["torch_dtype"] = torch.float16 if self.device == "cuda" else torch.float32
                if self.device == "cuda":
                    kwargs["device_map"] = "auto"
            self.model = AutoModelForCausalLM.from_pretrained(model_id, **kwargs)
            if self.device != "cuda":
                self.model = self.model.to(self.device)

            self.pipeline = pipeline(
                "text-generation",
                model=self.model,
                tokenizer=self.tokenizer,
                device=0 if self.device == "cuda" else -1,
                max_new_tokens=500,
                temperature=0.1,
                do_sample=False,
            )
            self.model_loaded = True
            self.model_name = model_id
            return True
        except Exception:
            return False

    def get_model_recommendations(self) -> dict[str, dict[str, Any]]:
        return {
            "low_memory": {
                "name": "SmolLM2-1.7B-Instruct",
                "size_gb": 1.7,
                "ram_gb": 4,
                "description": "Tiny but capable - runs on 4GB RAM",
                "quantization": "none",
                "hf_id": "HuggingFaceTB/SmolLM2-1.7B-Instruct",
            },
            "medium_memory": {
                "name": "Phi-3-mini-4k-instruct",
                "size_gb": 2.4,
                "ram_gb": 6,
                "description": "Microsoft Phi-3 - excellent for extraction",
                "quantization": "none",
                "hf_id": "microsoft/Phi-3-mini-4k-instruct",
            },
            "balanced": {
                "name": "Qwen2.5-7B-Instruct",
                "size_gb": 7,
                "ram_gb": 10,
                "description": "Qwen 7B - best accuracy for CAS extraction",
                "quantization": "4bit",
                "hf_id": "Qwen/Qwen2.5-7B-Instruct",
            },
            "high_accuracy": {
                "name": "Mistral-7B-Instruct-v0.3",
                "size_gb": 7,
                "ram_gb": 10,
                "description": "Mistral 7B - excellent instruction following",
                "quantization": "4bit",
                "hf_id": "mistralai/Mistral-7B-Instruct-v0.3",
            },
        }

    def recommend_model_for_hardware(self) -> dict[str, Any]:
        recs = self.get_model_recommendations()
        if psutil:
            ram_gb = psutil.virtual_memory().total / (1024**3)
        else:
            ram_gb = 8.0
        if _HF_AVAILABLE and torch is not None and torch.cuda.is_available():
            try:
                gpu_mem = torch.cuda.get_device_properties(0).total_memory / (1024**3)
                if gpu_mem >= 8:
                    return recs["balanced"]
                if gpu_mem >= 4:
                    return recs["medium_memory"]
            except Exception:
                pass
        if ram_gb >= 16:
            return recs["balanced"]
        if ram_gb >= 8:
            return recs["medium_memory"]
        return recs["low_memory"]

    # --- Prompt 3: PDF processing ---
    def extract_text_from_pdf(self, pdf_file: Any) -> tuple[str, list[Any]]:
        """Extract text and tables from PDF; preserve structure. Uses pdfplumber."""
        if not _PDFPLUMBER_AVAILABLE or pdfplumber is None:
            if hasattr(pdf_file, "read"):
                raw = pdf_file.read()
                pdf_file.seek(0)
            else:
                raw = pdf_file
            return (raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)), []
        text_parts: list[str] = []
        tables: list[Any] = []
        with pdfplumber.open(BytesIO(pdf_file.read() if hasattr(pdf_file, "read") else pdf_file)) as pdf:
            for page_num, page in enumerate(pdf.pages):
                text_parts.append(f"\n[PAGE {page_num + 1}]\n")
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text + "\n")
                page_tables = page.extract_tables()
                if page_tables:
                    text_parts.append("\n[TABLES]\n")
                    for table in page_tables:
                        for row in table:
                            if any(row):
                                text_parts.append(" | ".join(str(c) if c else "" for c in row) + "\n")
                        text_parts.append("---\n")
                    text_parts.append("[END TABLES]\n")
                tables.extend(page_tables)
        if hasattr(pdf_file, "seek"):
            pdf_file.seek(0)
        return "".join(text_parts), tables

    def extract_section_3(self, text: str) -> str:
        """Extract Section 3 (Composition/Ingredients) from SDS text."""
        patterns = [
            r"Section\s*3[:\s]*(.*?)(?=Section\s*4|\Z)",
            r"3\.\s*Composition[^\n]*(.*?)(?=\d\.\s|\Z)",
            r"COMPOSITION[^\n]*(.*?)(?=\d\.\s|\Z)",
            r"Information on Ingredients[^\n]*(.*?)(?=\d\.\s|\Z)",
        ]
        for pat in patterns:
            m = re.search(pat, text, re.IGNORECASE | re.DOTALL)
            if m:
                return m.group(1).strip()
        return ""

    # --- Prompt 4: Rule-based CAS extraction ---
    def extract_cas_rules(self, text: str) -> list[dict[str, Any]]:
        """Rule-based extraction from Section 3."""
        results: list[dict[str, Any]] = []
        section_3 = self.extract_section_3(text)
        if not section_3:
            section_3 = text

        pattern1 = re.compile(
            r"([A-Za-z][A-Za-z0-9\s\-/]+?)\s*\(?\s*CAS(?:\s*No\.?)?:?\s*(\d{1,7}-\d{2}-\d)\s*\)?",
            re.IGNORECASE,
        )
        for m in pattern1.finditer(section_3):
            component = m.group(1).strip()
            cas = clean_cas(m.group(2))
            if cas and validate_cas(cas):
                results.append({
                    "cas": cas,
                    "component": component,
                    "source": "section_3_pattern1",
                    "confidence": "high",
                    "method": "rule_based",
                })

        for line in section_3.split("\n"):
            cells = [c.strip() for c in line.split("|") if c.strip()]
            for cell in cells:
                cas_m = re.search(r"(\d{1,7}-\d{2}-\d)", cell)
                if cas_m:
                    cas = clean_cas(cas_m.group(1))
                    if cas and validate_cas(cas) and not any(r["cas"] == cas for r in results):
                        results.append({
                            "cas": cas,
                            "component": cells[0] if cells else "unknown",
                            "source": "section_3_table",
                            "confidence": "high",
                            "method": "rule_based",
                        })
        return results

    # --- Prompt 6: Prompt engineering ---
    def create_optimized_prompt(self, section_text: str) -> str:
        """Model-appropriate prompt for CAS extraction."""
        trunc = section_text[:1000]
        if self.model_name and "phi" in self.model_name.lower():
            return f"""<|user|>
Extract all CAS numbers and chemical names from this SDS section.
Rules: CAS format NNNNN-NN-N (e.g. 124-09-4). Include chemical name and concentration if present. Return ONLY a valid JSON array.
Text: {section_text[:800]}
<|assistant|>
["""
        return f"""Task: Extract chemical information from Safety Data Sheet.
Instructions:
1. Find all CAS registry numbers (format: XXXXX-XX-X).
2. Identify the corresponding chemical names.
3. Note concentrations if available.
4. Return as JSON array only.
Example: [{{"cas": "124-09-4", "chemical": "Hexamethylenediamine", "concentration": "60%"}}]
Section 3 text:
{trunc}
Output JSON:"""

    # --- Prompt 5: LLM verification ---
    def verify_with_llm(self, text: str, rule_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Use loaded HF model to verify and add CAS entries."""
        if not self.model_loaded or self.pipeline is None:
            return rule_results
        section_3 = self.extract_section_3(text)[:1000]
        if not section_3:
            return rule_results
        prompt = self.create_optimized_prompt(section_3)
        try:
            out = self.pipeline(
                prompt,
                max_new_tokens=500,
                temperature=0.1,
                do_sample=False,
                return_full_text=False,
            )
            response = out[0]["generated_text"] if out else ""
            json_m = re.search(r"\[.*\]", response, re.DOTALL)
            if not json_m:
                return rule_results
            llm_list = json.loads(json_m.group())
            existing_cas = {r["cas"] for r in rule_results}
            for item in llm_list:
                if not isinstance(item, dict) or "cas" not in item:
                    continue
                cas = clean_cas(item["cas"])
                if not cas or not validate_cas(cas) or cas in existing_cas:
                    continue
                existing_cas.add(cas)
                rule_results.append({
                    "cas": cas,
                    "component": item.get("chemical", "unknown"),
                    "source": "llm_verification",
                    "confidence": "medium",
                    "method": "llm_enhanced",
                    "concentration": item.get("concentration"),
                })
        except Exception:
            pass
        return rule_results

    # --- Prompt 7: Complete pipeline ---
    def extract_cas_from_pdf(self, pdf_file: Any, use_llm: bool = True) -> dict[str, Any]:
        """Full pipeline: PDF → text → rules → optional LLM → dedupe."""
        results: dict[str, Any] = {
            "cas_numbers": [],
            "details": [],
            "method_used": "rule_based",
            "llm_used": False,
            "timestamp": datetime.now().isoformat(),
        }
        text, _ = self.extract_text_from_pdf(pdf_file)
        rule_results = self.extract_cas_rules(text)
        results["details"] = list(rule_results)
        if use_llm and self.model_loaded:
            enhanced = self.verify_with_llm(text, list(rule_results))
            if len(enhanced) > len(rule_results):
                results["details"] = enhanced
                results["llm_used"] = True
                results["method_used"] = "llm_enhanced"
        seen: set[str] = set()
        unique: list[dict[str, Any]] = []
        for item in results["details"]:
            c = item.get("cas")
            if c and c not in seen:
                seen.add(c)
                unique.append(item)
        results["details"] = unique
        results["cas_numbers"] = [u["cas"] for u in unique]
        return results

    # --- Prompt 8: Caching helpers (script uses st.cache_data / session_state) ---
    @staticmethod
    def pdf_hash(pdf_bytes: bytes) -> str:
        return hashlib.md5(pdf_bytes).hexdigest()
