import { inputTypeForQuery, normalizeCasInput } from '../domain/cas';
import { getBundledDsstoxRecord } from '../domain/dsstox';
import {
  computeExposureBands,
  extractEcotoxicity,
  extractGhsClassification,
  extractHazardMetrics,
  extractToxicities,
  prioritizeToxicityData,
} from '../domain/hazardLogic';
import { ChemicalReport, PubChemPropertyRow } from '../domain/types';

const pubChemBase = 'https://pubchem.ncbi.nlm.nih.gov/rest/pug';
const pugViewBase = 'https://pubchem.ncbi.nlm.nih.gov/rest/pug_view';

async function fetchJson<T>(url: string): Promise<T> {
  const response = await fetch(url, {
    headers: { Accept: 'application/json' },
  });

  if (!response.ok) {
    throw new Error(`PubChem request failed (${response.status})`);
  }

  return response.json() as Promise<T>;
}

async function resolveCid(identifier: string, inputType: 'cas' | 'name'): Promise<number> {
  const encoded = encodeURIComponent(identifier);
  const url =
    inputType === 'cas'
      ? `${pubChemBase}/compound/xref/RegistryID/${encoded}/cids/JSON`
      : `${pubChemBase}/compound/name/${encoded}/cids/JSON`;
  const data = await fetchJson<{ IdentifierList?: { CID?: number[] } }>(url);
  const cid = data.IdentifierList?.CID?.[0];

  if (!cid) {
    throw new Error(`No PubChem compound found for "${identifier}".`);
  }

  return cid;
}

async function fetchProperties(cid: number): Promise<PubChemPropertyRow> {
  const propertyList = 'MolecularFormula,MolecularWeight,IUPACName,CanonicalSMILES,IsomericSMILES';
  const data = await fetchJson<{ PropertyTable?: { Properties?: PubChemPropertyRow[] } }>(
    `${pubChemBase}/compound/cid/${cid}/property/${propertyList}/JSON`,
  );
  const properties = data.PropertyTable?.Properties?.[0];

  if (!properties) {
    throw new Error(`No PubChem properties found for CID ${cid}.`);
  }

  return properties;
}

async function fetchPugView(cid: number): Promise<unknown | undefined> {
  try {
    return await fetchJson<unknown>(`${pugViewBase}/data/compound/${cid}/JSON`);
  } catch {
    return undefined;
  }
}

function reportId(query: string, cid: number): string {
  return `${cid}-${Date.now()}-${query.replace(/[^a-zA-Z0-9]/g, '')}`;
}

export async function assessCompound(rawQuery: string): Promise<ChemicalReport> {
  const normalizedQuery = normalizeCasInput(rawQuery);
  const inputType = inputTypeForQuery(normalizedQuery);
  const cid = await resolveCid(normalizedQuery, inputType);
  const [properties, pugView] = await Promise.all([fetchProperties(cid), fetchPugView(cid)]);

  const ghs = pugView ? extractGhsClassification(pugView) : { hCodes: [], pCodes: [], signalWord: '', pictograms: [] };
  const metrics = pugView
    ? extractHazardMetrics(pugView)
    : { flashPoint: [], vaporPressure: [], nfpa: undefined, iarc: undefined, prop65: undefined };
  const toxicities = pugView ? extractToxicities(pugView) : [];
  const ecotoxicity = extractEcotoxicity(ghs, toxicities);
  const smiles = properties.IsomericSMILES ?? properties.CanonicalSMILES;

  return {
    id: reportId(normalizedQuery, cid),
    queriedAt: new Date().toISOString(),
    query: rawQuery,
    normalizedQuery,
    inputType,
    cid,
    iupacName: properties.IUPACName,
    formula: properties.MolecularFormula,
    molecularWeight:
      properties.MolecularWeight === undefined
        ? undefined
        : typeof properties.MolecularWeight === 'number'
          ? properties.MolecularWeight.toFixed(2)
          : String(properties.MolecularWeight),
    smiles,
    structureImageUrl: `${pubChemBase}/compound/cid/${cid}/PNG?image_size=large`,
    dsstox: inputType === 'cas' ? getBundledDsstoxRecord(normalizedQuery) : undefined,
    ghs,
    flashPoint: metrics.flashPoint,
    vaporPressure: metrics.vaporPressure,
    nfpa: metrics.nfpa,
    iarc: metrics.iarc,
    prop65: metrics.prop65,
    toxicities,
    prioritizedToxicity: prioritizeToxicityData(toxicities),
    ecotoxicity,
    exposureBands: computeExposureBands(toxicities),
  };
}
