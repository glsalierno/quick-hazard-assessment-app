import type {
  ChemicalAssessment,
  EcotoxicityEntry,
  EcotoxicitySummary,
  GhsClassification,
  QueryInputType,
  ToxicityEntry,
} from "../types/chemical";
import { isValidCasFormat, normalizeCasInput } from "../utils/cas";

const PUBCHEM_BASE = "https://pubchem.ncbi.nlm.nih.gov/rest/pug";
const PUG_VIEW_BASE = "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view";
const MAX_RETRIES = 3;

const H_CODE_RE = /H\d+(?:\+\d+)?(?:\s*\([^)]+\))?/g;
const P_CODE_RE = /P\d+(?:\+\d+)?(?:\s*\([^)]+\))?/g;
const UNIT_RE = /(mg\/kg|mg\/m³|ppm|g\/kg|mL\/kg|mg\/L|µg\/kg|mg\/m3|ppb)\b/i;
const SPECIES_ROUTE_RE =
  /\b(rat|mouse|rabbit|dog|guinea pig|human|oral|dermal|inhalation|ip|iv|sc|ld50|lc50|fish|trout|daphnia|algae|aquatic)\b/gi;
const ECOTOX_ENDPOINT_RE = /\b(LC50|EC50|LC10|LC20|LC90|EC10|EC20|EC90|NOEC|LOEC)\b/i;
const ECOTOX_DURATION_RE = /(\d+\s*(?:h|hr|hrs|hour|hours|d|day|days))\b/i;
const ECOTOX_VALUE_UNIT_RE = /([<>~]?\s*\d+(?:[.,]\d+)?)\s*(mg\/L|µg\/L|ug\/L|mg\/kg|g\/L|mg\/l)\b/i;
const ECOTOX_CI_RE = /(?:CI|confidence interval)[^0-9]*([0-9]+(?:\.\d+)?)\s*[–-]\s*([0-9]+(?:\.\d+)?)/i;

type PugViewValue = string | number | null | undefined | Record<string, unknown>;
type PugViewObject = Record<string, unknown>;

function unique<T>(items: T[]) {
  return Array.from(new Set(items));
}

function getStringFromValue(value: PugViewValue) {
  if (value == null) {
    return "";
  }
  if (typeof value === "string" || typeof value === "number") {
    return String(value).trim();
  }

  const stringWithMarkup = value.StringWithMarkup;
  const rows = Array.isArray(stringWithMarkup) ? stringWithMarkup : stringWithMarkup ? [stringWithMarkup] : [];
  const parts = rows
    .map((row) => (typeof row === "object" && row !== null ? (row as PugViewObject).String : undefined))
    .filter((part): part is string => typeof part === "string");

  return parts.join(" ").trim() || (typeof value.String === "string" ? value.String : "");
}

function getReferenceUrls(value: PugViewValue) {
  if (!value || typeof value !== "object") {
    return [];
  }
  const stringWithMarkup = (value as PugViewObject).StringWithMarkup;
  const rows = Array.isArray(stringWithMarkup) ? stringWithMarkup : stringWithMarkup ? [stringWithMarkup] : [];
  const urls: string[] = [];

  for (const row of rows) {
    if (!row || typeof row !== "object") {
      continue;
    }
    const markup = (row as PugViewObject).Markup;
    if (!Array.isArray(markup)) {
      continue;
    }
    for (const item of markup) {
      if (item && typeof item === "object" && typeof (item as PugViewObject).URL === "string") {
        urls.push((item as PugViewObject).URL as string);
      }
    }
  }
  return urls;
}

async function fetchJsonWithRetry<T>(url: string): Promise<T> {
  let lastError: unknown;
  for (let attempt = 0; attempt < MAX_RETRIES; attempt += 1) {
    try {
      const response = await fetch(url);
      if (response.ok) {
        return (await response.json()) as T;
      }
      if (response.status !== 503) {
        throw new Error(`PubChem request failed (${response.status})`);
      }
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolve) => setTimeout(resolve, 500 * 2 ** attempt));
  }
  throw lastError instanceof Error ? lastError : new Error("PubChem request failed");
}

async function getCid(identifier: string, inputType: QueryInputType) {
  const encoded = encodeURIComponent(identifier);
  const path =
    inputType === "cas"
      ? `${PUBCHEM_BASE}/compound/xref/RegistryID/${encoded}/cids/JSON`
      : `${PUBCHEM_BASE}/compound/name/${encoded}/cids/JSON`;
  const data = await fetchJsonWithRetry<{ IdentifierList?: { CID?: number[] } }>(path);
  return data.IdentifierList?.CID?.[0];
}

function walkPugView(root: unknown, visitor: (node: PugViewObject) => void) {
  if (Array.isArray(root)) {
    root.forEach((child) => walkPugView(child, visitor));
    return;
  }
  if (!root || typeof root !== "object") {
    return;
  }
  const node = root as PugViewObject;
  visitor(node);
  Object.values(node).forEach((value) => walkPugView(value, visitor));
}

function extractGhsCodes(pugView: PugViewObject): GhsClassification {
  const result: GhsClassification = { hCodes: [], pCodes: [], signalWord: "", pictograms: [] };

  walkPugView(pugView, (node) => {
    const heading = String(node.TOCHeading ?? "");
    if (!heading.includes("GHS") && !heading.includes("Classification")) {
      return;
    }

    const information = Array.isArray(node.Information) ? node.Information : [];
    for (const info of information) {
      if (!info || typeof info !== "object") {
        continue;
      }
      const item = info as PugViewObject;
      const name = String(item.Name ?? "");
      const value = item.Value as PugViewValue;
      const text = getStringFromValue(value);
      const nameLower = name.toLowerCase();

      if (nameLower.includes("signal")) {
        result.signalWord = text || name;
      } else if (nameLower.includes("hazard") && text) {
        result.hCodes.push(...(text.match(H_CODE_RE) ?? []).map((code) => code.replace(/\s*\([^)]+\)/g, "").trim()));
      } else if (nameLower.includes("precautionary") && text) {
        result.pCodes.push(...(text.match(P_CODE_RE) ?? []).map((code) => code.replace(/\s*\([^)]+\)/g, "").trim()));
      }
    }
  });

  return {
    ...result,
    hCodes: unique(result.hCodes.filter(Boolean)),
    pCodes: unique(result.pCodes.filter(Boolean)),
    pictograms: unique(result.pictograms.filter(Boolean)),
  };
}

function extractHazardMetrics(pugView: PugViewObject) {
  const result = { flashPoint: [] as string[], vaporPressure: [] as string[], nfpa: [] as string[], iarc: [] as string[], prop65: [] as string[] };

  function processInfo(name: string, value: PugViewValue, heading: string) {
    const text = getStringFromValue(value);
    if (!text) {
      return;
    }
    const nameLower = name.toLowerCase();
    const headingLower = heading.toLowerCase();
    if (nameLower.includes("flash point") || headingLower.includes("flash point")) {
      result.flashPoint.push(text);
    }
    if (nameLower.includes("vapor pressure") || headingLower.includes("vapor pressure")) {
      result.vaporPressure.push(text);
    }
    if (nameLower.includes("nfpa") || headingLower.includes("nfpa")) {
      result.nfpa.push(text);
    }
    if (nameLower.includes("iarc") || headingLower.includes("iarc")) {
      result.iarc.push(text);
    }
    if (nameLower.includes("proposition 65") || nameLower.includes("prop 65")) {
      result.prop65.push(text);
    }
  }

  function walkSection(section: unknown, parentHeading = "") {
    if (!section || typeof section !== "object") {
      return;
    }
    const node = section as PugViewObject;
    const heading = String(node.TOCHeading ?? parentHeading);
    const information = Array.isArray(node.Information) ? node.Information : [];
    for (const info of information) {
      if (info && typeof info === "object") {
        const item = info as PugViewObject;
        processInfo(String(item.Name ?? ""), item.Value as PugViewValue, heading);
      }
    }
    const sections = Array.isArray(node.Section) ? node.Section : [];
    sections.forEach((child) => walkSection(child, heading));
  }

  const record = pugView.Record as PugViewObject | undefined;
  const sections = Array.isArray(record?.Section) ? record.Section : [];
  sections.forEach((section) => walkSection(section));

  return {
    flashPoint: unique(result.flashPoint),
    vaporPressure: unique(result.vaporPressure),
    nfpa: unique(result.nfpa),
    iarc: unique(result.iarc),
    prop65: unique(result.prop65),
  };
}

function classifyRouteAndSpecies(entry: Pick<ToxicityEntry, "value"> & { speciesRoute?: string[] }) {
  const value = entry.value.toLowerCase();
  const speciesRoute = (entry.speciesRoute ?? []).map((item) => item.toLowerCase());
  const speciesTokens = new Set(["rat", "mouse", "rabbit", "dog", "guinea pig", "human", "fish", "trout", "daphnia", "algae"]);
  const routeTokens = new Set(["oral", "dermal", "inhalation", "ip", "iv", "sc"]);
  const speciesParts = speciesRoute.filter((item) => speciesTokens.has(item) || (!routeTokens.has(item) && item !== "ld50" && item !== "lc50"));
  const routeParts = speciesRoute.filter((item) => routeTokens.has(item));
  let species = speciesParts.length > 0 ? unique(speciesParts).join(", ") : "—";
  let route = "Other";

  if (/fish|trout|daphnia|algae|aquatic/.test(value)) {
    route = "Ecotoxicity (aquatic)";
    if (species === "—") {
      species = value.includes("fish") || value.includes("trout") ? "fish" : value.includes("daphnia") ? "Daphnia" : value.includes("algae") ? "algae" : "aquatic";
    }
  } else if (value.includes("dermal") || value.includes("skin") || routeParts.includes("dermal")) {
    route = "Dermal";
    if (species === "—") {
      species = value.includes("rabbit") ? "rabbit" : value.includes("rat") ? "rat" : species;
    }
  } else if (value.includes("inhalation") || value.includes("inhaled") || value.includes("mg/m") || value.includes("ppm") || routeParts.includes("inhalation")) {
    route = "Inhalation";
    if (species === "—" && value.includes("rat")) {
      species = "rat";
    }
  } else if (value.includes("oral") || value.includes("po ") || routeParts.includes("oral") || (value.includes("rat") && !value.includes("dermal") && !value.includes("inhalation"))) {
    route = "Oral";
    if (species === "—") {
      species = value.includes("rat") ? "rat" : value.includes("mouse") ? "mouse" : species;
    }
  }

  return { route, species };
}

function extractToxicities(pugView: PugViewObject): ToxicityEntry[] {
  const toxEntries: ToxicityEntry[] = [];
  const toxKeywords = ["tox", "safety", "hazard", "health", "exposure", "pharmacokinetics", "carcinogen"];

  function processSection(section: unknown, parentHeading = "") {
    if (!section || typeof section !== "object") {
      return;
    }
    const node = section as PugViewObject;
    const heading = String(node.TOCHeading ?? parentHeading);
    const sections = Array.isArray(node.Section) ? node.Section : [];

    if (!toxKeywords.some((keyword) => heading.toLowerCase().includes(keyword))) {
      sections.forEach((child) => processSection(child, heading));
      return;
    }

    const information = Array.isArray(node.Information) ? node.Information : [];
    for (const info of information) {
      if (!info || typeof info !== "object") {
        continue;
      }
      const item = info as PugViewObject;
      const text = getStringFromValue(item.Value as PugViewValue);
      if (!text) {
        continue;
      }
      const speciesRoute = unique(text.match(SPECIES_ROUTE_RE) ?? []);
      const { route, species } = classifyRouteAndSpecies({ value: text, speciesRoute });
      toxEntries.push({
        type: String(item.Name ?? "Toxicity"),
        value: text.slice(0, 400),
        unit: text.match(UNIT_RE)?.[0],
        route,
        species,
        sourceSection: heading || parentHeading,
        referenceUrls: getReferenceUrls(item.Value as PugViewValue).slice(0, 5),
      });
    }
    sections.forEach((child) => processSection(child, heading));
  }

  const record = pugView.Record as PugViewObject | undefined;
  const sections = Array.isArray(record?.Section) ? record.Section : [];
  sections.forEach((section) => processSection(section));
  return toxEntries;
}

function parseEcotoxText(raw: string): Partial<EcotoxicityEntry> {
  const out: Partial<EcotoxicityEntry> = { conditions: raw };
  const endpoint = raw.match(ECOTOX_ENDPOINT_RE)?.[1];
  const duration = raw.match(ECOTOX_DURATION_RE)?.[1];
  const valueMatch = raw.match(ECOTOX_VALUE_UNIT_RE);
  const ciMatch = raw.match(ECOTOX_CI_RE);

  if (endpoint) {
    out.endpoint = endpoint.toUpperCase();
  }
  if (duration) {
    out.duration = duration;
  }
  if (valueMatch?.[1] && valueMatch[2]) {
    out.unit = valueMatch[2];
    const numeric = Number(valueMatch[1].replace(/[<>~\s,]/g, ""));
    if (!Number.isNaN(numeric)) {
      out.valueNum = numeric;
    }
  }
  if (ciMatch?.[1] && ciMatch[2]) {
    out.ciLow = Number(ciMatch[1]);
    out.ciHigh = Number(ciMatch[2]);
  }
  return out;
}

function extractEcotoxicity(ghs: GhsClassification, toxicities: ToxicityEntry[]): EcotoxicitySummary {
  const summary: EcotoxicitySummary = {
    hCodesAquatic: ghs.hCodes.filter((code) => code.startsWith("H4")),
    entries: [],
  };

  for (const toxicity of toxicities) {
    const lower = toxicity.value.toLowerCase();
    if (!/fish|trout|daphnia|algae|aquatic/.test(lower)) {
      continue;
    }
    const species = lower.includes("fish") || lower.includes("trout") ? "fish" : lower.includes("daphnia") ? "Daphnia" : lower.includes("algae") ? "algae" : "aquatic";
    const parsed = parseEcotoxText(toxicity.value);
    const entry: EcotoxicityEntry = {
      value: toxicity.value,
      species,
      unit: parsed.unit ?? "mg/L",
      ...parsed,
    };
    summary.entries.push(entry);

    if (entry.valueNum != null) {
      if (entry.endpoint?.startsWith("EC")) {
        summary.aquaticEc50MgL = entry.valueNum;
      } else {
        summary.aquaticLc50MgL = summary.aquaticLc50MgL ?? entry.valueNum;
      }
      summary.aquaticSpecies = summary.aquaticSpecies ?? species;
      summary.aquaticValueRaw = toxicity.value.slice(0, 250);
    }
  }

  return summary;
}

function computeExposureBands(toxicities: ToxicityEntry[]): ChemicalAssessment["exposureBands"] {
  const bandFromValue = (value: number, bands: Array<[number, number]>) => bands.find(([threshold]) => value <= threshold)?.[1] ?? 5;
  const bands = {
    oral: [
      [5, 1],
      [50, 2],
      [300, 3],
      [2000, 4],
      [5000, 5],
    ] as Array<[number, number]>,
    dermal: [
      [50, 1],
      [200, 2],
      [1000, 3],
      [2000, 4],
      [5000, 5],
    ] as Array<[number, number]>,
  };

  const oral = toxicities.find((toxicity) => /ld50/i.test(toxicity.value) && /mg\s*\/\s*kg/i.test(toxicity.value) && !/dermal|skin/i.test(toxicity.value));
  const dermal = toxicities.find((toxicity) => /ld50/i.test(toxicity.value) && /mg\s*\/\s*kg/i.test(toxicity.value) && /dermal|skin/i.test(toxicity.value));
  const inhalation = toxicities.find((toxicity) => /lc50/i.test(toxicity.value) && /(mg\s*\/\s*m|mg\/m3)/i.test(toxicity.value));

  const oralValue = oral?.value.match(/(\d+(?:\.\d+)?)\s*mg\s*\/\s*kg/i)?.[1];
  const dermalValue = dermal?.value.match(/(\d+(?:\.\d+)?)\s*mg\s*\/\s*kg/i)?.[1];
  const inhalationValue = inhalation?.value.match(/(\d+(?:[.,]\d+)*)\s*mg\s*\/\s*m/i)?.[1] ?? inhalation?.value.match(/(\d+(?:[.,]\d+)*)\s*mg\/m3/i)?.[1];

  const inhalationNumber = inhalationValue ? Number(inhalationValue.replace(",", "")) : undefined;
  const inhalationBand =
    inhalationNumber == null ? undefined : inhalationNumber <= 100 ? 1 : inhalationNumber <= 500 ? 2 : inhalationNumber <= 2500 ? 3 : inhalationNumber <= 20000 ? 4 : 5;

  return {
    oral: oralValue ? { ld50MgKg: Number(oralValue), band: bandFromValue(Number(oralValue), bands.oral), source: "PubChem" } : {},
    dermal: dermalValue ? { ld50MgKg: Number(dermalValue), band: bandFromValue(Number(dermalValue), bands.dermal), source: "PubChem" } : {},
    inhalation: inhalationNumber != null ? { lc50MgM3: inhalationNumber, band: inhalationBand, source: "PubChem" } : {},
  };
}

export async function fetchChemicalAssessment(query: string): Promise<ChemicalAssessment> {
  const normalizedQuery = normalizeCasInput(query);
  const inputType: QueryInputType = isValidCasFormat(normalizedQuery) ? "cas" : "name";
  const cid = await getCid(normalizedQuery, inputType);
  if (!cid) {
    throw new Error(`No PubChem compound found for "${query}".`);
  }

  const propertiesUrl = `${PUBCHEM_BASE}/compound/cid/${cid}/property/MolecularFormula,MolecularWeight,IUPACName,CanonicalSMILES,IsomericSMILES/JSON`;
  const [propertiesData, pugView] = await Promise.all([
    fetchJsonWithRetry<{ PropertyTable?: { Properties?: Array<Record<string, unknown>> } }>(propertiesUrl),
    fetchJsonWithRetry<PugViewObject>(`${PUG_VIEW_BASE}/data/compound/${cid}/JSON`),
  ]);
  const properties = propertiesData.PropertyTable?.Properties?.[0] ?? {};
  const ghs = extractGhsCodes(pugView);
  const hazards = extractHazardMetrics(pugView);
  const toxicities = extractToxicities(pugView);

  return {
    query,
    normalizedQuery,
    inputType,
    cid,
    iupacName: properties.IUPACName as string | undefined,
    formula: properties.MolecularFormula as string | undefined,
    molecularWeight: properties.MolecularWeight != null ? String(properties.MolecularWeight) : undefined,
    smiles: (properties.IsomericSMILES as string | undefined) ?? (properties.CanonicalSMILES as string | undefined),
    ghs,
    flashPoint: hazards.flashPoint,
    vaporPressure: hazards.vaporPressure,
    toxicities,
    ld50: toxicities.filter((toxicity) => toxicity.value.toUpperCase().includes("LD50")).map((toxicity) => toxicity.value),
    lc50: toxicities.filter((toxicity) => toxicity.value.toUpperCase().includes("LC50")).map((toxicity) => toxicity.value),
    ecotoxicity: extractEcotoxicity(ghs, toxicities),
    exposureBands: computeExposureBands(toxicities),
    nfpa: hazards.nfpa.join("; ") || undefined,
    iarc: hazards.iarc.join("; ") || undefined,
    prop65: hazards.prop65.join("; ") || undefined,
    assessedAt: new Date().toISOString(),
  };
}
