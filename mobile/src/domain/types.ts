export type DsstoxRecord = {
  cas: string;
  dtxsid: string;
  preferredName?: string;
};

export type GhsClassification = {
  hCodes: string[];
  pCodes: string[];
  signalWord?: string;
  pictograms: string[];
};

export type ToxicityEntry = {
  type: string;
  value: string;
  unit?: string;
  speciesRoute?: string[];
  route: string;
  species: string;
  sourceSection?: string;
  referenceUrls?: string[];
};

export type PrioritizedToxicityItem = {
  bucket: 'Quantitative' | 'Categorical';
  source: 'PubChem' | 'ToxValDB';
  endpoint: string;
  value: string;
  units: string;
  species: string;
  route: string;
  details?: string;
};

export type EcotoxicityEntry = {
  value: string;
  species: string;
  unit: string;
  endpoint?: string;
  duration?: string;
  valueNum?: number;
  ciLow?: number;
  ciHigh?: number;
  conditions?: string;
};

export type EcotoxicitySummary = {
  aquaticLc50MgL?: number;
  aquaticEc50MgL?: number;
  aquaticSpecies?: string;
  aquaticValueRaw?: string;
  hCodesAquatic: string[];
  entries: EcotoxicityEntry[];
};

export type ExposureBand = {
  value: number;
  metric: 'ld50_mg_kg' | 'lc50_mg_m3';
  band: number;
  source: 'PubChem';
};

export type ExposureBands = {
  oral?: ExposureBand;
  dermal?: ExposureBand;
  inhalation?: ExposureBand;
};

export type HazardSummaryHighlight = {
  label: string;
  value: string;
  tone: 'info' | 'warning' | 'danger';
};

export type HazardSummary = {
  headline: string;
  paragraphs: string[];
  highlights: HazardSummaryHighlight[];
  concernLevel: 'high' | 'moderate' | 'low' | 'unknown';
};

export type ChemicalReport = {
  id: string;
  queriedAt: string;
  query: string;
  normalizedQuery: string;
  inputType: 'cas' | 'name';
  cid: number;
  iupacName?: string;
  formula?: string;
  molecularWeight?: string;
  smiles?: string;
  structureImageUrl: string;
  dsstox?: DsstoxRecord;
  ghs: GhsClassification;
  flashPoint: string[];
  vaporPressure: string[];
  nfpa?: string;
  iarc?: string;
  prop65?: string;
  toxicities: ToxicityEntry[];
  prioritizedToxicity: PrioritizedToxicityItem[];
  ecotoxicity: EcotoxicitySummary;
  exposureBands: ExposureBands;
  hazardSummary?: HazardSummary;
  isExample?: boolean;
};

export type PubChemPropertyRow = {
  CID: number;
  MolecularFormula?: string;
  MolecularWeight?: number | string;
  IUPACName?: string;
  CanonicalSMILES?: string;
  IsomericSMILES?: string;
};
