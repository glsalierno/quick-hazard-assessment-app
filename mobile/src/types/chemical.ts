export type QueryInputType = "cas" | "name";

export type DsstoxRecord = {
  cas?: string;
  dtxsid?: string;
  preferredName?: string;
  systematicName?: string;
  molecularFormula?: string;
  averageMass?: string | number;
  monoisotopicMass?: string | number;
  inchi?: string;
  inchikey?: string;
  smiles?: string;
};

export type GhsClassification = {
  hCodes: string[];
  pCodes: string[];
  signalWord: string;
  pictograms: string[];
};

export type ToxicityEntry = {
  type: string;
  value: string;
  unit?: string;
  route: string;
  species: string;
  sourceSection?: string;
  referenceUrls?: string[];
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
  band?: number;
  source?: string;
  ld50MgKg?: number;
  lc50MgM3?: number;
};

export type ChemicalAssessment = {
  query: string;
  normalizedQuery: string;
  inputType: QueryInputType;
  cid: number;
  iupacName?: string;
  formula?: string;
  molecularWeight?: string;
  smiles?: string;
  dsstox?: DsstoxRecord;
  ghs: GhsClassification;
  flashPoint: string[];
  vaporPressure: string[];
  toxicities: ToxicityEntry[];
  ld50: string[];
  lc50: string[];
  ecotoxicity: EcotoxicitySummary;
  exposureBands: {
    oral: ExposureBand;
    dermal: ExposureBand;
    inhalation: ExposureBand;
  };
  nfpa?: string;
  iarc?: string;
  prop65?: string;
  assessedAt: string;
};

export type PrioritizedToxicity = {
  quantitative: ToxicityEntry[];
  categorical: ToxicityEntry[];
};

export type AssessmentHistoryItem = {
  id: string;
  query: string;
  title: string;
  subtitle?: string;
  assessedAt: string;
  assessment: ChemicalAssessment;
};
