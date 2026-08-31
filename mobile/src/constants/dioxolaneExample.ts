import { prioritizeToxicityData } from '../domain/hazardLogic';
import { summarizeReport } from '../domain/hazardSummary';
import { ChemicalReport, ToxicityEntry } from '../domain/types';

const toxicities: ToxicityEntry[] = [
  {
    type: 'Non-Human Toxicity Values',
    value: 'LD50 Rat oral 5.2 g/kg with 95% confidence limits of 4.3 to 6.1 g/kg.',
    unit: 'g/kg',
    route: 'Oral',
    species: 'rat',
  },
  {
    type: 'Non-Human Toxicity Values',
    value: 'LD50 Rat inhalation 20650 mg/cu m/4 hr',
    unit: 'mg/m3',
    route: 'Inhalation',
    species: 'rat',
  },
  {
    type: 'Non-Human Toxicity Values',
    value: 'LC50 Rat inhalation 68.4 mg/L/4 hr',
    unit: 'mg/L',
    route: 'Inhalation',
    species: 'rat',
  },
  {
    type: 'Ecotoxicity Values',
    value: 'LC50; Species: Cyprinodon variegatus (Sheepshead minnow); Concentration: 10000 mg/L for 96 hr',
    unit: 'mg/L',
    route: 'Ecotoxicity (aquatic)',
    species: 'fish',
  },
  {
    type: 'Health Effects',
    value: '1,3-dioxolane is a CNS depressant. Inhalation or contact may irritate skin and eyes. Vapors may cause dizziness.',
    route: 'Inhalation',
    species: 'human',
  },
];

const snapshot: Omit<ChemicalReport, 'hazardSummary' | 'prioritizedToxicity'> = {
  id: 'example-646-06-0',
  queriedAt: '2026-08-30T23:24:00.000Z',
  query: '646-06-0',
  normalizedQuery: '646-06-0',
  inputType: 'cas',
  cid: 12586,
  iupacName: '1,3-dioxolane',
  formula: 'C3H6O2',
  molecularWeight: '74.08',
  smiles: 'C1COCO1',
  structureImageUrl: 'https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/12586/PNG?image_size=large',
  dsstox: { cas: '646-06-0', dtxsid: 'DTXSID4027284', preferredName: '1,3-Dioxolane' },
  ghs: {
    hCodes: ['H225', 'H319', 'H360', 'H318', 'H361', 'H303', 'H316', 'H341'],
    pCodes: [
      'P203',
      'P210',
      'P233',
      'P240',
      'P241',
      'P242',
      'P243',
      'P264',
      'P280',
      'P303+P361+P353',
      'P305+P351+P338',
      'P318',
      'P337+P317',
      'P370+P378',
      'P403+P235',
      'P405',
      'P501',
    ],
    signalWord: 'Danger',
    pictograms: ['Flammable', 'Irritant', 'Health Hazard', 'Corrosive'],
  },
  flashPoint: ['35 °F (2 °C) (Open cup)'],
  vaporPressure: ['79 mm Hg at 20 °C', '79.0 [mmHg]'],
  nfpa: 'Health 1; Fire 3; Instability 2',
  toxicities,
  ecotoxicity: {
    aquaticLc50MgL: 10000,
    aquaticSpecies: 'fish',
    hCodesAquatic: [],
    entries: [
      {
        species: 'fish',
        endpoint: 'LC50',
        duration: '96 hr',
        value: '96-hour LC50 was reported to be 10,000 mg/L for sheepshead minnow (Cyprinodon variegatus).',
        valueNum: 10000,
        unit: 'mg/L',
      },
      {
        species: 'fish',
        endpoint: 'LC50',
        duration: '48 hr',
        value: '48-hr LC50 was reported to be 12,000 mg/L for sheepshead minnow.',
        valueNum: 12000,
        unit: 'mg/L',
      },
    ],
  },
  exposureBands: {
    oral: { value: 10000, metric: 'ld50_mg_kg', band: 5, source: 'PubChem' },
    inhalation: { value: 20650, metric: 'lc50_mg_m3', band: 5, source: 'PubChem' },
  },
  isExample: true,
};

export function getDioxolaneExampleReport(): ChemicalReport {
  const report = {
    ...snapshot,
    prioritizedToxicity: prioritizeToxicityData(snapshot.toxicities),
  };
  return {
    ...report,
    hazardSummary: summarizeReport(report),
  };
}
