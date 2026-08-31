import { getHazardPhrase } from './ghs';
import type { ChemicalReport, ExposureBands, GhsClassification, HazardSummary, HazardSummaryHighlight } from './types';

const cmrPrefixes = ['H340', 'H341', 'H350', 'H351', 'H360', 'H361', 'H362'];
const seriousHealth = new Set(['H300', 'H301', 'H310', 'H311', 'H314', 'H318', 'H330', 'H331', 'H334', 'H370', 'H372']);
const highlyFlammable = new Set(['H220', 'H222', 'H224', 'H225']);
const flammable = new Set([...highlyFlammable, 'H221', 'H223', 'H226', 'H228']);

function cleanCodes(codes?: string[]): string[] {
  return Array.from(new Set((codes ?? []).map((code) => code.trim()).filter(Boolean)));
}

function isCmr(code: string): boolean {
  return cmrPrefixes.some((prefix) => code.startsWith(prefix));
}

function phrase(code: string): string {
  const text = getHazardPhrase(code);
  return text ? `${code}: ${text.replace(/\.$/, '')}` : code;
}

function shortText(text: string, limit = 80): string {
  const compact = text.replace(/\s+/g, ' ').trim();
  return compact.length <= limit ? compact : `${compact.slice(0, limit - 1)}…`;
}

function firstText(values?: string[] | string): string {
  if (Array.isArray(values)) {
    return values.find((value) => value.trim())?.trim() ?? '';
  }
  return values?.trim() ?? '';
}

function concernLevel(hCodes: string[], signalWord: string): HazardSummary['concernLevel'] {
  if (hCodes.some((code) => isCmr(code) || seriousHealth.has(code))) {
    return 'high';
  }
  if (hCodes.some((code) => highlyFlammable.has(code)) || signalWord.toLowerCase() === 'danger') {
    return 'high';
  }
  if (hCodes.length > 0 || signalWord) {
    return 'moderate';
  }
  return 'unknown';
}

function headline(name: string, hCodes: string[], signalWord: string): string {
  const bits: string[] = [];
  if (signalWord && !['none', 'n/a'].includes(signalWord.toLowerCase())) {
    bits.push(signalWord);
  }
  if (hCodes.some((code) => highlyFlammable.has(code))) {
    bits.push('highly flammable liquid');
  } else if (hCodes.some((code) => flammable.has(code))) {
    bits.push('flammable liquid');
  }
  if (hCodes.some((code) => code.startsWith('H360') || code.startsWith('H361'))) {
    bits.push('reproductive toxicity');
  } else if (hCodes.some((code) => code.startsWith('H350') || code.startsWith('H351'))) {
    bits.push('carcinogenicity concern');
  } else if (hCodes.some((code) => code.startsWith('H340') || code.startsWith('H341'))) {
    bits.push('germ-cell mutagenicity concern');
  } else if (hCodes.includes('H318')) {
    bits.push('serious eye damage');
  } else if (hCodes.includes('H319')) {
    bits.push('eye irritation');
  }

  if (bits.length >= 2) {
    return `${bits[0]} — ${bits.slice(1).join('; ')}`;
  }
  if (bits.length === 1) {
    return `${bits[0]} — ${name}`;
  }
  return `${name} — limited GHS data in PubChem`;
}

function physicalSentence(hCodes: string[], flashPoint: string, vaporPressure: string): string {
  const physical = hCodes.filter((code) => code.startsWith('H2'));
  const parts: string[] = [];
  if (physical.length > 0) {
    parts.push(`Physical hazards: ${physical.map(phrase).join('; ')}.`);
  }
  const extras = [
    flashPoint ? `reported flash point ${flashPoint}` : '',
    vaporPressure ? `vapor pressure ${vaporPressure}` : '',
  ].filter(Boolean);
  if (extras.length > 0) {
    parts.push(`Key physical data include ${extras.join(' and ')}.`);
  }
  return parts.join(' ');
}

function healthSentence(
  hCodes: string[],
  iarc: string | undefined,
  prop65: string | undefined,
  exposureBands: ExposureBands,
): string {
  const health = hCodes.filter((code) => code.startsWith('H3'));
  const parts: string[] = [];
  if (health.length > 0) {
    const ordered = [...health.filter(isCmr), ...health.filter((code) => !isCmr(code))];
    parts.push(`Health hazards: ${ordered.map(phrase).join('; ')}.`);
  }
  if (iarc) {
    parts.push(`IARC: ${iarc}.`);
  }
  if (prop65) {
    parts.push(`Proposition 65: ${prop65}.`);
  }

  const bandBits = (['oral', 'dermal', 'inhalation'] as const)
    .map((route) => {
      const band = exposureBands[route];
      if (!band) {
        return undefined;
      }
      const unit = band.metric === 'lc50_mg_m3' ? 'mg/m3' : 'mg/kg';
      return `${route} ${band.value} ${unit} (GHS-style band ${band.band})`;
    })
    .filter((item): item is string => Boolean(item));
  if (bandBits.length > 0) {
    parts.push(`Acute toxicity values used for screening bands: ${bandBits.join('; ')}.`);
  }
  return parts.join(' ');
}

function environmentSentence(hCodes: string[], aquaticCodes: string[], aquaticLc50?: number): string {
  const codes = cleanCodes([...hCodes.filter((code) => code.startsWith('H4')), ...aquaticCodes]);
  if (codes.length > 0) {
    return `Aquatic/environmental hazards: ${codes.map(phrase).join('; ')}.`;
  }
  if (aquaticLc50 !== undefined) {
    return `No GHS aquatic H-codes were extracted; an aquatic LC50 of ${aquaticLc50} mg/L was parsed from PubChem text.`;
  }
  return 'No GHS aquatic hazard statements were extracted from the current PubChem record.';
}

export function buildHazardSummary(input: {
  query: string;
  preferredName?: string;
  iupacName?: string;
  formula?: string;
  ghs: GhsClassification;
  flashPoint: string[];
  vaporPressure: string[];
  nfpa?: string;
  iarc?: string;
  prop65?: string;
  exposureBands: ExposureBands;
  aquaticCodes: string[];
  aquaticLc50MgL?: number;
}): HazardSummary {
  const hCodes = cleanCodes(input.ghs.hCodes);
  const signalWord = (input.ghs.signalWord ?? '').trim();
  const name = (input.preferredName || input.iupacName || input.query || 'This compound').trim();
  const identity = [name, input.formula, input.query && input.query.toLowerCase() !== name.toLowerCase() ? `query ${input.query}` : '']
    .filter(Boolean)
    .join(' / ');
  const flash = firstText(input.flashPoint);
  const vapor = firstText(input.vaporPressure);
  const concern = concernLevel(hCodes, signalWord);

  const intro = signalWord
    ? `${identity} has a PubChem GHS signal word of ${signalWord}.`
    : `${identity} has limited GHS classification text in the current PubChem record.`;

  const highlights: HazardSummaryHighlight[] = [];
  if (signalWord) {
    highlights.push({
      label: 'Signal word',
      value: signalWord,
      tone: signalWord.toLowerCase() === 'danger' ? 'danger' : 'warning',
    });
  }
  highlights.push({
    label: 'Concern',
    value: concern.charAt(0).toUpperCase() + concern.slice(1),
    tone: concern === 'high' ? 'danger' : concern === 'moderate' ? 'warning' : 'info',
  });
  if (flash) {
    highlights.push({ label: 'Flash point', value: shortText(flash), tone: 'warning' });
  }
  if (input.nfpa) {
    highlights.push({ label: 'NFPA', value: shortText(input.nfpa), tone: 'info' });
  }

  const priority = hCodes.filter((code) => isCmr(code) || seriousHealth.has(code) || highlyFlammable.has(code));
  const remaining = hCodes.filter((code) => !priority.includes(code));
  for (const code of [...priority, ...remaining].slice(0, 4)) {
    highlights.push({
      label: code,
      value: phrase(code).split(': ').slice(1).join(': ') || code,
      tone: isCmr(code) || seriousHealth.has(code) || highlyFlammable.has(code) ? 'danger' : 'warning',
    });
  }

  return {
    headline: headline(name, hCodes, signalWord),
    paragraphs: [
      intro,
      physicalSentence(hCodes, flash, vapor),
      healthSentence(hCodes, input.iarc, input.prop65, input.exposureBands),
      environmentSentence(hCodes, input.aquaticCodes, input.aquaticLc50MgL),
    ].filter(Boolean),
    highlights,
    concernLevel: concern,
  };
}

export function summarizeReport(report: Pick<
  ChemicalReport,
  | 'normalizedQuery'
  | 'iupacName'
  | 'formula'
  | 'dsstox'
  | 'ghs'
  | 'flashPoint'
  | 'vaporPressure'
  | 'nfpa'
  | 'iarc'
  | 'prop65'
  | 'exposureBands'
  | 'ecotoxicity'
>): HazardSummary {
  return buildHazardSummary({
    query: report.normalizedQuery,
    preferredName: report.dsstox?.preferredName,
    iupacName: report.iupacName,
    formula: report.formula,
    ghs: report.ghs,
    flashPoint: report.flashPoint,
    vaporPressure: report.vaporPressure,
    nfpa: report.nfpa,
    iarc: report.iarc,
    prop65: report.prop65,
    exposureBands: report.exposureBands,
    aquaticCodes: report.ecotoxicity.hCodesAquatic,
    aquaticLc50MgL: report.ecotoxicity.aquaticLc50MgL,
  });
}
