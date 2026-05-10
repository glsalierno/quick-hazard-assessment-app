import {
  EcotoxicityEntry,
  EcotoxicitySummary,
  ExposureBand,
  ExposureBands,
  GhsClassification,
  PrioritizedToxicityItem,
  ToxicityEntry,
} from './types';

const hCodePattern = /H\d+(?:\+\d+)?(?:\s*\([^)]+\))?/g;
const pCodePattern = /P\d+(?:\+\d+)?(?:\s*\([^)]+\))?/g;
const unitPattern = /(mg\/kg|mg\/m³|ppm|g\/kg|mL\/kg|mg\/L|µg\/kg|mg\/m3|ppb)\b/i;
const speciesRoutePattern =
  /\b(rat|mouse|rabbit|dog|guinea pig|human|oral|dermal|inhalation|ip|iv|sc|ld50|lc50|fish|trout|daphnia|algae|aquatic)\b/gi;
const ecotoxEndpointPattern = /\b(LC50|EC50|LC10|LC20|LC90|EC10|EC20|EC90|NOEC|LOEC)\b/i;
const ecotoxDurationPattern = /(\d+\s*(?:h|hr|hrs|hour|hours|d|day|days))\b/i;
const ecotoxValueUnitPattern = /([<>~]?\s*\d+(?:[.,]\d+)?)\s*(mg\/L|µg\/L|ug\/L|mg\/kg|g\/L|mg\/l)\b/i;
const ecotoxCiPattern =
  /(?:CI|confidence interval)[^0-9]*([0-9]+(?:\.\d+)?)\s*[–-]\s*([0-9]+(?:\.\d+)?)/i;

const speciesTokens = new Set(['rat', 'mouse', 'rabbit', 'dog', 'guinea pig', 'human', 'fish', 'trout', 'daphnia', 'algae']);
const routeTokens = new Set(['oral', 'dermal', 'inhalation', 'ip', 'iv', 'sc']);

type PugViewValue =
  | undefined
  | null
  | string
  | number
  | {
      String?: string;
      StringWithMarkup?: { String?: string; Markup?: { URL?: string; Extra?: string }[] }[];
    };

type PugViewNode = {
  TOCHeading?: string;
  Information?: { Name?: string; Value?: PugViewValue }[];
  Section?: PugViewNode[];
  [key: string]: unknown;
};

export function getStringFromPugValue(value: PugViewValue): string {
  if (value === undefined || value === null) {
    return '';
  }
  if (typeof value === 'string') {
    return value.trim();
  }
  if (typeof value === 'number') {
    return String(value);
  }
  const swm = value.StringWithMarkup;
  if (Array.isArray(swm)) {
    const parts = swm
      .map((item) => item.String)
      .filter((part): part is string => Boolean(part));
    if (parts.length > 0) {
      return parts.join(' ').trim();
    }
  }
  return value.String ?? '';
}

function getReferenceUrls(value: PugViewValue): string[] {
  if (!value || typeof value !== 'object' || !Array.isArray(value.StringWithMarkup)) {
    return [];
  }

  return value.StringWithMarkup.flatMap((item) => item.Markup ?? [])
    .map((markup) => markup.URL)
    .filter((url): url is string => Boolean(url))
    .slice(0, 5);
}

function walkNodes(node: unknown, visit: (node: PugViewNode) => void) {
  if (Array.isArray(node)) {
    node.forEach((child) => walkNodes(child, visit));
    return;
  }
  if (!node || typeof node !== 'object') {
    return;
  }

  const pugNode = node as PugViewNode;
  visit(pugNode);
  Object.values(pugNode).forEach((value) => walkNodes(value, visit));
}

function uniqueCodes(codes: string[]): string[] {
  return Array.from(new Set(codes.map((code) => code.replace(/\s*\([^)]+\)/g, '').trim()).filter(Boolean)));
}

export function extractGhsClassification(pugView: unknown): GhsClassification {
  const result: GhsClassification = { hCodes: [], pCodes: [], signalWord: '', pictograms: [] };

  walkNodes(pugView, (node) => {
    const heading = String(node.TOCHeading ?? '');
    if (!heading.includes('GHS') && !heading.includes('Classification')) {
      return;
    }

    for (const info of node.Information ?? []) {
      const name = info.Name ?? '';
      const text = getStringFromPugValue(info.Value);
      const lowerName = name.toLowerCase();

      if (lowerName.includes('pictogram') && info.Value && typeof info.Value === 'object') {
        for (const item of info.Value.StringWithMarkup ?? []) {
          for (const markup of item.Markup ?? []) {
            if (markup.Extra && !result.pictograms.includes(markup.Extra)) {
              result.pictograms.push(markup.Extra);
            }
          }
        }
      } else if (lowerName.includes('signal')) {
        result.signalWord = text || name;
      } else if (lowerName.includes('hazard') && text) {
        result.hCodes.push(...uniqueCodes(text.match(hCodePattern) ?? []));
      } else if (lowerName.includes('precautionary') && text) {
        result.pCodes.push(...uniqueCodes(text.match(pCodePattern) ?? []));
      }
    }
  });

  result.hCodes = uniqueCodes(result.hCodes);
  result.pCodes = uniqueCodes(result.pCodes);
  return result;
}

export function extractHazardMetrics(pugView: unknown) {
  const metrics = {
    flashPoint: [] as string[],
    vaporPressure: [] as string[],
    nfpa: [] as string[],
    iarc: [] as string[],
    prop65: [] as string[],
  };

  const record = (pugView as { Record?: { Section?: PugViewNode[] } } | undefined)?.Record;

  const visitSection = (section: PugViewNode, inheritedHeading = '') => {
    const heading = section.TOCHeading ?? inheritedHeading;
    for (const info of section.Information ?? []) {
      const name = info.Name ?? '';
      const text = getStringFromPugValue(info.Value);
      const nameLower = name.toLowerCase();
      const headingLower = heading.toLowerCase();
      if (!text) {
        continue;
      }

      if (nameLower.includes('flash point') || headingLower.includes('flash point')) {
        metrics.flashPoint.push(text);
      }
      if (nameLower.includes('vapor pressure') || headingLower.includes('vapor pressure')) {
        metrics.vaporPressure.push(text);
      }
      if (nameLower.includes('nfpa') || headingLower.includes('nfpa')) {
        metrics.nfpa.push(text);
      }
      if (nameLower.includes('iarc') || headingLower.includes('iarc')) {
        metrics.iarc.push(text);
      }
      if (nameLower.includes('proposition 65') || nameLower.includes('prop 65')) {
        metrics.prop65.push(text);
      }
    }

    for (const child of section.Section ?? []) {
      visitSection(child, heading);
    }
  };

  for (const section of record?.Section ?? []) {
    visitSection(section);
  }

  return {
    flashPoint: Array.from(new Set(metrics.flashPoint)),
    vaporPressure: Array.from(new Set(metrics.vaporPressure)),
    nfpa: Array.from(new Set(metrics.nfpa)).join('; ') || undefined,
    iarc: Array.from(new Set(metrics.iarc)).join('; ') || undefined,
    prop65: Array.from(new Set(metrics.prop65)).join('; ') || undefined,
  };
}

export function extractToxicities(pugView: unknown): ToxicityEntry[] {
  const toxicities: ToxicityEntry[] = [];
  const toxKeywords = ['tox', 'safety', 'hazard', 'health', 'exposure', 'pharmacokinetics', 'carcinogen'];
  const record = (pugView as { Record?: { Section?: PugViewNode[] } } | undefined)?.Record;

  const visitSection = (section: PugViewNode, inheritedHeading = '') => {
    const heading = section.TOCHeading ?? inheritedHeading;
    const shouldExtract = toxKeywords.some((keyword) => heading.toLowerCase().includes(keyword));

    if (shouldExtract) {
      for (const info of section.Information ?? []) {
        const value = getStringFromPugValue(info.Value);
        if (!value) {
          continue;
        }

        const speciesRoute = Array.from(new Set(value.match(speciesRoutePattern)?.map((part) => part.toLowerCase()) ?? []));
        const baseEntry: Omit<ToxicityEntry, 'route' | 'species'> = {
          type: info.Name || 'Toxicity',
          value: value.slice(0, 400),
          unit: value.match(unitPattern)?.[0],
          speciesRoute: speciesRoute.length > 0 ? speciesRoute : undefined,
          sourceSection: heading,
          referenceUrls: getReferenceUrls(info.Value),
        };
        const { route, species } = classifyRouteAndSpecies(baseEntry);
        toxicities.push({ ...baseEntry, route, species });
      }
    }

    for (const child of section.Section ?? []) {
      visitSection(child, heading);
    }
  };

  for (const section of record?.Section ?? []) {
    visitSection(section);
  }

  return toxicities;
}

function classifyRouteAndSpecies(entry: Pick<ToxicityEntry, 'value' | 'speciesRoute'>): { route: string; species: string } {
  const value = entry.value.toLowerCase();
  const speciesRoute = entry.speciesRoute ?? [];
  const speciesParts = speciesRoute.filter((part) => speciesTokens.has(part) || (!routeTokens.has(part) && part !== 'ld50' && part !== 'lc50'));
  const routeParts = speciesRoute.filter((part) => routeTokens.has(part));
  let species = speciesParts.length > 0 ? Array.from(new Set(speciesParts)).join(', ') : '—';
  let route = 'Other';

  if (/(fish|trout|daphnia|algae|aquatic)/.test(value)) {
    route = 'Ecotoxicity (aquatic)';
    if (species === '—') {
      species = value.includes('fish') || value.includes('trout') ? 'fish' : value.includes('daphnia') ? 'Daphnia' : value.includes('algae') ? 'algae' : 'aquatic';
    }
  } else if (value.includes('dermal') || value.includes('skin') || routeParts.includes('dermal')) {
    route = 'Dermal';
    if (species === '—' && value.includes('rabbit')) {
      species = 'rabbit';
    } else if (species === '—' && value.includes('rat')) {
      species = 'rat';
    }
  } else if (value.includes('inhalation') || value.includes('inhaled') || value.includes('mg/m') || value.includes('ppm') || routeParts.includes('inhalation')) {
    route = 'Inhalation';
    if (species === '—' && value.includes('rat')) {
      species = 'rat';
    }
  } else if (value.includes('oral') || value.includes('po ') || routeParts.includes('oral') || (value.includes('rat') && !value.includes('dermal') && !value.includes('inhalation'))) {
    route = 'Oral';
    if (species === '—' && value.includes('rat')) {
      species = 'rat';
    } else if (species === '—' && value.includes('mouse')) {
      species = 'mouse';
    }
  }

  return { route, species };
}

export function prioritizeToxicityData(toxicities: ToxicityEntry[]): PrioritizedToxicityItem[] {
  return toxicities
    .map((toxicity) => {
      const hasNumericValue = /^[<>~]?\s*\d+(?:[.,]\d+)?/.test(toxicity.value);
      return {
        bucket: toxicity.unit && hasNumericValue ? 'Quantitative' : 'Categorical',
        source: 'PubChem',
        endpoint: toxicity.type.trim() || 'Toxicity',
        value: toxicity.value,
        units: toxicity.unit ?? '',
        species: toxicity.species,
        route: toxicity.route,
        details: toxicity.sourceSection,
      } satisfies PrioritizedToxicityItem;
    })
    .sort((a, b) => (a.bucket === b.bucket ? 0 : a.bucket === 'Quantitative' ? -1 : 1));
}

function parseEcotoxText(raw: string): Partial<EcotoxicityEntry> {
  const endpoint = raw.match(ecotoxEndpointPattern)?.[1]?.toUpperCase();
  const duration = raw.match(ecotoxDurationPattern)?.[1];
  const valueMatch = raw.match(ecotoxValueUnitPattern);
  const ciMatch = raw.match(ecotoxCiPattern);
  const parsed: Partial<EcotoxicityEntry> = { endpoint, duration, conditions: raw };

  if (valueMatch) {
    parsed.unit = valueMatch[2];
    parsed.valueNum = Number(valueMatch[1].replace(/[<>~\s,]/g, ''));
  }

  if (ciMatch) {
    parsed.ciLow = Number(ciMatch[1]);
    parsed.ciHigh = Number(ciMatch[2]);
  }

  return parsed;
}

export function extractEcotoxicity(ghs: GhsClassification, toxicities: ToxicityEntry[]): EcotoxicitySummary {
  const summary: EcotoxicitySummary = {
    hCodesAquatic: ghs.hCodes.filter((code) => code.startsWith('H4')),
    entries: [],
  };

  for (const toxicity of toxicities) {
    const valueLower = toxicity.value.toLowerCase();
    if (!/(fish|trout|daphnia|algae|aquatic)/.test(valueLower)) {
      continue;
    }

    const parsed = parseEcotoxText(toxicity.value);
    const species = valueLower.includes('fish') || valueLower.includes('trout')
      ? 'fish'
      : valueLower.includes('daphnia')
        ? 'Daphnia'
        : valueLower.includes('algae')
          ? 'algae'
          : 'aquatic';

    const entry: EcotoxicityEntry = {
      value: toxicity.value,
      species,
      unit: parsed.unit ?? 'mg/L',
      ...parsed,
    };

    summary.entries.push(entry);

    if (entry.valueNum !== undefined) {
      if (entry.endpoint?.startsWith('EC')) {
        summary.aquaticEc50MgL = entry.valueNum;
      } else if (summary.aquaticLc50MgL === undefined) {
        summary.aquaticLc50MgL = entry.valueNum;
      }
      summary.aquaticValueRaw = toxicity.value.slice(0, 250);
      summary.aquaticSpecies = summary.aquaticSpecies ?? species;
    }
  }

  return summary;
}

function bandFromValue(value: number, bands: [number, number][]): number {
  return bands.find(([threshold]) => value <= threshold)?.[1] ?? 5;
}

function firstMetric(regex: RegExp, toxicities: ToxicityEntry[], predicate?: (value: string) => boolean): number | undefined {
  for (const toxicity of toxicities) {
    const value = toxicity.value.toLowerCase();
    if (predicate && !predicate(value)) {
      continue;
    }
    const match = value.match(regex);
    if (match) {
      return Number(match[1].replace(',', ''));
    }
  }
  return undefined;
}

function exposureBand(value: number, metric: ExposureBand['metric'], band: number): ExposureBand {
  return { value, metric, band, source: 'PubChem' };
}

export function computeExposureBands(toxicities: ToxicityEntry[]): ExposureBands {
  const oralLd50 = firstMetric(/(\d+(?:\.\d+)?)\s*mg\s*\/\s*kg/, toxicities, (value) =>
    value.includes('ld50') && !value.includes('dermal') && !value.includes('skin'),
  );
  const dermalLd50 = firstMetric(/(\d+(?:\.\d+)?)\s*mg\s*\/\s*kg/, toxicities, (value) =>
    value.includes('ld50') && (value.includes('dermal') || value.includes('skin')),
  );
  const inhalationLc50 = firstMetric(/(\d+(?:[.,]\d+)*)\s*mg\s*\/\s*m(?:3|³)/, toxicities, (value) => value.includes('lc50'));

  return {
    oral:
      oralLd50 !== undefined
        ? exposureBand(oralLd50, 'ld50_mg_kg', bandFromValue(oralLd50, [[5, 1], [50, 2], [300, 3], [2000, 4], [5000, 5]]))
        : undefined,
    dermal:
      dermalLd50 !== undefined
        ? exposureBand(dermalLd50, 'ld50_mg_kg', bandFromValue(dermalLd50, [[50, 1], [200, 2], [1000, 3], [2000, 4], [5000, 5]]))
        : undefined,
    inhalation:
      inhalationLc50 !== undefined
        ? exposureBand(
            inhalationLc50,
            'lc50_mg_m3',
            inhalationLc50 <= 100 ? 1 : inhalationLc50 <= 500 ? 2 : inhalationLc50 <= 2500 ? 3 : inhalationLc50 <= 20000 ? 4 : 5,
          )
        : undefined,
  };
}
