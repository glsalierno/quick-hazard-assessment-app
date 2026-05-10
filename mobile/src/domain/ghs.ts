const hPhrases: Record<string, string> = {
  H220: 'Extremely flammable gas.',
  H225: 'Highly flammable liquid and vapour.',
  H226: 'Flammable liquid and vapour.',
  H302: 'Harmful if swallowed.',
  H312: 'Harmful in contact with skin.',
  H314: 'Causes severe skin burns and eye damage.',
  H315: 'Causes skin irritation.',
  H319: 'Causes serious eye irritation.',
  H332: 'Harmful if inhaled.',
  H335: 'May cause respiratory irritation.',
  H340: 'May cause genetic defects.',
  H350: 'May cause cancer.',
  H351: 'Suspected of causing cancer.',
  H360: 'May damage fertility or the unborn child.',
  H373: 'May cause damage to organs through prolonged or repeated exposure.',
  H400: 'Very toxic to aquatic life.',
  H410: 'Very toxic to aquatic life with long lasting effects.',
  H411: 'Toxic to aquatic life with long lasting effects.',
};

const pPhrases: Record<string, string> = {
  P210: 'Keep away from heat, hot surfaces, sparks, open flames and other ignition sources. No smoking.',
  P261: 'Avoid breathing dust/fume/gas/mist/vapours/spray.',
  P273: 'Avoid release to the environment.',
  P280: 'Wear protective gloves/protective clothing/eye protection/face protection/hearing protection.',
  'P305+P351+P338':
    'IF IN EYES: Rinse continuously with water for several minutes. Remove contact lenses, if present and easy to do. Continue rinsing.',
  P310: 'Immediately call a POISON CENTER or doctor/physician.',
  P501: 'Dispose of contents/container in accordance with applicable regulations.',
};

export function getHazardPhrase(code: string): string | undefined {
  return hPhrases[code.trim()];
}

export function getPrecautionaryPhrase(code: string): string | undefined {
  return pPhrases[code.trim()];
}

export function phrasePairs(codes: string[], type: 'h' | 'p') {
  const phraseGetter = type === 'h' ? getHazardPhrase : getPrecautionaryPhrase;
  return codes
    .map((code) => ({ code, phrase: phraseGetter(code) }))
    .filter((item): item is { code: string; phrase: string } => Boolean(item.phrase));
}
