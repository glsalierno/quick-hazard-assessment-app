export const GHS_H_PHRASES: Record<string, string> = {
  H220: "Extremely flammable gas.",
  H225: "Highly flammable liquid and vapour.",
  H226: "Flammable liquid and vapour.",
  H302: "Harmful if swallowed.",
  H312: "Harmful in contact with skin.",
  H314: "Causes severe skin burns and eye damage.",
  H315: "Causes skin irritation.",
  H319: "Causes serious eye irritation.",
  H332: "Harmful if inhaled.",
  H335: "May cause respiratory irritation.",
  H340: "May cause genetic defects.",
  H350: "May cause cancer.",
  H351: "Suspected of causing cancer.",
  H360: "May damage fertility or the unborn child.",
  H373: "May cause damage to organs through prolonged or repeated exposure.",
  H400: "Very toxic to aquatic life.",
  H410: "Very toxic to aquatic life with long lasting effects.",
  H411: "Toxic to aquatic life with long lasting effects.",
};

export const GHS_P_PHRASES: Record<string, string> = {
  P210: "Keep away from heat, hot surfaces, sparks, open flames and other ignition sources. No smoking.",
  P261: "Avoid breathing dust/fume/gas/mist/vapours/spray.",
  P273: "Avoid release to the environment.",
  P280: "Wear protective gloves/protective clothing/eye protection/face protection/hearing protection/...",
  "P305+P351+P338":
    "IF IN EYES: Rinse continuously with water for several minutes. Remove contact lenses, if present and easy to do. Continue rinsing.",
  P310: "Immediately call a POISON CENTER or doctor/physician.",
  P501: "Dispose of contents/container to ...",
};

export function getHPhrase(code: string) {
  const normalized = code.trim();
  return GHS_H_PHRASES[normalized] ?? `${normalized}: (phrase not found)`;
}

export function getPPhrase(code: string) {
  const normalized = code.trim();
  return GHS_P_PHRASES[normalized] ?? `${normalized}: (phrase not found)`;
}
