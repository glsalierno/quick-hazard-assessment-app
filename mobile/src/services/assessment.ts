import type { ChemicalAssessment, PrioritizedToxicity, ToxicityEntry } from "../types/chemical";

function hasNumericValue(value: string | number | undefined) {
  if (value == null) {
    return false;
  }
  return /^[<>~]?\s*\d+(?:[.,]\d+)?/.test(String(value).trim());
}

export function prioritizeToxicityData(assessment: ChemicalAssessment): PrioritizedToxicity {
  return assessment.toxicities.reduce<PrioritizedToxicity>(
    (accumulator, toxicity) => {
      if (toxicity.unit && hasNumericValue(toxicity.value)) {
        accumulator.quantitative.push(toxicity);
      } else {
        accumulator.categorical.push(toxicity);
      }
      return accumulator;
    },
    { quantitative: [], categorical: [] },
  );
}

export function getOverallHazardBadges(assessment: ChemicalAssessment) {
  const badges: Array<{ label: string; tone: "danger" | "warning" | "info" }> = [];
  const hCodes = assessment.ghs.hCodes;

  if (assessment.ghs.signalWord.toLowerCase() === "danger") {
    badges.push({ label: "Signal word: Danger", tone: "danger" });
  } else if (assessment.ghs.signalWord) {
    badges.push({ label: `Signal word: ${assessment.ghs.signalWord}`, tone: "warning" });
  }

  if (hCodes.some((code) => ["H340", "H350", "H360"].includes(code))) {
    badges.push({ label: "Chronic health hazard", tone: "danger" });
  }
  if (hCodes.some((code) => ["H220", "H225", "H226"].includes(code))) {
    badges.push({ label: "Flammability hazard", tone: "warning" });
  }
  if (hCodes.some((code) => code.startsWith("H4"))) {
    badges.push({ label: "Aquatic hazard", tone: "info" });
  }
  if (assessment.exposureBands.oral.band && assessment.exposureBands.oral.band <= 3) {
    badges.push({ label: `Oral acute tox band ${assessment.exposureBands.oral.band}`, tone: "warning" });
  }

  return badges.length > 0 ? badges : [{ label: "No major GHS flags found", tone: "info" as const }];
}

export function buildReportRows(assessment: ChemicalAssessment) {
  return [
    { label: "CAS / query", value: assessment.normalizedQuery },
    { label: "PubChem CID", value: String(assessment.cid) },
    { label: "IUPAC name", value: assessment.iupacName ?? "N/A" },
    { label: "Formula", value: assessment.formula ?? "N/A" },
    { label: "Molecular weight", value: assessment.molecularWeight ? `${assessment.molecularWeight} g/mol` : "N/A" },
    { label: "Flash point", value: assessment.flashPoint.join(" | ") || "N/A" },
    { label: "Vapor pressure", value: assessment.vaporPressure.join(" | ") || "N/A" },
  ];
}

export function sortToxicityForDisplay(toxicities: ToxicityEntry[]) {
  return [...toxicities].sort((left, right) => {
    const leftHasUnit = left.unit ? 0 : 1;
    const rightHasUnit = right.unit ? 0 : 1;
    return leftHasUnit - rightHasUnit || left.route.localeCompare(right.route);
  });
}
