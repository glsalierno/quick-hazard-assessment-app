import AsyncStorage from "@react-native-async-storage/async-storage";

import type { AssessmentHistoryItem, ChemicalAssessment } from "../types/chemical";

const HISTORY_KEY = "quick-hazard-assessment:history:v1";
const MAX_HISTORY_ITEMS = 25;

export async function getHistory(): Promise<AssessmentHistoryItem[]> {
  const raw = await AsyncStorage.getItem(HISTORY_KEY);
  if (!raw) {
    return [];
  }
  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

export async function saveAssessmentToHistory(assessment: ChemicalAssessment) {
  const history = await getHistory();
  const title = assessment.dsstox?.preferredName ?? assessment.iupacName ?? assessment.normalizedQuery;
  const item: AssessmentHistoryItem = {
    id: `${assessment.cid}-${assessment.assessedAt}`,
    query: assessment.normalizedQuery,
    title,
    subtitle: assessment.formula,
    assessedAt: assessment.assessedAt,
    assessment,
  };

  const deduped = history.filter((existing) => existing.assessment.cid !== assessment.cid);
  const nextHistory = [item, ...deduped].slice(0, MAX_HISTORY_ITEMS);
  await AsyncStorage.setItem(HISTORY_KEY, JSON.stringify(nextHistory));
  return nextHistory;
}

export async function clearHistory() {
  await AsyncStorage.removeItem(HISTORY_KEY);
}
