import type { AssessmentHistoryItem, ChemicalAssessment } from "../types/chemical";

export type RootStackParamList = {
  Start: undefined;
  Assessment: { query: string };
  Results: { assessment: ChemicalAssessment };
  History: undefined;
  HistoryDetail: { item: AssessmentHistoryItem };
};
