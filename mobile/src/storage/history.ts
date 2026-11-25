import AsyncStorage from '@react-native-async-storage/async-storage';

import { ChemicalReport } from '../domain/types';

const historyKey = 'quick-hazard-assessment/history/v1';
const maxHistoryItems = 25;

export async function loadHistory(): Promise<ChemicalReport[]> {
  const raw = await AsyncStorage.getItem(historyKey);
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

export async function saveReportToHistory(report: ChemicalReport): Promise<void> {
  const existing = await loadHistory();
  const deduped = existing.filter((item) => item.id !== report.id && item.normalizedQuery !== report.normalizedQuery);
  await AsyncStorage.setItem(historyKey, JSON.stringify([report, ...deduped].slice(0, maxHistoryItems)));
}

export async function clearHistory(): Promise<void> {
  await AsyncStorage.removeItem(historyKey);
}
