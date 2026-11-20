import { DsstoxRecord } from './types';

const bundledDsstoxExamples: Record<string, DsstoxRecord> = {
  '67-64-1': { cas: '67-64-1', dtxsid: 'DTXSID8021482', preferredName: 'Acetone' },
  '64-17-5': { cas: '64-17-5', dtxsid: 'DTXSID9020584', preferredName: 'Ethanol' },
  '71-43-2': { cas: '71-43-2', dtxsid: 'DTXSID3039242', preferredName: 'Benzene' },
  '50-00-0': { cas: '50-00-0', dtxsid: 'DTXSID7020637', preferredName: 'Formaldehyde' },
};

export function getBundledDsstoxRecord(cas: string): DsstoxRecord | undefined {
  return bundledDsstoxExamples[cas.trim()];
}
