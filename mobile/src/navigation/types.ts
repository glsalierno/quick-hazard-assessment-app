import { ChemicalReport } from '../domain/types';

export type RootStackParamList = {
  Start: undefined;
  Results: { report: ChemicalReport };
  History: undefined;
};
