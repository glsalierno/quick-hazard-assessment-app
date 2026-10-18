# GHhaz6 golden panel (manual regression)

Assess each CAS in the Streamlit app after v6 changes. Confirm GHS accounting, ecotox cleanliness, preferred name, and OPERA warnings where applicable.

| CAS | Chemical | Why included |
|-----|----------|--------------|
| `381-73-7` | Difluoroacetic acid | Primary QA case: GHS table may include H314/H227/H318/H402/H410 — all must appear as displayed, unmapped, or subsumed (not silently dropped). Ecotox must not show general classification text. |
| `67-56-1` | Methanol | Common solvent; strong PubChem GHS/toxicity coverage. |
| `71-43-2` | Benzene | Carcinogen flag / chronic hazard evidence path. |
| `7732-18-5` | Water | Negative / sparse hazard control (should not invent hazards). |
| `7647-01-0` | Hydrochloric acid | Corrosive (H314); H318 often co-listed. H318 may be listed as subsumed by H314 with reason. |
| `50-00-0` | Formaldehyde | Dense multi-endpoint tox; prioritized table quality. |
| `108-88-3` | Toluene | Physchem + aquatic codes; OPERA/PubChem logP comparison if OPERA runs. |
| `1607-31-4` | (2-Oxo-1,3-dioxolan-4-yl)methyl acetate | CID resolution edge case (RegistryID 404 → RN xref). |

## Checks per compound

1. Summary preferred name not blank if IUPAC/PubChem name exists.
2. QA expander: `retrieved_h_codes == displayed + unmapped + suppressed`.
3. Ecotoxicity endpoint table has no “Flammable liquids” / “Skin corrosion” contamination.
4. Prioritized toxicity has no blank endpoint+value rows.
5. Optional: OPERA expander shows identity / logP disagreement warnings when applicable.
