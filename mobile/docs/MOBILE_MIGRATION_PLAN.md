# Mobile migration plan

## Existing Streamlit app analysis

The current app (`app.py`) is a chemical hazard report generator, not a questionnaire. It accepts a CAS number or chemical name, normalizes the input, looks up DSSTox/ToxValDB local data when available, retrieves PubChem/PUG View data, and renders a formatted report.

### Business logic

- `utils/cas_validator.py`
  - Normalizes input such as `67-64-1 (Acetone)`.
  - Detects CAS format and supports checksum validation.
- `utils/pubchem_client.py`
  - Resolves CAS/name to PubChem CID.
  - Fetches core compound properties.
  - Parses PUG View data for:
    - GHS H/P codes, signal word, pictograms
    - Flash point, vapor pressure, NFPA, IARC, Prop 65 text
    - Toxicity snippets and units
    - Route/species inference
    - Aquatic ecotoxicity
    - GHS-style exposure bands from LD50/LC50 values
- `utils/data_formatter.py`
  - Prioritizes quantitative toxicity rows before categorical rows.
  - Builds CSV/JSON export payloads.
- `utils/ghs_formatter.py`
  - Expands common H/P codes to human-readable phrases.

There is no scoring table or decision tree in the repository. The closest decision logic is route/species classification, quantitative-vs-categorical prioritization, and exposure-band calculation.

### Persistence

- Streamlit session state caches the current query, fetched result, and display preferences.
- Optional local persistence is in `data/chemical_db.sqlite`, but the file in this checkout is a Git LFS pointer. When present, it contains DSSTox and ToxValDB tables.
- `DSS/cas_dtxsid_mapping.csv` is present as a CSV mapping of CASRN to DTXSID.
- Report download is generated dynamically as CSV/JSON; no server-side history is stored.

### UI structure

- A single Streamlit page with:
  - Search form and example buttons
  - Sidebar local database status
  - Molecular structure section
  - Identifiers and key properties
  - Toxicity endpoint tabs
  - Ecotoxicity section
  - GHS classification display options
  - CSV/JSON download buttons

## Framework recommendation

Use **React Native with Expo** for this specific app.

Why Expo/RN fits:

- The app is already API/data-formatting heavy, and TypeScript maps directly to the Python dictionaries and pure functions.
- Expo provides fast native navigation, image loading, fetch, and local storage without building native modules first.
- PubChem structure images can be rendered with native `Image`, avoiding WebView wrapping.
- Offline capability needed here is report history/cache, which AsyncStorage covers well for phase one.
- A future full DSSTox/ToxValDB mobile database can use `expo-sqlite` or a backend sync without rewriting the UI.

Flutter would also work, but it is less efficient for this repository because the current logic ports naturally to TypeScript and the UI is standard form/report navigation rather than custom graphics-heavy screens.

## Phase 0: Project setup and navigation

Implemented files:

- `package.json`
- `App.tsx`
- `src/navigation/types.ts`
- `src/screens/StartScreen.tsx`
- `src/screens/ResultsScreen.tsx`
- `src/screens/HistoryScreen.tsx`

Runnable commands:

```bash
cd mobile
npm install
npm run android
```

Navigation stack:

```tsx
<Stack.Navigator>
  <Stack.Screen component={StartScreen} name="Start" />
  <Stack.Screen component={ResultsScreen} name="Results" />
  <Stack.Screen component={HistoryScreen} name="History" />
</Stack.Navigator>
```

## Phase 1: Recreate data models in TypeScript

Implemented in `src/domain/types.ts`.

Models include:

- `ChemicalReport`
- `GhsClassification`
- `ToxicityEntry`
- `PrioritizedToxicityItem`
- `EcotoxicitySummary`
- `ExposureBands`
- `DsstoxRecord`

These replace the loosely typed Streamlit dictionaries with explicit mobile-safe interfaces.

## Phase 2: Implement hazard assessment logic without UI

Implemented files:

- `src/domain/cas.ts`
- `src/domain/ghs.ts`
- `src/domain/dsstox.ts`
- `src/domain/hazardLogic.ts`
- `src/services/pubchem.ts`

The pure logic ports the Python workflow:

- CAS normalization and type detection
- PubChem CID and property resolution
- PUG View traversal
- GHS code extraction
- Hazard metric extraction
- Toxicity parsing
- Quantitative-first prioritization
- Ecotoxicity parsing
- LD50/LC50 exposure bands

Verification:

```bash
cd mobile
npm run typecheck
```

## Phase 3: Build mobile screens

Implemented screens:

- Start/search screen: `src/screens/StartScreen.tsx`
- Results screen: `src/screens/ResultsScreen.tsx`
- History screen: `src/screens/HistoryScreen.tsx`

The Streamlit layout was converted to mobile cards:

- Molecular structure
- Identifiers
- Key properties
- GHS classification
- Toxic doses and endpoints
- Ecotoxicity
- Exposure bands

## Phase 4: Add local storage

Implemented in `src/storage/history.ts` using AsyncStorage.

Behavior:

- Stores the latest 25 reports.
- Deduplicates by normalized query.
- Enables offline review of previously fetched reports.
- Replaces Streamlit session-state caching for mobile use.

Future local database option:

- Use `expo-sqlite` for a compact, indexed DSSTox/ToxValDB database if full offline lookup is required.
- Do not bundle the 362 MB SQLite LFS database directly in the app binary without pruning; it is too large for a first mobile release.

## Phase 5: Mobile-specific polish

Implemented:

- Native stack navigation
- Safe-area handling
- Native card layout
- Pressable example chips
- Offline report history
- Native PubChem structure image rendering

Good next polish steps:

- Add pull-to-refresh on a report.
- Add a share sheet for JSON/CSV export.
- Add `expo-sqlite` for a pruned offline DSSTox subset.
- Add EAS builds and device testing.
- Add accessibility labels to every report section.

## MCP recommendations

Only add MCPs that directly improve this project:

1. **Android/ADB MCP**
   - Useful once testing on Android emulators or physical devices.
   - Speeds up logcat inspection, app install/reload, screenshots, and network-debug workflows.

2. **Emuluxe or equivalent emulator-control MCP**
   - Useful for visual QA of the search, results, and history flows.
   - Helps automate taps, screenshots, and regression checks across screen sizes.

Not needed initially:

- Browser-only MCPs, because this is not a WebView migration.
- Database MCPs, unless you decide to build and inspect a pruned mobile SQLite DSSTox/ToxValDB database.

## Quick start

```bash
cd mobile
npm install
npm run android
```

Optional checks:

```bash
npm run typecheck
npx expo install --check
npx expo export --platform android
```
