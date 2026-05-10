# Mobile migration plan for Quick Hazard Assessment

## Existing Streamlit app analysis

The current app is a chemical hazard lookup and report generator, not a multi-question decision-tree questionnaire.

- **Hazard logic:** `app.py` accepts a CAS number or chemical name, normalizes CAS-like input, resolves DSSTox/ToxValDB identifiers when local data exists, fetches PubChem data, then displays identifiers, molecular properties, GHS H/P codes, signal word, physical hazards, toxicity endpoints, ecotoxicity, and downloadable reports. `utils/pubchem_client.py` contains the main extraction heuristics for GHS codes, flash point, vapor pressure, LD50/LC50 text, ecotoxicity, and exposure bands.
- **Scoring / decision tree:** there is no explicit user-question score or branching questionnaire. The closest decision logic is heuristic classification: route/species inference, quantitative-vs-categorical toxicity prioritization, aquatic hazard extraction, and acute exposure-band calculation.
- **Persistence:** Streamlit uses `st.session_state` for the active query, cached result, and display preferences. Durable data is local source data: `DSS/cas_dtxsid_mapping.csv` and `data/chemical_db.sqlite` for DSSTox/ToxValDB. Exports are generated CSV/JSON downloads.
- **UI structure:** the Streamlit UI is one centered page with a sidebar database status panel, a search form, example buttons, conditional result sections, tabs for toxicity views, expanders for raw data and GHS options, and download buttons.

## Framework recommendation

Use **React Native with Expo** for this app.

Why it fits this codebase:

- The existing business logic is mostly JSON parsing, REST calls, string normalization, and typed report shaping. That ports directly into TypeScript pure functions.
- Expo gives native stack navigation, local storage, networking, Android/iOS builds, and emulator workflows with less project overhead than a full native setup.
- The Streamlit UI maps naturally to mobile cards, chips, collapsible sections, and history screens.
- Offline history is straightforward with AsyncStorage; full offline DSSTox/ToxValDB can later use `expo-sqlite` or a small API without changing the assessment model.
- Flutter is also viable, but Dart would require a larger rewrite of parsing utilities and less direct sharing with the web/REST ecosystem.

## Generated app

The runnable Expo app is in `mobile/`.

```bash
cd mobile
npm install
npm run start
```

## Phase 0: Expo project and native navigation

Implemented files:

- `mobile/App.tsx`
- `mobile/src/navigation/RootNavigator.tsx`
- `mobile/src/navigation/types.ts`
- `mobile/src/screens/StartScreen.tsx`
- `mobile/src/screens/AssessmentScreen.tsx`
- `mobile/src/screens/ResultsScreen.tsx`
- `mobile/src/screens/HistoryScreen.tsx`

Navigation is a native stack:

```text
Start -> Assessment -> Results
Start -> History -> HistoryDetail
```

This gives a native-like flow instead of embedding the Streamlit app in a WebView.

## Phase 1: Recreate data models in TypeScript

Implemented in `mobile/src/types/chemical.ts`.

Key models:

- `ChemicalAssessment`
- `GhsClassification`
- `ToxicityEntry`
- `EcotoxicitySummary`
- `ExposureBand`
- `DsstoxRecord`
- `AssessmentHistoryItem`

The models include DSSTox/ToxValDB-compatible fields even though the first runnable mobile version calls PubChem directly.

## Phase 2: Implement hazard assessment logic as pure functions

Implemented in:

- `mobile/src/utils/cas.ts`
- `mobile/src/services/pubchem.ts`
- `mobile/src/services/assessment.ts`
- `mobile/src/data/ghsPhrases.ts`

Ported behavior from Python:

- CAS normalization and checksum validation
- PubChem CID resolution for CAS/name
- PubChem property fetch
- PUG View traversal
- GHS H/P code extraction
- Signal word extraction
- Flash point and vapor pressure extraction
- Toxicity text extraction
- Route/species classification
- Ecotoxicity LC50/EC50 parsing
- Acute oral/dermal/inhalation exposure bands
- Prioritized quantitative/categorical toxicity display

## Phase 3: Build mobile screens

Implemented screens:

- **Start screen:** native text input, example chemical buttons, local-history entry point.
- **Assessment screen:** loading/error state while fetching and classifying data.
- **Results screen:** card-based report with identifiers, hazard badges, GHS phrases, toxicity endpoints, and ecotoxicity.
- **History screen:** locally saved assessments with detail navigation.

## Phase 4: Add local storage

Implemented in `mobile/src/store/historyStore.ts` with AsyncStorage.

This replaces Streamlit session-state result retention with durable device-local history:

- saves the most recent 25 assessments
- deduplicates by PubChem CID
- supports clearing local history

Future DSSTox/ToxValDB options:

1. **Recommended for larger datasets:** expose the existing Python SQLite lookup as a small backend API and call it from mobile.
2. **Recommended for true offline mode:** package a compact mobile SQLite database and query it with `expo-sqlite`.

## Phase 5: Mobile-specific polish

Implemented:

- native stack headers
- card layouts
- large touch targets
- chips/badges for high-level hazard cues
- local history gestures via pressable cards
- responsive single-column layout suitable for phones

Recommended next polish:

- add collapsible result sections for very long toxicity lists
- add share/export using Expo Sharing
- render molecular structures using a native-safe image service or a small backend endpoint
- add offline DSSTox/ToxValDB SQLite packaging if network-free use is required

## MCP recommendations

Only these MCPs would directly speed this project up:

- **Android/ADB MCP:** useful for installing the Expo dev build, reading device logs, inspecting network failures, and reproducing Android-specific layout/runtime issues.
- **Emuluxe or emulator-control MCP:** useful if you want automated visual smoke tests across common phone sizes and OS versions.

I would not add generic database or browser MCPs for this mobile task unless the DSSTox/ToxValDB migration becomes an API/backend project. For the current Expo app, emulator/device debugging MCPs provide the most direct benefit.
