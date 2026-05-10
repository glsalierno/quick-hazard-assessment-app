# Quick Hazard Assessment Mobile

Native mobile version of the Streamlit Quick Hazard Assessment app, built with Expo and React Native.

## Quick start

```bash
cd mobile
npm install
npm run android
# or: npm run ios
```

For local development without an emulator:

```bash
cd mobile
npm start
```

Scan the QR code with Expo Go, then search for `67-64-1` or `acetone`.

## What is implemented

- Native stack navigation for search, results, and history screens.
- TypeScript domain models for chemical reports, GHS, toxicity endpoints, ecotoxicity, exposure bands, and DSSTox identifiers.
- PubChem REST and PUG View retrieval from the device.
- Mobile equivalent of the Python extraction logic for:
  - GHS H/P codes and signal word
  - Flash point, vapor pressure, NFPA, IARC, Prop 65 text
  - Toxicity endpoint extraction and quantitative-first prioritization
  - Aquatic ecotoxicity parsing
  - GHS-style LD50/LC50 exposure bands
- AsyncStorage local history for offline review of previously fetched reports.
- Bundled DSSTox identifiers for the four example chemicals from the Streamlit app.

The original Streamlit app does not contain a questionnaire, scoring table, or decision tree. Its "assessment" is a lookup, extraction, prioritization, and report-display workflow.

## Verification

```bash
cd mobile
npm run typecheck
npx expo install --check
npx expo export --platform android
```

See `docs/MOBILE_MIGRATION_PLAN.md` for the framework recommendation, implementation phases, and MCP recommendations.
