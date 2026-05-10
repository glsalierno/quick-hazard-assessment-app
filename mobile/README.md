# Quick Hazard Assessment Mobile

Native Expo/React Native implementation of the Streamlit Quick Hazard Assessment app.

## Quick start

```bash
cd mobile
npm install
npm run start
```

Then press:

- `a` for Android emulator/device
- `i` for iOS simulator on macOS
- scan the QR code with Expo Go

## What works in this first mobile version

- Native stack navigation, not a wrapped Streamlit web view
- CAS number or chemical name input
- PubChem CID resolution and property lookup
- PUG View parsing for GHS H/P codes, signal word, physical hazards, toxicity endpoints, ecotoxicity, and acute exposure bands
- TypeScript data models for PubChem, DSSTox, ToxValDB-compatible fields, GHS, toxicity, and history
- On-device assessment history with AsyncStorage

## Useful scripts

```bash
npm run typecheck
npm run android
npm run ios
npm run web
```

## Data-source note

The Streamlit app can read `data/chemical_db.sqlite` directly from Python. Expo mobile apps cannot use that server-side SQLite connection as-is. This app keeps the same DSSTox/ToxValDB fields in the TypeScript model so the next step can either:

1. expose the Python lookup through a small API, or
2. bundle a mobile SQLite database with `expo-sqlite`.

PubChem lookup is implemented directly in the mobile app and is immediately runnable.
