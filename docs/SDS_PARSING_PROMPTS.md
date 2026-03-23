# Enhanced SDS Parsing Prompts for Cursor

Professional prompts to fix SDS parsing and improve the SDS extraction experience: restructure extraction to database-style tables, improve display, add section-aware parsing, and match SDS data to database records.

---

## Prompt 1: Restructure SDS Extraction to Database-Style Tables

```
Completely redesign the SDS PDF extraction output to match database query results:

1. **Target Output Structure**
   Instead of nested JSON, create pandas DataFrames that mirror database tables:

   ```python
   # Example of desired output structure
   {
       'hazard_classifications': pd.DataFrame({
           'source': ['SDS_section_2', 'SDS_section_2', ...],
           'hazard_class': ['Flam. Liq. 3', 'Acute Tox. 4', ...],
           'h_code': ['H226', 'H302', ...],
           'category': ['3', '4', ...],
           'confidence': ['high', 'medium', ...],
           'raw_text': ['Flammable liquid and vapor', 'Harmful if swallowed', ...]
       }),
       
       'physical_properties': pd.DataFrame({
           'property': ['flash_point', 'boiling_point', 'vapor_pressure', ...],
           'value': [116, 215, 0.3, ...],
           'unit': ['°C', '°C', 'mmHg', ...],
           'method': ['closed cup', 'estimated', 'measured', ...],
           'conditions': ['at 101.3 kPa', '20°C', '25°C', ...],
           'raw_text': ['Flash Point 116 °C', 'Boiling point 215°C', ...]
       }),
       
       'ecotoxicity': pd.DataFrame({
           'species': ['Daphnia magna', 'Pseudokirchneriella subcapitata', 'Leuciscus idus'],
           'endpoint': ['EC50', 'EC50', 'LC50'],
           'duration_h': [48, 72, 96],
           'value': [23.4, 15, 62],
           'unit': ['mg/L', 'mg/L', 'mg/L'],
           'raw_text': ['EC50: = 23.4 mg/L, 48h', 'EC50: = 15 mg/L, 72h', 'LC50: 62mg/L/96h']
       }),
       
       'ghs_information': pd.DataFrame({
           'element_type': ['signal_word', 'h_statement', 'p_statement', 'pictogram'],
           'value': ['Warning', 'H226, H302, H315', 'P210, P233, P280', 'GHS02, GHS07'],
           'source_section': ['section_2', 'section_2', 'section_2', 'section_2']
       })
   }
   ```

2. **Parsing Rules Enhancement**
   - Replace regex-only approach with hybrid parser:
     * Section detection (identify SDS sections 1-16)
     * Context-aware extraction (know if text is from Section 3, 9, 11, etc.)
     * Multi-line value capture (e.g., H-statements that span lines)
   - Implement validation:
     * Check extracted H-codes against GHS H-code list
     * Validate units against known patterns (°C, mg/L, etc.)
     * Flag ambiguous extractions for user review

3. **Table Normalization**
   - Ensure all tables have consistent columns:
     * `source_location`: PDF page/section where found
     * `extraction_confidence`: high/medium/low
     * `normalized_value`: standardized format for database matching
     * `original_value`: as extracted
   - Add lookup columns for joining with chemical database:
     * `matched_cas`: if chemical can be identified
     * `matched_name`: standardized name
```

---

## Prompt 2: User-Friendly Display of Extracted Data

```
Create a professional display for extracted SDS data that mirrors database search results:

1. **Tabbed Results View**
   Implement a multi-tab display matching database search layout:

   ```
   Tab 1: "GHS Classification" - Table showing:
   | Hazard Class | H-Code | Category | Signal Word | Pictograms |
   |--------------|--------|----------|--------------|------------|
   | Flammable Liquid | H226 | Category 3 | Warning | GHS02 |
   | Acute Toxicity | H302 | Category 4 | Warning | GHS07 |
   
   Tab 2: "Physical Properties" - Table:
   | Property | Value | Unit | Method | Conditions |
   |----------|-------|------|--------|------------|
   | Flash Point | 116 | °C | closed cup | 101.3 kPa |
   | Boiling Point | 215 | °C | estimated | - |
   
   Tab 3: "Ecotoxicity" - Table:
   | Species | Endpoint | Duration | Value | Unit |
   |---------|----------|----------|-------|------|
   | Daphnia magna | EC50 | 48h | 23.4 | mg/L |
   
   Tab 4: "Comparison" - Side-by-side with database data
   ```

2. **Confidence Indicators**
   - For each extracted value, show:
     * Green checkmark: High confidence (clear pattern match, validated)
     * Yellow warning: Medium confidence (ambiguous, needs review)
     * Red flag: Low confidence (likely error, user should verify)
   - Add hover explanation showing extraction method and raw text matched

3. **Interactive Verification**
   - Allow users to:
     * Click on any extracted value to see the source text highlighted
     * Manually correct extraction errors
     * Add missing values
     * Flag false positives for training
   - Save corrections to improve future extractions
```

---

## Prompt 3: Intelligent SDS Section Parsing

```
Implement intelligent SDS section parsing with context awareness:

1. **Section Detection Algorithm**
   ```python
   class SDSParser:
       def __init__(self):
           self.section_patterns = {
               1: r'(?i)section\s*1[:\s]*(identification|product identifier)',
               2: r'(?i)section\s*2[:\s]*(hazards? identification|classification)',
               3: r'(?i)section\s*3[:\s]*(composition|ingredients?)',
               4: r'(?i)section\s*4[:\s]*(first aid)',
               5: r'(?i)section\s*5[:\s]*(fire fighting)',
               6: r'(?i)section\s*6[:\s]*(accidental release)',
               7: r'(?i)section\s*7[:\s]*(handling and storage)',
               8: r'(?i)section\s*8[:\s]*(exposure controls|personal protection)',
               9: r'(?i)section\s*9[:\s]*(physical and chemical properties)',
               10: r'(?i)section\s*10[:\s]*(stability and reactivity)',
               11: r'(?i)section\s*11[:\s]*(toxicological information)',
               12: r'(?i)section\s*12[:\s]*(ecological information)',
               13: r'(?i)section\s*13[:\s]*(disposal considerations)',
               14: r'(?i)section\s*14[:\s]*(transport information)',
               15: r'(?i)section\s*15[:\s]*(regulatory information)',
               16: r'(?i)section\s*16[:\s]*(other information)'
           }
   
       def extract_section_content(self, text, section_num):
           """Extract content for specific section using context"""
           # Find section header
           # Capture all text until next section header
           # Handle multi-page sections
           # Return cleaned text for that section
   ```

2. **Section-Specific Parsers**
   - **Section 2 (Hazard Identification)**: Extract H/P codes, signal word, pictograms
   - **Section 9 (Physical Properties)**: Extract property tables with units
   - **Section 11 (Toxicology)**: Extract LD50/LC50 values with species and routes
   - **Section 12 (Ecology)**: Extract ecotoxicity data with durations

3. **Cross-Section Validation**
   - Compare extracted values across sections for consistency
   - Flag discrepancies (e.g., different flash points in Sections 9 and 5)
   - Build confidence scores based on cross-validation
```

---

## Prompt 4: User-Centric Extraction Interface

```
Redesign the SDS upload interface to focus on what users need, not the parsing method:

1. **Simplified Upload Experience**
   ```
   [SDS PDF UPLOAD]
   Drag and drop or click to upload
   
   [PROCESSING INDICATOR]
   ████████░░ 80% - Extracting GHS classifications...
   
   [RESULTS PREVIEW]
   ✅ Found: 4 hazard classifications
   ✅ Found: 8 physical properties  
   ✅ Found: 3 ecotoxicity values
   ⚠️ Needs review: 2 ambiguous values
   
   [VIEW FULL RESULTS BUTTON]
   ```

2. **Review Mode for Ambiguous Extractions**
   - When extraction confidence is low, show:
     ```
     Ambiguous Value Detected
     ------------------------
     Raw text: "EC50 = 85 mg/L 2 h EC50 = 23.4 mg/L, 48h"
     
     What would you like to extract?
     
     ☐ EC50 = 85 mg/L, 2h (Daphnia)
     ☐ EC50 = 23.4 mg/L, 48h (Daphnia magna)
     ☐ Both values
     ☐ None of these
     
     [Confirm Selection] [Skip]
     ```

3. **Comparison View**
   - Show side-by-side with database data:
     ```
     Property        | From SDS        | From Database   | Match
     ----------------|-----------------|-----------------|------
     Flash Point     | 116°C (closed)  | 116°C (closed)  | ✓ Exact
     H-Codes         | H226, H302      | H226, H302      | ✓ Exact
     Aquatic Toxicity| EC50: 23.4 mg/L | EC50: 24.1 mg/L | ⚠ Similar
     ```
```

---

## Prompt 5: Professional Headers for SDS Section

```
Update the SDS interface headers to be more professional and user-focused:

1. **Main Header Section**
   ```markdown
   # Safety Data Sheet (SDS) Intelligence Platform
   
   **Extract, Validate, and Compare Chemical Hazard Data**
   
   Upload an SDS PDF to automatically extract GHS classifications, physical properties,
   and toxicological data. Results are presented in structured tables matching our
   comprehensive chemical database.
   ```

2. **Upload Section Header**
   ```markdown
   ## Document Upload
   
   **Supported Format:** PDF (SDS compliant with ANSI Z400.1/Z129.1 or REACH Annex II)
   
   Our parser intelligently identifies all 16 SDS sections and extracts:
   - GHS hazard classifications with H/P codes and pictograms
   - Physical and chemical properties with units
   - Toxicological and ecotoxicological data
   - First aid, fire fighting, and exposure controls
   ```

3. **Results Section Headers**
   ```markdown
   ## Extracted Hazard Intelligence
   
   Data organized by category for easy comparison with regulatory databases.
   
   ### ▸ GHS Classification Summary
   Harmonized hazard communication elements extracted from Section 2
   
   ### ▸ Physical & Chemical Properties
   Measured and estimated values from Section 9
   
   ### ▸ Toxicological Profile
   Acute toxicity, irritation, and sensitization data from Section 11
   
   ### ▸ Environmental Fate & Ecotoxicity
   Aquatic toxicity and persistence data from Section 12
   ```

4. **Confidence Indicators Header**
   ```markdown
   ## Extraction Quality Metrics
   
   | Confidence | Meaning | Action |
   |------------|---------|--------|
   | ✅ High | Clear pattern match, validated | Ready for use |
   | ⚠️ Medium | Ambiguous, needs review | Verify before using |
   | ❌ Low | Uncertain extraction | Manual entry recommended |
   ```
```

---

## Prompt 6: SDS-to-Database Matching System

```
Implement a matching system that links SDS extractions to database records:

1. **Chemical Identification from SDS**
   - Extract chemical names from Section 1 and 3
   - Look for CAS numbers in Section 3 (Composition)
   - Use fuzzy matching to find closest database match
   - Present top 3 matches for user selection

2. **Field Mapping System**
   ```python
   # Map SDS extractions to database fields
   field_mappings = {
       'flash_point': {
           'sds_section': 9,
           'database_field': 'physical_properties.flash_point',
           'validation': lambda x: x > -30 and x < 400,
           'units': ['°C', '°F', 'K']
       },
       'h_codes': {
           'sds_section': 2,
           'database_field': 'ghs_classifications.h_code',
           'validation': lambda x: x in GHS_H_CODES,
           'extraction_method': 'regex_h_codes'
       }
   }
   ```

3. **Discrepancy Reporting**
   - When SDS data differs from database, generate report:
     ```
     Data Discrepancy Detected
     -------------------------
     Property: Acute Oral Toxicity (LD50)
     
     SDS Value: 300 mg/kg (rat)
     Database Value: 1500 mg/kg (rat)
     
     Possible reasons:
     □ Different test methodology
     □ Updated data in database
     □ Extraction error
     
     [Accept SDS] [Accept Database] [Investigate]
     ```
```

---

## How to Use

- Feed these prompts to Cursor one at a time (or in small groups) when working on SDS parsing.
- Implement in order: Prompt 1 (data structure) → Prompt 3 (section parsing) → Prompts 2, 4, 5 (display/UX) → Prompt 6 (matching).
- Keep dependencies and resource limits consistent with the rest of the app (see `UX_ENHANCEMENT_PROMPTS.md` for constraints).

These prompts transform SDS extraction from a simple regex dump into a professional chemical intelligence platform that mirrors database query results and supports validation, comparison, and user review.
