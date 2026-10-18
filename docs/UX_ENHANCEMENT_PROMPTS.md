# UX Enhancement Prompts — Chemical Hazard Assessment App

This document holds professional prompts for Cursor (or other AI-assisted development) to implement UX improvements. Use one prompt at a time for focused implementation.

---

## Implementation Priority

| Phase | Focus | Estimated Effort |
|-------|-------|-------------------|
| 1 | Smart Chemical Input & Validation | 2-3 days |
| 2 | Professional Styling & Polish | 1-2 days |
| 3 | Confidence Indicators & Source Attribution | 3-4 days |
| 4 | Interactive Dashboard Enhancements | 4-5 days |
| 5 | Reporting & Export System | 3-4 days |
| 6 | User Guidance & Help System | 2-3 days |
| 7 | Advanced Features (Comparison, Sharing) | 3-4 days |

---

## Prompt 1: Enhanced Chemical Input System

Implement an advanced chemical input system with the following specifications:

1. **Smart Identifier Detection**
   - Create an input component that auto-detects identifier type (CAS RN, SMILES, chemical name, InChIKey)
   - Add real-time validation with visual feedback (green checkmark for valid, red X for invalid)
   - Include a resolver service that converts between identifier types (e.g., if user enters name, resolve to CAS)
   - Display the resolved canonical identifier prominently

2. **Batch Input Interface**
   - Design a multi-tab input area with options:
     * Single chemical input (with smart detection)
     * CSV/Excel upload with drag-and-drop zone
     * Text area for pasting multiple identifiers (one per line)
   - Show a preview table of uploaded/entered chemicals before submission
   - Include a template download button (CSV with example formats)

3. **Chemical Registry**
   - Implement a searchable history sidebar showing recently assessed chemicals
   - Store: CAS, name, assessment timestamp, overall hazard score (color-coded)
   - Allow one-click re-assessment from history
   - Include a "favorites" pinning system for frequently assessed chemicals

Use Streamlit components with custom CSS for professional styling. Ensure all inputs have clear labels and helper text.

---

## Prompt 2: Data Provenance & Confidence Visualization

Create a comprehensive data provenance system:

1. **Source Attribution Panel**
   - For each hazard endpoint, display a compact source indicator with:
     * Source icons/abbreviations (ECHA, Danish QSAR, VEGA, ICE, EPA)
     * Tooltips on hover showing full source name and data type (experimental/predicted)
     * Click to expand full source details including model names and applicability domain
   - Add a "Data Quality Dashboard" collapsible section showing:
     * Number of sources contributing to each endpoint
     * Data freshness indicators (when each source was last updated)
     * Reliability scores per source (e.g., Klimisch scores for experimental data)

2. **Confidence Visualization System**
   - Implement a three-tier confidence indicator for each score:
     * High: Green solid circle with checkmark (multiple high-quality sources agree)
     * Moderate: Yellow half-filled circle (limited data or some disagreement)
     * Low: Red open circle (single prediction or outside applicability domain)
   - Add confidence bars showing % of sources supporting each classification
   - Create a hover tooltip explaining the confidence calculation:
     * Number of sources
     * Quality of sources
     * Consensus level
     * Applicability domain status

3. **Conflict Resolution Interface**
   - When sources disagree, highlight the endpoint with a warning icon
   - Create an expandable "Data Reconciliation" view showing:
     * Table of all source predictions with their confidence levels
     * Option to manually select which source to prioritize
     * Visual indication of majority vs minority opinions
     * Links to view raw data from each source

Use consistent color coding (green/yellow/red) throughout and ensure accessibility compliance.

---

## Prompt 3: Interactive Results Dashboard

Design an interactive hazard assessment dashboard:

1. **Customizable Hazard Matrix**
   - Create a main results view with:
     * Chemical identifier header (CAS, name, formula)
     * Hazard categories organized by domain: Human Health, Environmental, Physical-Chemical
     * Each hazard displayed as a card with:
       - Hazard name and GHS pictogram (where applicable)
       - Score/classification (e.g., "Category 3", "H317")
       - Confidence indicator
       - Expandable details arrow
   - Add a "Customize View" button allowing users to:
     * Show/hide specific hazard categories
     * Reorder categories via drag-and-drop
     * Save custom views per user session

2. **Comparative Analysis Tool**
   - Implement a multi-chemical comparison mode:
     * Select 2-5 chemicals from history or batch upload
     * Side-by-side table view with:
       - Columns: Hazard endpoints
       - Rows: Selected chemicals
       - Color-coded cells (red=high concern, yellow=moderate, green=low)
       - Sortable by any hazard column
     * Add a "Similarity Score" showing how closely chemicals match
     * Export comparison as CSV or image

3. **Detailed Data Explorer**
   - For each hazard endpoint, create an expandable section showing:
     * Tabbed view: Summary | Experimental Data | Predictions | Source Details
     * Summary tab: Final score with rationale
     * Experimental Data tab: Table of all in vivo/in vitro studies with links
     * Predictions tab: All QSAR model outputs with applicability domain
     * Source Details tab: Complete provenance with timestamps and URLs
   - Include a "Download Raw Data" button for each endpoint (JSON format)

Ensure the dashboard is responsive and performs well with large datasets. Use Streamlit's caching extensively.

---

## Prompt 4: Professional Reporting System

Develop a comprehensive reporting and export system:

1. **Automated Report Generator**
   - Create a "Generate Report" button that creates:
     * Executive Summary: One-page overview with key hazards flagged
     * Detailed Assessment: Full endpoint-by-endpoint analysis
     * Data Sources Appendix: Complete provenance documentation
     * Methodology: Explanation of scoring algorithm and confidence system
   - Support multiple output formats:
     * PDF (print-ready with professional formatting)
     * DOCX (editable for further annotation)
     * HTML (interactive web report)
   - Include report customization options:
     * Select which sections to include
     * Choose detail level (summary vs. comprehensive)
     * Add company logo and custom header

2. **Multi-Format Data Export**
   - Implement export options with user-selectable fields:
     * CSV: Select specific hazard columns to include
     * Excel: Multiple sheets (Summary, Detailed, Sources)
     * JSON: Complete raw data for API integration
     * RDF: For semantic web applications
   - Add export presets:
     * "P2OASYS Standard" (default hazard categories)
     * "Regulatory Submission" (GHS classifications only)
     * "Research Export" (all available data points)

3. **Shareable Assessment Links**
   - Generate unique, shareable URLs for each assessment
   - Include options for:
     * Public view (read-only, no sensitive data)
     * Team collaboration (add comments/notes)
     * Version tracking (see assessment history)
   - Add QR code generation for easy mobile access

Use professional report templates with clean typography and consistent branding. Include page numbers, dates, and document identifiers.

---

## Prompt 5: User Guidance & Onboarding

Create a comprehensive help and guidance system:

1. **Interactive Onboarding Tour**
   - Implement a first-visit tour using Shepherd.js or similar:
     * Step 1: Chemical input introduction
     * Step 2: Understanding the hazard matrix
     * Step 3: Interpreting confidence indicators
     * Step 4: Accessing detailed data
     * Step 5: Generating reports
   - Add a "Restart Tour" button in help menu
   - Include progress indicators and skip option

2. **Contextual Help System**
   - Add (?) icons next to each major component with:
     * Brief explanation of the feature
     * Link to detailed documentation
     * Example use cases
   - Create a floating help button with:
     * Quick search of help topics
     * FAQ section
     * Contact support option
   - Implement tooltips for:
     * Hazard category definitions (with GHS criteria links)
     * Source abbreviations and authority levels
     * Confidence scoring methodology

3. **Knowledge Base Integration**
   - Create a dedicated "Learn" section with:
     * Tutorial videos (embedded or linked)
     * Step-by-step guides for common workflows
     * Glossary of terms (CAS, SMILES, GHS, P2OASYS, etc.)
     * Best practices for hazard assessment
   - Include a "Feedback" form for users to:
     * Report data discrepancies
     * Suggest new features
     * Request additional sources
   - Add release notes and update announcements

Ensure all help content is accessible via keyboard navigation and screen readers. Use clear, non-technical language where possible.

---

## Prompt 6: Performance & Professional Polish

Enhance the application's professional appearance and performance:

1. **Advanced Styling System**
   - Implement a custom CSS theme with:
     * Professional color palette (blues, grays with accent colors for hazards)
     * Consistent typography (Inter or Roboto font family)
     * Responsive design for all screen sizes
     * Print-optimized styles for reports
   - Add dark/light mode toggle with preference saving
   - Use subtle animations for state changes (loading, expanding sections)

2. **Performance Optimizations**
   - Implement comprehensive caching:
     * st.cache_data for all API calls and database queries
     * st.cache_resource for model loading
     * Session state for user preferences
   - Add progress indicators for:
     * Batch processing (percentage complete)
     * Data retrieval (spinner with status messages)
     * Report generation (estimated time)
   - Implement lazy loading for detailed sections

3. **Professional Error Handling**
   - Create user-friendly error messages:
     * "Unable to retrieve data for CAS 123-45-6" (not raw exceptions)
     * Suggestions for resolution
     * Option to retry or contact support
   - Add a system status dashboard showing:
     * API availability (green/yellow/red indicators)
     * Last successful data updates
     * Known issues or maintenance windows
   - Implement graceful degradation when sources are unavailable

4. **Analytics & Monitoring**
   - Add privacy-compliant usage analytics:
     * Track feature usage (opt-in only)
     * Monitor error rates
     * Measure response times
   - Create an admin dashboard for:
     * System health monitoring
     * Usage statistics
     * User feedback aggregation

Use Streamlit's theming capabilities and custom components where needed. Ensure all additions maintain fast load times.

---

## How to Use

- Copy one prompt (or one phase) into Cursor and ask it to implement.
- Reference this file: "See docs/UX_ENHANCEMENT_PROMPTS.md for the full prompt set and priority."
- Implement in the order of the priority table for a phased rollout.
