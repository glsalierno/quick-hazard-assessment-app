"""
Scrape IARC Monographs classifications from the official WHO IARC page.
Outputs CSV/JSON/Excel with columns compatible with the iarc folder loader
(CAS No., Agent, Group) for use in fast-p2oasys.

Usage:
  python scripts/scrape_iarc_classifications.py
  python scripts/scrape_iarc_classifications.py --output ../fastP2OASys/iarc
  python scripts/scrape_iarc_classifications.py --url https://monographs.iarc.who.int/list-of-classifications/

Note: The official IARC "List of Classifications" is often loaded via JavaScript. If the scraper
extracts no or few rows, use the spreadsheet export from
https://monographs.iarc.who.int/list-of-classifications/ and save the file into the iarc folder
(fastP2OASys/iarc); the app's iarc_lookup loads CSV/Excel with columns "CAS No." and "Group".

Dependencies: requests, beautifulsoup4, pandas; openpyxl for Excel export.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import time
from pathlib import Path

try:
    import pandas as pd
    import requests
    from bs4 import BeautifulSoup
except ImportError as e:
    print(f"Missing dependency: {e}. Install with: pip install requests beautifulsoup4 pandas [openpyxl]")
    sys.exit(1)

try:
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    HTTPAdapter = None
    Retry = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Main page has summary only; full list may be at list-of-classifications (often JS-rendered).
DEFAULT_URL = "https://monographs.iarc.who.int/agents-classified-by-the-iarc/"
LIST_URL = "https://monographs.iarc.who.int/list-of-classifications/"
CAS_PATTERN = re.compile(r"\d+-\d{2}-\d")
GROUP_PATTERN = re.compile(r"Group\s+(\d+[A-Z]?)", re.IGNORECASE)


def create_session() -> requests.Session:
    """Create a session with retry strategy and browser-like headers."""
    session = requests.Session()
    if HTTPAdapter is not None and Retry is not None:
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return session


def fetch_page(url: str, session: requests.Session | None = None) -> str | None:
    """Fetch HTML content with error handling."""
    if session is None:
        session = create_session()
    try:
        logger.info("Fetching %s", url)
        r = session.get(url, timeout=30)
        r.raise_for_status()
        logger.info("Fetched %s bytes", len(r.content))
        return r.text
    except requests.RequestException as e:
        logger.error("Failed to fetch page: %s", e)
        return None


def parse_tables(html: str) -> pd.DataFrame:
    """
    Parse HTML and extract IARC classifications from tables.
    Handles group header rows and data rows; outputs Agent, CAS No., Group.
    """
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        main = soup.find("div", class_=re.compile("entry-content|content")) or soup.find("main")
        if main:
            tables = main.find_all("table")
    if not tables:
        raise ValueError("No tables found on page")

    main_table = max(tables, key=lambda t: len(t.find_all("tr")))
    rows = main_table.find_all("tr")
    logger.info("Processing %d rows from largest table", len(rows))

    classifications: list[dict[str, str]] = []
    current_group_num: str | None = None

    for row in rows:
        cells = row.find_all(["td", "th"])
        if not cells:
            continue

        row_text = " ".join(c.get_text(strip=True) for c in cells).strip()

        # Group header: e.g. "Group 1 | Carcinogenic to humans | 135 agents"
        group_match = GROUP_PATTERN.search(row_text)
        if group_match:
            current_group_num = group_match.group(1).upper()
            if current_group_num not in ("1", "2A", "2B", "3", "4"):
                current_group_num = None
            continue

        # Data row: expect at least Agent and optionally CAS
        if len(cells) >= 2 and current_group_num:
            agent = cells[0].get_text(strip=True)
            cas_raw = cells[1].get_text(strip=True) if len(cells) > 1 else ""

            if not agent or agent.lower() in ("agent", "cas no.", "cas no", "substance"):
                continue
            if "list of classifications" in agent.lower():
                continue

            cas_clean = re.sub(r"\[.*?\]|\(.*?\)", "", cas_raw).strip()
            cas_clean = re.sub(r"\s+", " ", cas_clean).strip()

            if "," in cas_clean or "\n" in cas_clean:
                for part in re.split(r"[,;\n]", cas_clean):
                    part = part.strip()
                    if CAS_PATTERN.search(part):
                        classifications.append({
                            "Agent": agent,
                            "CAS No.": part,
                            "Group": current_group_num,
                        })
            else:
                cas_val = cas_clean if CAS_PATTERN.search(cas_clean) else ""
                classifications.append({
                    "Agent": agent,
                    "CAS No.": cas_val or "",
                    "Group": current_group_num,
                })

    df = pd.DataFrame(classifications)
    if not df.empty:
        df = df.drop_duplicates(subset=["Agent", "CAS No."], keep="first")
    logger.info("Parsed %d entries with CAS", df["CAS No."].notna().sum() if "CAS No." in df.columns else 0)
    return df


def save_outputs(df: pd.DataFrame, output_dir: Path, formats: list[str] | None = None) -> list[Path]:
    """Save DataFrame to CSV, JSON, and optionally Excel. Returns list of saved paths."""
    if formats is None:
        formats = ["csv", "json"]
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = time.strftime("%Y%m%d")
    saved: list[Path] = []

    if "csv" in formats:
        path = output_dir / f"List of Classifications - IARC Monographs {timestamp}.csv"
        df.to_csv(path, index=False, encoding="utf-8")
        logger.info("Saved %s", path)
        saved.append(path)

    if "json" in formats:
        path = output_dir / f"iarc_classifications_{timestamp}.json"
        df.to_json(path, orient="records", indent=2)
        logger.info("Saved %s", path)
        saved.append(path)

    if "excel" in formats:
        try:
            path = output_dir / f"iarc_classifications_{timestamp}.xlsx"
            df.to_excel(path, index=False)
            logger.info("Saved %s", path)
            saved.append(path)
        except ImportError:
            logger.warning("Excel export requires openpyxl: pip install openpyxl")

    return saved


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Scrape IARC classifications from WHO IARC monographs page. "
        "If the main page returns no table data (JS-rendered list), use the spreadsheet export from the website and place it in the iarc folder."
    )
    parser.add_argument("--url", default=DEFAULT_URL, help="IARC page URL (default: agents-classified-by-the-iarc)")
    parser.add_argument("--try-list", action="store_true", help="Also try list-of-classifications URL if first URL yields no data")
    parser.add_argument(
        "--output", "-o",
        default=".",
        help="Output directory (default: current). Use e.g. ../fastP2OASys/iarc to update iarc folder.",
    )
    parser.add_argument("--formats", nargs="+", default=["csv", "json"], choices=["csv", "json", "excel"], help="Output formats")
    parser.add_argument("--no-save", action="store_true", help="Only print summary, do not save files")
    args = parser.parse_args()

    session = create_session()
    df = pd.DataFrame()
    for url in [args.url, LIST_URL] if args.try_list else [args.url]:
        html = fetch_page(url, session)
        if not html:
            continue
        try:
            parsed = parse_tables(html)
            if not parsed.empty and parsed["CAS No."].astype(str).str.strip().ne("").any():
                df = parsed
                break
            if parsed.shape[0] > df.shape[0]:
                df = parsed
        except ValueError:
            continue
    if df.empty:
        logger.warning("No classifications extracted. Page structure may be JS-rendered.")
        print("No table data found. Use the official 'List of Classifications' spreadsheet from")
        print("https://monographs.iarc.who.int/list-of-classifications/ and save CSV/Excel to the iarc folder.")
        return 0

    with_cas = df["CAS No."].notna() & (df["CAS No."].astype(str).str.strip() != "")
    n_with_cas = with_cas.sum()
    print(f"\nScraped {len(df)} agents, {n_with_cas} with CAS numbers")
    if "Group" in df.columns:
        print("\nBy group:")
        print(df["Group"].value_counts().sort_index().to_string())
    print("\nFirst 5 rows:")
    print(df.head().to_string())

    if not args.no_save:
        saved = save_outputs(df, Path(args.output), args.formats)
        if saved:
            print(f"\nSaved to: {[str(p) for p in saved]}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
