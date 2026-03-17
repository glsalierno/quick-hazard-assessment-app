#!/usr/bin/env Rscript

# Build a flattened ECOTOX CSV from the EPA ASCII export using ECOTOXr.
# This CSV is then loaded by GHhaz2 via scripts/setup_chemical_db.py.
#
# Usage (from GHhaz2 root in a machine with R and ECOTOXr installed):
#   Rscript scripts/build_ecotox_flat.R
#
# It expects the ECOTOX ASCII folder at:
#   ../GHhaz3/ECOTOX/ecotox_ascii_03_12_2026
# and writes:
#   data/raw_databases/ecotox/ecotox_results.csv

suppressPackageStartupMessages({
  library(ECOTOXr)
  library(DBI)
  library(dplyr)
  library(readr)
})

repo_root <- normalizePath(".", winslash = "/", mustWork = TRUE)
ascii_dir <- file.path(repo_root, "..", "GHhaz3", "ECOTOX", "ecotox_ascii_03_12_2026")
db_path   <- file.path(ascii_dir, "ecotox.sqlite")

message("ECOTOX ASCII dir: ", ascii_dir)

if (!dir.exists(ascii_dir)) {
  stop("ECOTOX ASCII folder not found at: ", ascii_dir)
}

# Build or update the local ECOTOX SQLite database.
if (!file.exists(db_path)) {
  message("Building ECOTOX SQLite database with ECOTOXr::download_ecotox_data() ...")
  download_ecotox_data(path = ascii_dir, build_db = TRUE, db_path = db_path)
} else {
  message("Reusing existing ECOTOX SQLite at: ", db_path)
}

con <- dbConnect(RSQLite::SQLite(), db_path)
on.exit(dbDisconnect(con), add = TRUE)

# NOTE: This query is based on ECOTOXr's schema; adjust table/column names
# if a future ECOTOX export changes them.

results_tbl   <- tbl(con, "results")
tests_tbl     <- tbl(con, "tests")
chem_tbl      <- tbl(con, "chemicals")
species_tbl   <- tbl(con, "species")

ecotox_flat <- results_tbl %>%
  left_join(tests_tbl,   by = "test_id") %>%
  left_join(chem_tbl,    by = "chemical_id") %>%
  left_join(species_tbl, by = "species_number") %>%
  transmute(
    cas            = cas_number,
    dtxsid         = NA_character_,  # can be backfilled later via DSSTox mapping
    species        = species_name,
    endpoint       = endpoint,
    value_numeric  = conc1_mean,
    units          = conc1_unit,
    duration_days  = case_when(
      obs_duration_unit == "h" ~ obs_duration_mean / 24,
      obs_duration_unit == "d" ~ obs_duration_mean,
      TRUE ~ NA_real_
    ),
    media          = media_type,
    organism_group = life_stage,
    effect         = effect,
    reference      = reference_number
  ) %>%
  collect()

out_dir <- file.path(repo_root, "data", "raw_databases", "ecotox")
out_csv <- file.path(out_dir, "ecotox_results.csv")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

message("Writing flattened ECOTOX CSV to: ", out_csv)
write_csv(ecotox_flat, out_csv, na = "")

message("Done. Now run: python scripts/setup_chemical_db.py to load ECOTOX into SQLite.")

