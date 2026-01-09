## Purpose

This file gives focused, actionable guidance to AI coding agents working in this repository so they can be productive immediately. It documents the project's structure, conventions, and concrete examples of expected changes.

## Big picture (what this repo does)

- The project fetches and processes daily municipal meteorological data (AEMET) for Barcelona/Spain. The primary ETL artifacts live under `data/raw/aemet/municipio_diaria/` (raw inputs) and `processed/aemet/municipio_diaria_parquet/` (clean, columnar outputs).
- The script entrypoint (currently present but empty) is `fetch_aemet_barcelona.py` — it's intended to download or ingest the AEMET "municipio_diaria" dataset and store raw files.
- `dashboards/` is the place for visualizations that consume `processed/..._parquet/` data.

## Key files & directories (quick map)

- `fetch_aemet_barcelona.py` — main fetch/ingest script. When modifying, keep idempotency and safe writes in mind.
- `data/raw/aemet/municipio_diaria/` — raw input files (CSV/JSON). Do not delete or rewrite these in-place unless explicitly migrating data.
- `processed/aemet/municipio_diaria_parquet/` — output parquet files used by dashboards and downstream analysis.
- `.gitignore` — exists at repo root (respect it; large binary/data files should not be committed).

## Project-specific conventions (follow these exactly)

- File layout: raw inputs -> processed parquet. Always write raw files to `data/raw/...` and processed outputs to `processed/...`.
- File naming: use a deterministic, date-aware pattern for raw fetches. Example: `data/raw/aemet/municipio_diaria/municipio_diaria_YYYYMMDD.json` or `.csv`.
- Atomic writes: write to a temporary filename (eg. with `.tmp` suffix) and then rename to the final path to avoid partial reads by downstream processes.
- Credentials & secrets: assume API keys are stored in environment variables (eg. `AEMET_API_KEY`). Do not hard-code secrets in code or commit them.
- Schema stability: produced parquet files must be schema-consistent. When adding/removing columns, produce a migration step that is additive and documented.

## Integration points & external dependencies

- External data source: AEMET API (municipio_diaria). Implement fetch logic to respect rate limits and retries. If you need the exact API schema, consult the official AEMET docs or the data files under `data/raw/aemet/municipio_diaria/`.
- Downstream consumers: dashboards in `dashboards/` read the parquet outputs. Preserve column names and types where possible to avoid breaking visualizations.

## Developer workflows & useful commands (assumptions)

Note: this repo contains Python scripts. The project currently has no lockfile or requirements file in the repo root; assume a typical Python workflow. If you add dependencies, also add a `requirements.txt` or `pyproject.toml`.

- Create a venv and install dependencies (suggested):

  python -m venv .venv
  source .venv/bin/activate
  python -m pip install --upgrade pip
  # then install any packages you add to requirements

- Running the ingest script (assumed pattern):

  python fetch_aemet_barcelona.py --out-dir data/raw/aemet/municipio_diaria

If you add CLI flags, keep them small and documented (--out-dir, --start-date, --end-date, --dry-run).

## Examples of concrete changes an AI agent may be asked to make

- Implement `fetch_aemet_barcelona.py` to: read `AEMET_API_KEY` from env, call AEMET municipio_diaria endpoint, write raw JSON/CSV files to `data/raw/aemet/municipio_diaria/municipio_diaria_YYYYMMDD.ext` using atomic writes, and log summary counts.
- Add a transformation script that reads raw files and writes Parquet files into `processed/aemet/municipio_diaria_parquet/` with a stable schema (partition by year/month if dataset grows large).
- Add basic input validation and a `--dry-run` mode to avoid creating files during exploratory runs.

## Constraints & do-not-change rules

- Never remove or rewrite historical files in `data/raw/` without an explicit migration script and clear commit message. Raw data is the single source of truth.
- Preserve column names/types consumed by dashboards; coordinate changes by updating both ETL and dashboard code in the same PR.

## How to verify changes (minimal checks)

- Unit smoke: run the fetch in `--dry-run` to ensure no files are written and network calls are attempted as expected.
- Integration smoke: run the fetch for a single date, confirm a file appears in `data/raw/aemet/municipio_diaria/` and a parquet is produced in `processed/...` if a transformer is present.

## If something is unclear

- There are very few code files in the repo currently. If you need schema details or the AEMET payloads, look under `data/raw/aemet/municipio_diaria/` for sample files (or ask the maintainers to provide a sample payload). When in doubt, implement conservative, backward-compatible changes and call out assumptions in the PR description.

---

If you'd like, I can: (1) implement a basic, idempotent `fetch_aemet_barcelona.py` skeleton that reads `AEMET_API_KEY` and writes a sample raw file, or (2) add a transformation script to produce Parquet files from a sample raw file. Tell me which and I'll proceed.
