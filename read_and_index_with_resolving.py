# -*- coding: utf-8 -*-
"""
read_and_index.py

Features
- Process one, many (comma-separated), or ALL sheets from an Excel file.
- Control processing order via SHEET_ORDER (later sheets can reuse earlier results).
- Denormalize ("blow up") foreign keys from other sheets into embedded objects/arrays.
- Keep processed rows in memory and reuse them for downstream sheets (prefer processed).
- Write one JSON file per row (<sheet>-00001.json, ...).
- Optional: write one combined JSON for a single sheet after all processing (--single-out=Sheet).
- Import each processed sheet into Elasticsearch if an indexer config exists.

Usage
  python read_and_index.py <excel_file> [sheet_name|A,B,C|ALL] [--single-out=SheetName]

Examples
  python read_and_index.py data.xlsx ALL
  python read_and_index.py data.xlsx Tijdschriften,Personen --single-out=Tijdschriften
"""

import os
import glob
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional, Set, DefaultDict
from collections import defaultdict

import numpy as np
import pandas as pd
from elasticsearch import Elasticsearch
from procrustus_indexer import build_indexer

# ============================ RELATIONS ============================
# Keys are MAIN sheet names → list of foreign key specs.

# Foreign key spec:
#   fk_col: column in main sheet holding the ID(s)
#   ref_sheet: sheet to look up referenced rows
#   ref_id_col: ID column in the ref sheet
#   as: output field name (embedded object/array)
#   many: True if fk_col has multiple IDs (separated by SEP or 'sep' override)
#   drop_fk: True to remove the original fk column from output
#   sep: optional override for delimiter for this relation
SEP = ";"  # default delimiter for multi-ID cells
RELATIONS: Dict[str, List[Dict[str, Any]]] = {
    "Uitgever_Drukker": [
        {"fk_col": "Eerste_generatie_ID", "ref_sheet": "Personen", "ref_id_col": "ID_def", "as": "Eerste_generatie_ID", "many": False, "drop_fk": True},
        {"fk_col": "Tweede_generatie_ID", "ref_sheet": "Personen", "ref_id_col": "ID_def", "as": "Tweede_generatie_ID", "many": False, "drop_fk": True},
        {"fk_col": "Plaats1_ID", "ref_sheet": "Plaatsnaam", "ref_id_col": "Plaats_ID", "as": "Plaats1", "many": False, "drop_fk": True},
        {"fk_col": "Plaats2_ID", "ref_sheet": "Plaatsnaam", "ref_id_col": "Plaats_ID", "as": "Plaats2", "many": False, "drop_fk": True},
        {"fk_col": "Plaats3_ID", "ref_sheet": "Plaatsnaam", "ref_id_col": "Plaats_ID", "as": "Plaats3", "many": False, "drop_fk": True},
    ],
    "Tijdschriften": [
        {"fk_col": "Uitgever_ID_nieuw", "ref_sheet": "Uitgever_Drukker", "ref_id_col": "Uitgever_ID_nieuw", "as": "Uitgever", "many": False, "drop_fk": True},
        {"fk_col": "Uitgever2_ID_nieuw", "ref_sheet": "Uitgever_Drukker", "ref_id_col": "Uitgever_ID_nieuw", "as": "Uitgever2", "many": False, "drop_fk": True},
        {"fk_col": "Drukker_ID_nieuw", "ref_sheet": "Uitgever_Drukker", "ref_id_col": "Uitgever_ID_nieuw", "as": "Drukker", "many": False, "drop_fk": True},
        {"fk_col": "Plaats1_ID", "ref_sheet": "Plaatsnaam", "ref_id_col": "Plaats_ID", "as": "Plaats1", "many": False, "drop_fk": True},
        {"fk_col": "Plaats2_ID", "ref_sheet": "Plaatsnaam", "ref_id_col": "Plaats_ID", "as": "Plaats2", "many": False, "drop_fk": True},
        {"fk_col": "Plaats3_ID", "ref_sheet": "Plaatsnaam", "ref_id_col": "Plaats_ID", "as": "Plaats3", "many": False, "drop_fk": True},
        {"fk_col": "Auteur-Redacteur1_ID", "ref_sheet": "Personen", "ref_id_col": "ID_def", "as": "Auteur_Redacteur1", "many": False, "drop_fk": True},
        {"fk_col": "Auteur-Redacteur2_ID", "ref_sheet": "Personen", "ref_id_col": "ID_def", "as": "Auteur_Redacteur2", "many": False, "drop_fk": True},
        {"fk_col": "Auteur-Redacteur3_ID", "ref_sheet": "Personen", "ref_id_col": "ID_def", "as": "Auteur_Redacteur3", "many": False, "drop_fk": True},
        {"fk_col": "Auteur-Redacteur4_ID", "ref_sheet": "Personen", "ref_id_col": "ID_def", "as": "Auteur_Redacteur4", "many": False, "drop_fk": True},
        {"fk_col": "Auteur-Redacteur5_ID", "ref_sheet": "Personen", "ref_id_col": "ID_def", "as": "Auteur_Redacteur5", "many": False, "drop_fk": True},
    ],
}

# ============================ SHEET ORDER CONFIG ================================
# Process in this order - any requested target sheets keep this relative order.
SHEET_ORDER = [
    "Plaatsnaam",
    "Personen",
    "Uitgever_Drukker",
    "Tijdschriften",
]

# ============================ OUTPUT CONFIG =====================================
WRITE_ONE_FILE_PER_ROW = True
OUT_DIR = Path("json-files-resolved")

es = Elasticsearch(hosts=["http://localhost:9200"])

# ============================ SANITIZATION ================================
def sanitize(value: Any) -> Any:
    """Convert pandas/NumPy types to JSON-safe Python types; drop NaNs; recurse."""
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if isinstance(value, (pd.Timestamp, pd.NaT.__class__)):
        return None if pd.isna(value) else value.isoformat()

    if isinstance(value, np.generic):
        return value.item()

    if isinstance(value, dict):
        return {k: sanitize(v) for k, v in value.items()}

    if isinstance(value, (list, tuple)):
        t = [sanitize(v) for v in value]
        return t if isinstance(value, list) else tuple(t)

    return value

# ------------------------------ In-memory stores --------------------------------
# Processed rows, by sheet (already-expanded, sanitized per row at write time).
_PROCESSED_SHEETS: Dict[str, List[Dict[str, Any]]] = {}
# Index from PROCESSED rows: (sheet, id_col) -> { id -> row_without_id }
_PROCESSED_INDEX_CACHE: Dict[Tuple[str, str], Dict[str, Dict[str, Any]]] = {}
# Raw ref index from Excel (fallback): (abs_excel_path, sheet, id_col) -> { id -> row_without_id }
_RAW_REF_INDEX_CACHE: Dict[Tuple[str, str, str], Dict[str, Dict[str, Any]]] = {}

def _infer_needed_indices_from_relations() -> DefaultDict[str, Set[str]]:
    """
    Build a mapping: sheet -> set(id_cols) that other sheets will use to reference this sheet.
    We infer this from RELATIONS: for each relation (ref_sheet, ref_id_col), add id_col under ref_sheet.
    """
    need: DefaultDict[str, Set[str]] = defaultdict(set)
    for rels in RELATIONS.values():
        for rel in rels:
            need[rel["ref_sheet"]].add(rel["ref_id_col"])
    return need

_NEEDED_INDICES = _infer_needed_indices_from_relations()

# ------------------------------ Ref index builders ------------------------------

def _rowdict_without_id(d: Dict[str, Any], id_col: str) -> Dict[str, Any]:
    # drop id_col and empty-string fields for cleaner embedding
    out = {k: v for k, v in d.items() if k != id_col and v != ""}
    return out

def build_processed_indices_for_sheet(sheet: str, records: List[Dict[str, Any]]) -> None:
    """
    From processed records, build {id -> row_without_id} for each id_col that other sheets use
    to reference this sheet (inferred from RELATIONS).
    """
    id_cols = _NEEDED_INDICES.get(sheet, set())
    if not id_cols:
        return
    for id_col in id_cols:
        idx: Dict[str, Dict[str, Any]] = {}
        for r in records:
            raw_id = r.get(id_col)
            if raw_id in (None, ""):
                continue
            key = str(raw_id).strip()
            if not key:
                continue
            idx[key] = _rowdict_without_id(r, id_col)
        _PROCESSED_INDEX_CACHE[(sheet, id_col)] = idx

def load_raw_ref_index(xls_path: str, sheet: str, id_col: str) -> Dict[str, Dict[str, Any]]:
    """Read a reference sheet from Excel and build a {id -> row-without-id} map. Cached."""
    cache_key = (os.path.abspath(xls_path), sheet, id_col)
    if cache_key in _RAW_REF_INDEX_CACHE:
        return _RAW_REF_INDEX_CACHE[cache_key]

    df = pd.read_excel(xls_path, sheet_name=sheet, dtype=str).fillna("")
    if id_col not in df.columns:
        raise KeyError(f"Sheet '{sheet}' missing id column '{id_col}'")

    if df[id_col].duplicated().any():
        dups = df[df[id_col].duplicated()][id_col].tolist()
        preview = dups[:5]
        raise ValueError(f"Duplicate IDs in '{sheet}.{id_col}': {preview}{' ...' if len(dups) > 5 else ''}")

    idx = {
        r[id_col]: {k: v for k, v in r.items() if v != "" and k != id_col}
        for r in df.to_dict(orient="records")
    }
    _RAW_REF_INDEX_CACHE[cache_key] = idx
    return idx

def get_best_ref_index(
        xls_path: str,
        ref_sheet: str,
        ref_id_col: str,
        prefer_processed: bool = True,
) -> Dict[str, Dict[str, Any]]:
    """
    Prefer the processed index (if that sheet was already processed in this run);
    otherwise fall back to a raw Excel ref index.
    """
    if prefer_processed:
        proc_key = (ref_sheet, ref_id_col)
        if proc_key in _PROCESSED_INDEX_CACHE:
            return _PROCESSED_INDEX_CACHE[proc_key]
    return load_raw_ref_index(xls_path, ref_sheet, ref_id_col)

# ------------------------------ Blow-up relations -------------------------------

def embed_relations_into_records(
        xls_path: str,
        main_sheet: str,
        records: List[Dict[str, Any]],
        relations: List[Dict[str, Any]],
        default_sep: str = SEP,
        prefer_processed_refs: bool = True,
) -> None:
    """Mutates `records` in place: replaces FK columns with embedded objects/arrays.

    For each relation, we resolve references via:
      1) processed in-memory indices (if available and prefer_processed_refs=True)
      2) otherwise cached raw ref indices from Excel.
    """
    ref_indices: Dict[Tuple[str, str], Dict[str, Dict[str, Any]]] = {}
    for rel in relations:
        key = (rel["ref_sheet"], rel["ref_id_col"])
        if key not in ref_indices:
            ref_indices[key] = get_best_ref_index(
                xls_path=xls_path,
                ref_sheet=rel["ref_sheet"],
                ref_id_col=rel["ref_id_col"],
                prefer_processed=prefer_processed_refs,
            )

    for row in records:
        for rel in relations:
            fk_col = rel["fk_col"]
            out_key = rel["as"]
            many = bool(rel.get("many", False))
            drop_fk = bool(rel.get("drop_fk", True))
            sep = rel.get("sep", default_sep)

            raw_val = row.get(fk_col, "")
            fk_val = str(raw_val).strip() if raw_val is not None else ""

            ref_idx = ref_indices[(rel["ref_sheet"], rel["ref_id_col"])]

            if many:
                ids = [s.strip() for s in fk_val.split(sep) if s and s.strip()]
                row[out_key] = [dict(id=i, **ref_idx[i]) for i in ids if i in ref_idx]
            else:
                row[out_key] = dict(id=fk_val, **ref_idx[fk_val]) if fk_val in ref_idx else None

            if drop_fk:
                row.pop(fk_col, None)

        # prune empty-string fields for cleaner JSON
        #for k in [k for k, v in row.items() if v == ""]:
        #    row.pop(k, None)

# ------------------------------ Excel helpers ----------------------------------

def _resolve_all_sheets(excel_path: str) -> List[str]:
    return pd.ExcelFile(excel_path).sheet_names

def _normalize_targets(excel_path: str, sheet_arg: Optional[str]) -> List[str]:
    """Return target sheet names in ordered form.
    - None or 'ALL' → all sheets
    - 'A,B,C' → those sheets
    - single name → that sheet
    Order is by SHEET_ORDER first; extras go to the end alphabetically.
    """
    all_sheets = _resolve_all_sheets(excel_path)

    if sheet_arg is None or sheet_arg.strip().upper() == "ALL":
        selected = all_sheets
    else:
        selected = [s.strip() for s in sheet_arg.split(",")]

    unknown = [s for s in selected if s not in all_sheets]
    if unknown:
        raise KeyError(f"Unknown sheet(s): {unknown}. Available: {all_sheets}")

    order_map = {name: i for i, name in enumerate(SHEET_ORDER)}
    def sort_key(name: str) -> Tuple[int, str]:
        return (order_map.get(name, len(SHEET_ORDER)), name.lower())

    return sorted(selected, key=sort_key)

# ------------------------------ Excel → JSON (per sheet) -----------------------

def excel_sheet_to_json(
        excel_path: str,
        main_sheet: str,
        prefer_processed_refs: bool = True,
) -> Tuple[int, List[Dict[str, Any]]]:
    """Process one sheet: blow up FKs (preferring processed refs), write per-row JSON files.
    Returns (count written, cleaned_records_for_this_sheet).
    """
    df = pd.read_excel(
        excel_path,
        sheet_name=main_sheet,
        keep_default_na=False,
        dtype_backend="numpy_nullable",
    )
    df = df.astype(object).where(~df.isna(), None)
    df.insert(0, "rowId", range(1, len(df) + 1))

    records = df.to_dict(orient="records")

    # Blow up relations for this sheet if configured
    if main_sheet in RELATIONS and RELATIONS[main_sheet]:
        embed_relations_into_records(
            xls_path=excel_path,
            main_sheet=main_sheet,
            records=records,
            relations=RELATIONS[main_sheet],
            default_sep=SEP,
            prefer_processed_refs=prefer_processed_refs,
        )

    # Keep processed (pre-sanitize) records in memory for downstream sheets
    _PROCESSED_SHEETS[main_sheet] = [dict(r) for r in records]
    # Build processed indices for any id_cols other sheets will use to reference this sheet
    build_processed_indices_for_sheet(main_sheet, _PROCESSED_SHEETS[main_sheet])

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    base = str(main_sheet).lower()
    pad = max(5, len(str(len(records))))

    written = 0
    cleaned_records: List[Dict[str, Any]] = []

    for rec in records:
        clean = sanitize(rec)
        cleaned_records.append(clean)

        if WRITE_ONE_FILE_PER_ROW:
            row_id = clean.get("rowId", written + 1)
            file_name = OUT_DIR / f"{base}-{int(row_id):0{pad}d}.json"
            with open(file_name, "w", encoding="utf-8") as f:
                json.dump(clean, f, ensure_ascii=False, indent=2, allow_nan=False)
            written += 1

    if WRITE_ONE_FILE_PER_ROW:
        print(f"[{main_sheet}] Wrote {written} row files → {OUT_DIR}/ (base '{base}')")

    return written, cleaned_records

# ------------------------------ Elasticsearch import ---------------------------

def import_index(sheet: str, es_client: Elasticsearch) -> None:
    """Import JSON files for a sheet into Elasticsearch if config exists."""
    es_index_name = f"hi-ga-tijdschriften-{sheet.lower()}"
    cfg = f"indexer-{sheet.lower()}-config.toml"
    pattern = f"{OUT_DIR}/{sheet.lower()}-*.json"

    inputs = glob.glob(pattern)
    if not inputs:
        print(f"[{sheet}] Skipping ES import: no JSON files found for pattern {pattern}")
        return

    if not Path(cfg).exists():
        print(f"[{sheet}] Skipping ES import: config not found: {cfg}")
        return

    indexer = build_indexer(cfg, es_index_name, es_client)
    indexer.create_mapping(overwrite=True)
    indexer.import_files(inputs)
    print(f"[{sheet}] Imported {len(inputs)} files into Elasticsearch index '{es_index_name}'")

# ----------------------------------- CLI ---------------------------------------

def _parse_args(argv: List[str]) -> Tuple[str, Optional[str], Optional[str]]:
    """Returns (excel_file, sheets_arg, single_out_sheet)."""
    if len(argv) < 2:
        print("Usage: python read_and_index_with_resolving.py <excel_file> [sheet_name|A,B,C|ALL] [--single-out=SheetName]")
        sys.exit(1)

    excel_file = argv[1]
    sheets_arg: Optional[str] = None
    single_out_sheet: Optional[str] = None

    for arg in argv[2:]:
        if arg.startswith("--single-out="):
            single_out_sheet = arg.split("=", 1)[1].strip()
        elif sheets_arg is None:
            sheets_arg = arg.strip()
        else:
            # ignore extras; add more flags here if needed
            pass

    return excel_file, sheets_arg, single_out_sheet

if __name__ == "__main__":
    excel_file, sheet_arg, single_out = _parse_args(sys.argv)

    targets = _normalize_targets(excel_file, sheet_arg)

    total_written = 0
    combined_buffer: Dict[str, List[Dict[str, Any]]] = {}

    # Process sheets strictly in order; each step enriches the in-memory stores
    for sheet in targets:
        written, cleaned = excel_sheet_to_json(
            excel_path=excel_file,
            main_sheet=sheet,
            prefer_processed_refs=True,  # <-- prefer results from already processed sheets
        )
        total_written += written
        combined_buffer[sheet] = cleaned

    # Import ES for each processed sheet, honoring the same order
    for sheet in targets:
        import_index(sheet, es)

    # After *all* processing is complete, optionally write one combined file for a single sheet
    if single_out:
        if single_out not in combined_buffer:
            raise KeyError(f"--single-out '{single_out}' was not processed. Available: {list(combined_buffer.keys())}")
        out_path = OUT_DIR / f"{single_out.lower()}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(combined_buffer[single_out], f, ensure_ascii=False, indent=2, allow_nan=False)
        print(f"[{single_out}] Wrote combined JSON file at the end: {out_path}")

    print(f"Done. Total JSON rows written across {len(targets)} sheet(s): {total_written}")
