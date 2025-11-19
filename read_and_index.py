# -*- coding: utf-8 -*-
import pandas as pd
from typing import Any
import numpy as np
import json
import sys
from pathlib import Path
from elasticsearch import Elasticsearch
from procrustus_indexer import build_indexer
import glob

es = Elasticsearch(
    hosts=["http://localhost:9200"]
)

def sanitize(value: Any) -> Any:
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if isinstance(value, (pd.Timestamp, pd.NaT.__class__)):
        return None if pd.isna(value) else value.isoformat()

    if isinstance(value, np.generic):
        return value.item()

    # Recurse
    if isinstance(value, dict):
        return {k: sanitize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        t = [sanitize(v) for v in value]
        return t if isinstance(value, list) else tuple(t)

    return value

def excel_to_json(excel_path: str, sheet_name: str):
    df = pd.read_excel(
        excel_path,
        sheet_name=sheet_name,
        keep_default_na=False,
        dtype_backend="numpy_nullable",
    )

    df = df.astype(object).where(~df.isna(), None)

    df.insert(0, "rowId", range(1, len(df) + 1))

    records = df.to_dict(orient="records")

    out_dir = Path("json-files-resolved")
    out_dir.mkdir(parents=True, exist_ok=True)

    base = str(sheet_name).lower()

    # Zero-padding width for nicer sorting (e.g., 00001)
    pad = max(5, len(str(len(records))))

    # Write one JSON per row
    written = 0
    for rec in records:
        clean = sanitize(rec)

        # Choose filename: <base>-<rowId>.json (zero-padded)
        row_id = clean.get("rowId", written + 1)
        file_name = out_dir / f"{base}-{int(row_id):0{pad}d}.json"

        with open(file_name, "w", encoding="utf-8") as f:
            json.dump(clean, f, ensure_ascii=False, indent=2, allow_nan=False)
        written += 1

    print(f"Wrote {written} row files to: {out_dir}/, base: '{base}'")

def import_index(sheet: str, es: Elasticsearch):
    indexer = build_indexer(f"indexer-{sheet.lower()}-config.toml", sheet.lower(), es)
    indexer.create_mapping(overwrite=True)
    input_list = glob.glob(f"json-files-resolved/{sheet.lower()}-*.json")
    indexer.import_files(input_list)
    print(f"Imported {len(input_list)} files into Elastic Search, index '{sheet.lower()}'")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python read_and_index.py <excel_file> [sheet_name]")
    else:
        # Create JSON files from the input Excel file
        excel_file = sys.argv[1]
        sheet = sys.argv[2] if len(sys.argv) > 2 else None
        excel_to_json(excel_file, sheet_name=sheet)

        # Import the ES indexes
        import_index(sheet, es)
