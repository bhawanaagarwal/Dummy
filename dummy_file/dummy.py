# src/file_tracker.py
import pandas as pd
from pathlib import Path
import shutil
import logging

logger = logging.getLogger(__name__)

PROCESSED_LOG = Path("output/processed_files_log.xlsx")

def ensure_output_dirs():
    Path("output").mkdir(parents=True, exist_ok=True)
    Path("archive").mkdir(parents=True, exist_ok=True)

def load_processed_log():
    ensure_output_dirs()
    if PROCESSED_LOG.exists():
        try:
            df = pd.read_excel(PROCESSED_LOG, engine="openpyxl")
            return df
        except Exception as e:
            logger.warning("Failed to read processed log, creating new. Error: %s", e)
    # empty log
    return pd.DataFrame(columns=["file_name", "file_path", "source", "month", "processed_timestamp"])

def save_processed_log(df):
    df.to_excel(PROCESSED_LOG, index=False, engine="openpyxl")

def is_file_processed(log_df, file_path):
    return file_path in log_df["file_path"].astype(str).values

def append_to_log(log_df, file_path, source, month, ts):
    new = {
        "file_name": Path(file_path).name,
        "file_path": str(file_path),
        "source": source,
        "month": month,
        "processed_timestamp": ts
    }
    return pd.concat([log_df, pd.DataFrame([new])], ignore_index=True)

def archive_file(file_path, source):
    """
    Moves the file to archive/<source>/ keeping filename.
    """
    src = Path(file_path)
    dest_dir = Path("archive") / source
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name
    try:
        shutil.move(str(src), str(dest))
        logger.info("Archived %s -> %s", src, dest)
        return dest
    except Exception as e:
        logger.exception("Failed to archive file %s: %s", file_path, e)
        raise


# src/reader.py
import pandas as pd
from pathlib import Path
from typing import List
import logging

logger = logging.getLogger(__name__)

def list_input_files(input_folder: str, pattern="*.xlsx"):
    p = Path(input_folder)
    if not p.exists():
        logger.warning("Input folder doesn't exist: %s", input_folder)
        return []
    return list(p.glob(pattern))

def read_excel_first_sheet(path: Path):
    # returns DataFrame of first sheet
    try:
        df = pd.read_excel(path, engine="openpyxl")
        return df
    except Exception as e:
        logger.exception("Failed to read %s: %s", path, e)
        raise

def read_master_if_exists(path="output/master_library.xlsx"):
    p = Path(path)
    if p.exists():
        try:
            return pd.read_excel(p, engine="openpyxl")
        except Exception as e:
            logger.exception("Failed to read master library %s: %s", path, e)
            raise
    else:
        return None


# src/comparator.py
"""
Compare incoming source DF with master and update master with the rule:
- If a (message_definition, market, month) DOES NOT EXIST -> append row with source_volume set
- If exists -> update source_volume only if existing value is 0 or NaN
Also recalculates total_volume
"""
import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def ensure_master_columns(master: pd.DataFrame, master_key_cols, source_col_name, default_volume=0):
    # Ensure key cols exist
    for c in master_key_cols:
        if c not in master.columns:
            master[c] = None
    # Ensure source column exists
    if source_col_name not in master.columns:
        master[source_col_name] = default_volume
    return master

def update_master_with_source(master: pd.DataFrame,
                              df_source: pd.DataFrame,
                              source_name: str,
                              master_key_cols=["message_definition","market","month"],
                              default_volume=0):
    col = f"{source_name}_volume"
    master = ensure_master_columns(master, master_key_cols, col, default_volume)

    # Ensure master has other source_volume columns initialized to default if not present in new rows.
    existing_volume_cols = [c for c in master.columns if c.endswith("_volume")]

    # Iterate incoming rows
    for _, row in df_source.iterrows():
        key_vals = {k: row[k] for k in master_key_cols}
        # mask for rows that match this key
        mask = pd.Series(True, index=master.index)
        for k, v in key_vals.items():
            mask = mask & (master[k] == v)

        if mask.any():
            # update only if existing value is 0 or NaN
            current_val = master.loc[mask, col].iloc[0]
            if pd.isna(current_val) or int(current_val) == 0:
                master.loc[mask, col] = int(row["volume"])
                logger.debug("Updated existing key %s -> %s = %s", key_vals, col, row["volume"])
            else:
                logger.debug("Skipped update for %s because existing value is non-zero", key_vals)
        else:
            # create new row with defaults
            new_row = {k: row[k] for k in master_key_cols}
            # initialize all current source volume columns to default
            for c in existing_volume_cols:
                new_row[c] = default_volume
            new_row[col] = int(row["volume"])
            master = pd.concat([master, pd.DataFrame([new_row])], ignore_index=True)
            logger.debug("Appended new row: %s", new_row)

    # Recompute total_volume if present or create it
    volume_cols = [c for c in master.columns if c.endswith("_volume")]
    if volume_cols:
        master["total_volume"] = master[volume_cols].sum(axis=1).astype(int)
    else:
        master["total_volume"] = 0

    # Ensure master has consistent column order: keys first, then volumes, then total and others
    cols_order = master_key_cols + sorted(volume_cols) + [c for c in master.columns if c not in master_key_cols + volume_cols + ["total_volume"]] + ["total_volume"]
    # keep only unique cols preserving order
    cols_order_unique = []
    for c in cols_order:
        if c not in cols_order_unique and c in master.columns:
            cols_order_unique.append(c)
    try:
        master = master[cols_order_unique]
    except Exception:
        pass

    return master



# src/writer.py
import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

MASTER_PATH = Path("output/master_library.xlsx")
DIFFS_DIR = Path("output/diff_logs")

def save_master(master_df: pd.DataFrame):
    MASTER_PATH.parent.mkdir(parents=True, exist_ok=True)
    master_df.to_excel(MASTER_PATH, index=False, engine="openpyxl")
    logger.info("Saved master to %s", MASTER_PATH)

def save_diff_log(diff_df: pd.DataFrame, name_suffix: str):
    DIFFS_DIR.mkdir(parents=True, exist_ok=True)
    path = DIFFS_DIR / f"diff_{name_suffix}.xlsx"
    diff_df.to_excel(path, index=False, engine="openpyxl")
    logger.info("Saved diff log to %s", path)



# src/orchestrator.py
import json
from pathlib import Path
import importlib
import logging
import pandas as pd
from datetime import datetime

from src.reader import list_input_files, read_excel_first_sheet, read_master_if_exists
from src.file_tracker import load_processed_log, save_processed_log, is_file_processed, append_to_log, archive_file, ensure_output_dirs
from src.comparator import update_master_with_source
from src.writer import save_master, save_diff_log

logger = logging.getLogger(__name__)

def load_config():
    with open("config/sources.json") as f:
        sources = json.load(f)
    # optionally load other configs
    try:
        with open("config/global_schema.json") as f:
            global_schema = json.load(f)
    except:
        global_schema = {"master_columns": ["message_definition","market","month"], "default_volume": 0}
    return sources, global_schema

def import_preprocessor(name):
    # dynamic import: src.preprocessing.<name>
    module_name = f"src.preprocessing.{name}"
    mod = importlib.import_module(module_name)
    return mod.preprocess

def process_one_file(file_path: Path, source_key: str, source_cfg: dict, master_df: pd.DataFrame, processed_log: pd.DataFrame, global_schema: dict):
    file_path = Path(file_path)
    logger.info("Processing %s for source %s", file_path, source_key)

    # read raw
    df_raw = read_excel_first_sheet(file_path)

    # import preprocessor
    preprocessor_name = source_cfg.get("preprocessor")
    if not preprocessor_name:
        raise ValueError(f"No preprocessor defined for {source_key}")
    preprocess_fn = import_preprocessor(preprocessor_name)

    df_std = preprocess_fn(df_raw, file_path, source_cfg)
    # group by key to get volume per key (some source preprocessors may already do that)
    key_cols = global_schema.get("master_columns", ["message_definition","market","month"])
    df_grouped = df_std.groupby(key_cols, as_index=False)["volume"].sum()

    source_name = source_key  # used to create column <source>_volume

    # Update master (create if not exists)
    if master_df is None:
        # create empty master with key columns
        master_df = pd.DataFrame(columns=key_cols + [])
    before = master_df.copy()

    master_df = update_master_with_source(master_df, df_grouped, source_name, master_key_cols=key_cols, default_volume=global_schema.get("default_volume", 0))

    # Build diff: what changed (new rows + updated rows)
    diff_rows = []
    # Find appended rows by comparing before and after on keys
    merged = master_df.merge(before, on=key_cols, how="left", indicator=True)
    new_rows = merged[merged["_merge"]=="left_only"].drop(columns=["_merge"])
    if not new_rows.empty:
        new_rows["change_type"] = "added"
        diff_rows.append(new_rows)

    # For updates: compare volume columns for this source between before and after where both exist
    col = f"{source_name}_volume"
    if col in before.columns:
        # join to detect changed values
        merged2 = master_df.merge(before[key_cols + [col]], on=key_cols, how="left", suffixes=("_new", "_old"))
        changed = merged2[(merged2[f"{col}_old"].notna()) & (merged2[f"{col}_old"] != merged2[f"{col}_new"])]
        if not changed.empty:
            changed = changed.assign(change_type="updated")
            diff_rows.append(changed)
    else:
        # if before had no such column, all rows where new col > 0 are either appended or updated from default.
        pass

    # Concatenate diff rows if any
    if diff_rows:
        diff_df = pd.concat(diff_rows, ignore_index=True, sort=False)
    else:
        diff_df = pd.DataFrame()

    # Update processed log
    ts = datetime.utcnow().isoformat()
    processed_log = append_to_log(processed_log, str(file_path), source_key, df_grouped["month"].unique().tolist(), ts)

    # Archive file
    archive_file(file_path, source_key)

    return master_df, processed_log, diff_df

def orchestrate():
    ensure_output_dirs()
    sources_cfg, global_schema = load_config()
    processed_log = load_processed_log()
    master_df = read_master_if_exists("output/master_library.xlsx")

    overall_diffs = []
    for source_key, source_cfg in sources_cfg.items():
        input_folder = source_cfg.get("input_folder")
        file_pattern = source_cfg.get("file_pattern", "*.xlsx")
        files = list_input_files(input_folder, file_pattern)
        for f in files:
            if is_file_processed(processed_log, str(f)):
                logger.info("Skipping already processed file: %s", f)
                continue
            try:
                master_df, processed_log, diff_df = process_one_file(f, source_key, source_cfg, master_df, processed_log, global_schema)
                if diff_df is not None and not diff_df.empty:
                    overall_diffs.append((source_key, f.name, diff_df))
            except Exception as e:
                logger.exception("Failed to process %s: %s", f, e)

    # save outputs
    if master_df is not None:
        save_master(master_df)
    save_processed_log(processed_log)

    # save diffs
    for source_key, fname, ddf in overall_diffs:
        name_suffix = f"{source_key}_{fname}_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}"
        save_diff_log(ddf, name_suffix)

    logger.info("Orchestration complete.")



# run_pipeline.py
import logging
from src.orchestrator import orchestrate

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

if __name__ == "__main__":
    setup_logging()
    orchestrate()
