# Databricks notebook source
# Atlas Enhancement — Add identity column, reorder etl_id/file_id, add IS_DELETED
#
# For each of the 18 destination tables, this notebook:
#   1. Reads the current schema
#   2. Creates a new table with:
#      - <table_name>_id  BIGINT GENERATED ALWAYS AS IDENTITY  (first column)
#      - etl_id                                                 (second column)
#      - file_id                                                (third column)
#      - ... remaining columns in original order ...
#      - IS_DELETED BOOLEAN DEFAULT false                       (last column)
#   3. Copies existing data into the new table
#   4. Drops the backup
#
# Run in Databricks on a cluster with access to development_021_bronze_finance.atlas

catalog_schema = "development_021_bronze_finance.atlas"

# All 18 destination tables
tables = [
    "RPM_Performance_CDS_Class_II_ManualBilling",
    "BW_ZRBINQ_BillingPlans",
    "RPM_Exp_RoyaltiesAlliant",
    "RPM_Exp_Depreciation",
    "BW_COPA_DailyFeePoker_Units",
    "BPC_Plug_Financials",
    "BPC_Plug_Financials_MachineCosts",
    "RPM_Peformance_FF_MJP",
    "RPM_Peformance_FF_MJP_PriorMonth",
    "RPM_Peformance_PremLotto_DE",
    "RPM_Peformance_PremLotto_NY",
    "RPM_Peformance_PremLotto_RI",
    "RPM_Peformance_NonPremLotto_RI",
    "RPM_Intl_Performance_EMEA_Africa",
    "RPM_Intl_Performance_EMEA_FixedFee",
    "RPM_Intl_Performance_EMEA_GreeceWLA",
    "RPM_Intl_Performance_EMEA_Iceland",
    "RPM_Intl_Performance_LAC",
]

results = []

for table_name in tables:
    full_table = f"{catalog_schema}.{table_name}"
    backup_table = f"{catalog_schema}.{table_name}_backup"
    identity_col = f"{table_name}_id"

    print(f"\n{'='*60}")
    print(f"Processing: {table_name}")
    print(f"{'='*60}")

    # Check if table exists
    try:
        df = spark.table(full_table)
    except Exception as e:
        print(f"  SKIPPED — table does not exist yet: {e}")
        results.append((table_name, "skipped - does not exist"))
        continue

    # Get current columns and types
    current_columns = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
    current_col_names = [c[0] for c in current_columns]

    print(f"  Current columns: {current_col_names}")

    # Skip if already has identity column
    if identity_col in current_col_names:
        print(f"  SKIPPED — identity column {identity_col} already exists")
        results.append((table_name, "skipped - already enhanced"))
        continue

    # Build ordered column list: etl_id and file_id first, then the rest
    priority_cols = []
    remaining_cols = []

    for col_name, col_type in current_columns:
        if col_name == "etl_id":
            priority_cols.insert(0, (col_name, col_type))
        elif col_name == "file_id":
            if len(priority_cols) > 0 and priority_cols[0][0] == "etl_id":
                priority_cols.append((col_name, col_type))
            else:
                priority_cols.insert(0, (col_name, col_type))
        elif col_name == "IS_DELETED":
            pass  # Will be re-added at the end
        else:
            remaining_cols.append((col_name, col_type))

    ordered_cols = priority_cols + remaining_cols

    # Build column definitions for CREATE TABLE
    col_defs = [f"  {identity_col} BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)"]
    for col_name, col_type in ordered_cols:
        col_defs.append(f"  {col_name} {col_type}")
    col_defs.append(f"  IS_DELETED BOOLEAN")

    create_cols_str = ",\n".join(col_defs)

    # Build column list for INSERT (excluding identity and IS_DELETED)
    insert_cols = [c[0] for c in ordered_cols]
    insert_cols_str = ", ".join(insert_cols)

    # Build SELECT for copying data (in the new column order)
    select_cols_str = ", ".join(insert_cols)

    try:
        # Step 1: Rename existing table to backup
        print(f"  Step 1: Renaming {table_name} -> {table_name}_backup")
        spark.sql(f"ALTER TABLE {full_table} RENAME TO {backup_table}")

        # Step 2: Create new table with identity column + IS_DELETED
        print(f"  Step 2: Creating new {table_name} with {identity_col} + IS_DELETED")
        create_sql = f"""
CREATE TABLE {full_table} (
{create_cols_str}
)
"""
        spark.sql(create_sql)

        # Step 2b: Enable column defaults and set IS_DELETED default
        spark.sql(f"ALTER TABLE {full_table} SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')")
        spark.sql(f"ALTER TABLE {full_table} ALTER COLUMN IS_DELETED SET DEFAULT false")
        print(f"  Step 2b: Enabled IS_DELETED DEFAULT false")

        # Step 3: Copy data from backup
        row_count = spark.table(backup_table).count()
        print(f"  Step 3: Copying {row_count} rows from backup")
        insert_sql = f"""
INSERT INTO {full_table} ({insert_cols_str})
SELECT {select_cols_str}
FROM {backup_table}
"""
        spark.sql(insert_sql)

        # Step 4: Verify row counts match
        new_count = spark.table(full_table).count()
        if new_count == row_count:
            print(f"  Step 4: Row count verified ({new_count} rows)")
            print(f"  Step 5: Dropping backup table")
            spark.sql(f"DROP TABLE {backup_table}")
            results.append((table_name, f"success - {new_count} rows migrated"))
        else:
            print(f"  WARNING: Row count mismatch! Backup={row_count}, New={new_count}")
            print(f"  Keeping backup table for manual review: {backup_table}")
            results.append((table_name, f"WARNING - count mismatch backup={row_count} new={new_count}"))

    except Exception as e:
        print(f"  ERROR: {e}")
        # Try to restore from backup if rename happened
        try:
            spark.sql(f"DROP TABLE IF EXISTS {full_table}")
            spark.sql(f"ALTER TABLE {backup_table} RENAME TO {full_table}")
            print(f"  Restored original table from backup")
        except Exception:
            pass
        results.append((table_name, f"error - {str(e)[:80]}"))

# --- Summary ---
print(f"\n{'='*60}")
print(f"SUMMARY: {len(results)} tables processed")
print(f"{'='*60}")
for name, status in results:
    print(f"  {name}: {status}")
