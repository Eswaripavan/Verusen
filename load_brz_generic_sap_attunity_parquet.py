# Databricks notebook source
from pyspark.dbutils import DBUtils
dbutils.widgets.text("brewdat_library_version", "v0.7.0", "01 - brewdat_library_version")
brewdat_library_version = dbutils.widgets.get("brewdat_library_version")
print(f"{brewdat_library_version = }")


dbutils.widgets.text("source_system", "sap_g_erp", "02 - source_system")
source_system = dbutils.widgets.get("source_system")
print(f"{source_system = }")

dbutils.widgets.text("source_system_raw", "sap_ecc_ero", "02 - source_system_raw")
source_system_raw = dbutils.widgets.get("source_system_raw")
print(f"{source_system_raw = }")

dbutils.widgets.text("source_table", "bsak", "03 - source_table")
source_table = dbutils.widgets.get("source_table")
print(f"{source_table = }")

dbutils.widgets.text("target_zone", "europe", "04 - target_zone")
target_zone = dbutils.widgets.get("target_zone")
print(f"{target_zone = }")

dbutils.widgets.text("target_business_domain", "supl", "05 - target_business_domain")
target_business_domain = dbutils.widgets.get("target_business_domain")
print(f"{target_business_domain = }")

dbutils.widgets.text("target_database", "brz_eur_supl_sap_g_erp", "06 - target_database")
target_database = dbutils.widgets.get("target_database")
print(f"{target_database = }")

dbutils.widgets.text("target_table", "bsak", "07 - target_table")
target_table = dbutils.widgets.get("target_table")
print(f"{target_table = }")

dbutils.widgets.text("reset_stream_checkpoint", "false", "08 - reset_stream_checkpoint")
reset_stream_checkpoint = dbutils.widgets.get("reset_stream_checkpoint")
print(f"{reset_stream_checkpoint = }")

# COMMAND ----------

import sys
from pyspark.sql import functions as F

# Import BrewDat Library modules and share dbutils globally
sys.path.append(f"/Workspace/Repos/brewdat_library/{brewdat_library_version}")
from brewdat.data_engineering import common_utils, lakehouse_utils, read_utils, transform_utils, write_utils
common_utils.set_global_dbutils(dbutils)

# Print a module's help
# help(read_utils)

# COMMAND ----------

# MAGIC %run "../lakehouse_utils"

# COMMAND ----------

lakehouse_utils.assert_valid_business_domain = assert_valid_business_domain
lakehouse_utils.assert_valid_zone = assert_valid_zone

# COMMAND ----------

# MAGIC %run "../set_project_context"

# COMMAND ----------

# Configure SPN for all ADLS access using AKV-backed secret scope
common_utils.configure_spn_access_for_adls(
    storage_account_names=[
        adls_raw_bronze_storage_account_name
    ],
    key_vault_name=key_vault_name,
    spn_client_id=spn_client_id,
    spn_secret_name=spn_secret_name,
)

# COMMAND ----------

raw_location = f"{lakehouse_raw_root}/data/{target_zone}/tech/{source_system_raw}/attunity/file.{source_table}"
print(f"{raw_location = }")

# COMMAND ----------

base_df = (
    read_utils.read_raw_streaming_dataframe(
        file_format=read_utils.RawFileFormat.PARQUET,
        location=f"{raw_location}/*.parquet",
        schema_location=raw_location,
        cast_all_to_string=True,
        handle_rescued_data=True,
        additional_options={
            "cloudFiles.useIncrementalListing": "false",
        },
    )
    .withColumn("__src_file", F.input_file_name())
    .transform(transform_utils.clean_column_names)
    .transform(transform_utils.create_or_replace_audit_columns)
)

# display(base_df)

# COMMAND ----------

# Check whether ct table exists
# ct_table_exists = False
# try:
#     dbutils.fs.ls(f"{raw_location}__ct")
#     ct_table_exists = True 
# except Exception as e:
#     print(f"Could not locate ct table: {e}")
    
# Check whether ct table exists
ct_table_exists = False
try:
    files = dbutils.fs.ls(f"{raw_location}__ct")
    ct_table_exists = True if files else False
except Exception as e:
    print(f"Could not locate ct table: {e}")

print(ct_table_exists)


# COMMAND ----------

print(ct_table_exists)

# COMMAND ----------

if ct_table_exists:
    ct_df = (
        read_utils.read_raw_streaming_dataframe(
            file_format=read_utils.RawFileFormat.PARQUET,
            location=f"{raw_location}__ct/*/*.parquet",
            schema_location=f"{raw_location}__ct",
            cast_all_to_string=True,
            handle_rescued_data=True,
        )
        .withColumn("__src_file", F.input_file_name())
        .transform(transform_utils.clean_column_names)
        .transform(transform_utils.create_or_replace_audit_columns)
      )
    union_df = base_df.unionByName(ct_df, allowMissingColumns=True)
else:
    union_df = base_df
    header_columns = ['header__change_seq', 'header__change_oper', 'header__stream_position', 'header__operation', 'header__transaction_id', 'header__timestamp', 'header__partition_name']
    for c in header_columns:
        union_df = union_df.withColumn(c, F.lit(None).cast("string"))
    col_expr=union_df.columns
    union_df = union_df.select(*col_expr)

# COMMAND ----------

target_location = lakehouse_utils.generate_bronze_table_location(
    lakehouse_bronze_root=lakehouse_bronze_root,
    target_zone=target_zone,
    target_business_domain=target_business_domain,
    source_system=source_system,
    table_name=target_table,
  
)
print(f"{target_location = }")

# COMMAND ----------

results = write_utils.write_stream_delta_table(
    df=union_df,
    location=target_location,
    database_name=target_database,
    table_name=target_table,
    load_type=write_utils.LoadType.APPEND_ALL,
    partition_columns=["TARGET_APPLY_DT"],
    schema_evolution_mode=write_utils.SchemaEvolutionMode.ADD_NEW_COLUMNS,
    enable_change_data_feed=True,
    enable_caching=False,
    reset_checkpoint=(reset_stream_checkpoint.lower() == "true"),
)
print(results)

# COMMAND ----------

common_utils.exit_with_object(results)

# COMMAND ----------


# dbutils.fs.ls("abfss://raw@brewdateuroperawbrzd.dfs.core.windows.net/data/europe/tech/sap_ecc_ero/attunity/file.bsak__ct")

# COMMAND ----------



# %sql

# select * from brz_eur_supl_sap_g_erp.bsak_20230313 where MANDT=100 and 
# -- BUKRS='NL11' and LIFNR='0000346082' and AUGDT='2021-03-03' AND  AUGBL='2000009603' AND ZUONR='0000001000765' AND GJAHR='2021' AND 
# BELNR='3100002543' AND BUZEI='002'AND HEADER__CHANGE_SEQ='20230312225832000000009487034351616' AND header__change_oper='U'


# -- select * from brz_eur_supl_sap_g_erp.bsak_20230313
# -- where


# COMMAND ----------

dbutils.fs.ls(f"abfss://raw@brewdateuroperawbrzd.dfs.core.windows.net/data/europe/tech/sap_ecc_ero/attunity/file.afpo/")

# COMMAND ----------

dbutils.fs.ls(f"abfss://raw@brewdateuroperawbrzd.dfs.core.windows.net/data/europe/tech/sap_ecc_ero/attunity/file.afpo__ct/")

# COMMAND ----------