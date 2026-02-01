# Formula 1 ETL Constants
# Note: In a production environment, these could be managed via DAB variables or Databricks Widgets

# Landing zone: where original source files (CSV/JSON) are stored
landing_folder_path = "/mnt/gualterformula1dl/landing"
landing_folder_path_uri = "abfss://landing@gualterformula1dl.dfs.core.windows.net"

# Raw/Bronze layer: source files converted to parquet format
raw_folder_path = "/mnt/gualterformula1dl/raw"
raw_folder_path_uri = "abfss://raw@gualterformula1dl.dfs.core.windows.net"

# Processed/Silver layer: cleansed and transformed data
processed_folder_path = "/mnt/gualterformula1dl/processed"
processed_folder_path_uri = "abfss://processed@gualterformula1dl.dfs.core.windows.net"

# Presentation/Gold layer: aggregated data for analytics
presentation_folder_path = "/mnt/gualterformula1dl/presentation"
presentation_folder_path_uri = (
    "abfss://presentation@gualterformula1dl.dfs.core.windows.net"
)
