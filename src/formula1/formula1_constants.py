# Formula 1 ETL Constants
# Note: In a production environment, these could be managed via DAB variables or Databricks Widgets

# Landing zone: where original source files (CSV/JSON) are stored
landing_folder_path = "/mnt/gualterformula1dl/landing"

# Raw/Bronze layer: source files converted to parquet format
raw_folder_path = "/mnt/gualterformula1dl/raw"

# Processed/Silver layer: cleansed and transformed data
processed_folder_path = "/mnt/gualterformula1dl/processed"

# Presentation/Gold layer: aggregated data for analytics
presentation_folder_path = "/mnt/gualterformula1dl/presentation"
