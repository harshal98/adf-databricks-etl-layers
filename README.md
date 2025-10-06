# adf-databricks-etl-layers

![Architecture Diagram](docs/Architecture.png)

## Overview

This repository contains an Azure Databricks PySpark ETL pipeline orchestrated with Azure Data Factory (ADF). The solution ingests EMR (Electronic Medical Records) data and transforms it through a **Bronze → Silver → Gold** lakehouse architecture for scalable, structured analytics. Sensitive credentials and secrets are securely managed using **Azure Key Vault**.

## Architecture
 
- **Azure Data Factory**: Orchestrates data movement and transformation workflows.
- **Azure Databricks**: Executes scalable PySpark ETL jobs.
- **Azure Key Vault**: Stores and manages secrets (e.g., database credentials, storage keys).
- **Lakehouse Layers**:
  - **Bronze**: Raw data ingestion.
  - **Silver**: Cleansed and conformed data.
  - **Gold**: Aggregated and business-ready data.


## Key Features

- **Secure Secret Management**: All secrets are accessed via Azure Key Vault, never stored in code or config files.
- **Modular ETL**: Each lakehouse layer is implemented as a separate notebook/pipeline for maintainability.
- **Scalable Data Processing**: Utilizes Databricks clusters for distributed PySpark jobs.
- **Configurable**: ETL parameters and mappings are managed via CSV files in the `configs/` directory.

## Folder Structure

```
configs/         # ETL configuration files (CSV)
datasets/        # Sample EMR and claims datasets
docs/            # Documentation and architecture diagrams
notebooks/       # Databricks notebooks for each ETL layer
pipeline/        # Pipeline workflow images
```

## Security

- Secrets are retrieved at runtime from Azure Key Vault.
- No credentials are stored in notebooks, configs, or code.

## Technologies Used

- Azure Data Factory
- Azure Databricks (PySpark)
- Azure Key Vault
- Azure Data Lake Storage

## License

MIT License

---

