# **Deploy ETLT Infrastructure with Airbyte, Minio, Clickhouse, Dbt and Airflow**
[![Airbyte](https://img.shields.io/badge/-Airbyte-4F8DFD?style=flat&logo=airbyte&logoColor=white)](https://airbyte.com/)
[![MinIO](https://img.shields.io/badge/-MinIO-EF2D5E?style=flat&logo=minio&logoColor=white)](https://min.io/)
[![ClickHouse](https://img.shields.io/badge/-ClickHouse-FFDD00?style=flat&logo=clickhouse&logoColor=black)](https://clickhouse.com/)
[![dbt](https://img.shields.io/badge/-dbt-FF694B?style=flat&logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Airflow](https://img.shields.io/badge/-Airflow-017CEE?style=flat&logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![Python](https://img.shields.io/badge/-Python-3776AB?style=flat&logo=python&logoColor=white)](https://python.org/)
[![SQL](https://img.shields.io/badge/-SQL-4479A1?style=flat&logo=postgresql&logoColor=white)](https://en.wikipedia.org/wiki/SQL)
[![Docker](https://img.shields.io/badge/-Docker-2496ED?style=flat&logo=docker&logoColor=white)](https://www.docker.com/)


This project sets up an analytics architecture using Docker containers for data extraction, storage, orchestration, and transformation.

- Airbyte (Extract) retrieves data from sources and deposits it into MinIO (Data Lake).
- In MinIO, an initial transformation (Python) is performed to clean or enrich the files.
- The data is then loaded (Load) from MinIO into ClickHouse, which is why "Load" appears under ClickHouse.
- Finally, dbt (Transform) runs SQL models that transform the data directly in ClickHouse to make it ready for analysis.


![flowchart](assets/etlt-diagram.png)

---

## Prerequisites

- **Docker**, **Docker Compose**, and **DBeaver** installed on your machine.
- Internet access to download required images.

---

## Included Services

- **Airbyte**: Data integration tool, installed locally via Docker.
- **Minio**: Data lake for storing data extracted by Airbyte.
- **Airflow**: ETLT job orchestrator.
- **Clickhouse**: Data warehouse for analytics.
- **dbt**: Modular data transformation in the warehouse.

---

## Local Installation

### 1. **Airbyte Installation**

Airbyte is installed locally via Docker in this setup. For production or Kubernetes environments, refer to the [Airbyte OSS installation guide](https://docs.airbyte.com/platform/deploying-airbyte). Example Helm installation:

```bash
helm install \
  airbyte \
  airbyte/airbyte \
  --namespace airbyte \
  --values ./values.yaml
```

We might also use this fastest way to install a QuickStart Aribyte by following different steps explained on [Airbyte website](https://docs.airbyte.com/platform/using-airbyte/getting-started/oss-quickstart).

Once Airbyte is installed, the first step is to connect it to your weather API (e.g., VisualCrossing). You can use Airbyte's API connector builder for this purpose. Review the [`custom_api_builder.yaml`](airbyte/custom_api_builder.yaml) file for connection details—this file outlines the configuration needed to connect to the VisualCrossing API, including authentication and endpoint setup. You can recreate the connector using Airbyte's no-code interface or import this YAML file.

To configure the connector, you will need:
- **API Key** from your VisualCrossing account.
- **Base URL**: `https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timelinemulti` (supports up to five locations per request for free accounts). See [VisualCrossing documentation](https://www.visualcrossing.com/resources/documentation/weather-api/using-the-timeline-weather-api-with-multiple-locations-in-the-same-request/) for details.
- **Locations**: Specify locations in the format `London,UK|Paris,France|Tokyo,Japan|Cape Town,South Africa`.

For guidance on building custom API connectors, refer to the [Airbyte Connector Builder tutorial](https://docs.airbyte.com/platform/connector-development/connector-builder-ui/tutorial).

In this project, multiple source connectors are created—each handling a chunk of locations (to cover all 99 departments of metropolitan France)—with MinIO as the destination.

Airbyte supports S3-compatible storage, and MinIO works seamlessly with Amazon S3 APIs. To set up the destination:
- Obtain your MinIO endpoint URL, access key, and secret key.
- In Airbyte, select the S3-compatible destination connector and enter your MinIO details.
- Ensure your MinIO bucket has read/write permissions and encryption in transit is enabled.

If you encounter issues, consult the [Airbyte S3 destination documentation](https://docs.airbyte.com/integrations/destinations/s3) or reach out to the Airbyte community.



### 2. **Clone the repository**:
  ```bash
  git clone https://github.com/donat-konan33/EtltAirbyteMinioClickhouseDbtAirflow.git
  cd EtltAirbyteMinioClickhouseDbtAirflow
  ```

### 3. **Start the infrastructure**:
  ```bash
  docker compose up -d
  docker compose -f docker-compose-minio-clickhouse up -d
  ```

### 4. **Access web interfaces**:
  - Airbyte: [http://localhost:8000](http://localhost:8000)
  - Minio: [http://localhost:9002](http://localhost:9002)
  - Airflow: [http://localhost:8080](http://localhost:8080)
  - Clickhouse: [http://localhost:8123](http://localhost:8123)

### 5. **Connect to** **Clickhouse** using **DBeaver** and view and checkout the DBT transformations achieved.

---

## Notes

This project is under development.
Refer to each service's documentation for more details.
It is important to create a local `.env` file with the same contents as `.env.example`.

If you have questions or need clarification about any part of this setup, feel free to reach out for assistance.

## Environment Variables

Here are variables you need for this project by refering to [`.env.example`](.env.example):

| Variable                   | Description                                               |
|----------------------------|-----------------------------------------------------------|
| **MINIO_USER_NAME**        | Username for MinIO user                                   |
| **MINIO_USER_PASSWORD**    | Password for MinIO user                                   |
| **AWS_ACCESS_KEY_ID**      | AWS access key, set from MinIO username                   |
| **AWS_SECRET_ACCESS_KEY**  | AWS secret key, set from MinIO password                   |
| **MINIO_HOST**             | Host address for MinIO server                             |
| **MINIO_API_PORT**         | Internal port for MinIO API                               |
| **CLICKHOUSE_DB**          | Name of ClickHouse database                               |
| **CLICKHOUSE_USER**        | Username for ClickHouse                                   |
| **CLICKHOUSE_PASSWORD**    | Password for ClickHouse user                              |
| **CLICKHOUSE_HOST**        | Host address for ClickHouse server                        |
| **CLICKHOUSE_HOSTNAME**    | Hostname for ClickHouse server                            |
| **POSTGRES_PASSWORD**      | Password for PostgreSQL user                              |
| **POSTGRES_USER**          | Username for PostgreSQL                                   |
| **POSTGRES_DB**            | Name of PostgreSQL database                               |
| **POSTGRES_HOST**          | Host address for PostgreSQL server                        |
| **HOST**                   | Host address for Airbyte server                           |
| **AIRBYTE_URL**            | URL for Airbyte instance                                  |
| **AIRBYTE_USER**           | Username for Airbyte                                      |
| **AIRBYTE_PASSWORD**       | Password for Airbyte user                                 |
| **LAKE_BUCKET**            | Name of first lake bucket for Airflow if Airflow Variable |
| **AIRFLOW_ADMIN_EMAIL**    | Email address for Airflow admin user                      |
| **AIRFLOW_ADMIN_USERNAME** | Username for Airflow admin user                           |
| **AIRFLOW_ADMIN_PASSWORD** | Password for Airflow admin user                           |
| **AIRFLOW_ADMIN_FIRST_NAME**| First name of Airflow admin user                         |
| **AIRFLOW_ADMIN_LAST_NAME** | Last name of Airflow admin user                          |
---
