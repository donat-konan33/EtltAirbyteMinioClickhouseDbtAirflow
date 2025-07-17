# **Deploy ETLT Infrastructure with Airbyte, Minio, Clickhouse, Dbt and Airflow**

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

1. **Airbyte Installation**

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



1. **Clone the repository**:
  ```bash
  git clone https://github.com/donat-konan33/EtltAirbyteMinioClickhouseDbtAirflow.git
  cd EtltAirbyteMinioClickhouseDbtAirflow
  ```

2. **Start the infrastructure**:
  ```bash
  docker compose up -d
  docker compose -f docker-compose-minio-clickhouse up -d
  ```

3. **Access web interfaces**:
  - Airbyte: [http://localhost:8000](http://localhost:8000)
  - Minio: [http://localhost:9002](http://localhost:9002)
  - Airflow: [http://localhost:8080](http://localhost:8080)
  - Clickhouse: [http://localhost:8123](http://localhost:8123)

4. Connect to **Clickhouse** using **DBeaver** and view and checkout the DBT transformations achieved.

---

## Notes

This project is under development.
Refer to each service's documentation for more details.
It is important to create a local `.env` file with the same contents as `.env.example`.

If you have questions or need clarification about any part of this setup, feel free to reach out for assistance.
