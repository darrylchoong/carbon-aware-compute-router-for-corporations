# Carbon-Aware Compute Router for Corporations

A streaming pipeline driven by events, implemented through **Confluent Cloud** and **Flink SQL**, for dynamically allocating batch computing tasks such as machine learning training, indexing, and rendering to geographic locations within clouds that have the minimum carbon footprint from the power grid, with hard budget constraints on costs.

---

## Architecture Overview

```text
[ Datagen Source: carbon_grid ] ---> ( carbon_grid topic ) ------\
                                                                  +---> [ Confluent Flink SQL ] ---> ( routed_orders topic )
[ Datagen Source: job_queue   ] ---> ( job_queue topic   ) ------/
```

- **Data Ingestion:** Two fully managed Confluent **Datagen Source Connectors** (`DatagenSource_CarbonGridMetrics` and `DatagenSource_BatchJobQueue`) stream continuous real-time telemetry for grid carbon intensity and incoming batch compute requests.
- **Schema Governance:** **Confluent Schema Registry** enforces strict `JSON_SR` schemas across all input topics and output sinks.
- **Stream Processing Engine:** **Confluent Cloud Flink SQL** continuously executes spatial stream cross-joins (`CROSS JOIN`), evaluates data egress budgets, and filters out high-carbon execution regions ($< 150 \text{ g/kWh}$).
- **Dynamic Routing Dispatch:** Emits carbon-optimized execution orders to `routed_orders` with assigned execution strategies (`SPATIAL_CARBON_SHIFT` vs. `LOCAL_CLEAN_EXECUTION`).

---

## Repository Layout

```text
carbon-aware-compute-router-for-corporations/
├── README.md
├── sql/
│   └── pipeline.sql
│── schemas/
    ├── carbon_grid_schema.json
    └── job_queue_schema.json

```

---

## Setup & Execution Guide

### 1. Topic Creation & Governance
Create three topics in Confluent Cloud:
- `carbon_grid`
- `job_queue`
- `routed_orders`

*Note: Update the Schema Registry compatibility setting to `NONE` for `carbon_grid-value` and `job_queue-value` subjects to allow initial schema creation from Datagen connectors.*

### 2. Managed Connector Configuration
Deploy two **Datagen Source Connectors** in Confluent Cloud using `JSON_SR` output format:
1. **`DatagenSource_CarbonGridMetrics`** $\rightarrow$ Target topic: `carbon_grid` (use schema from `schemas/carbon_grid_schema.json`)
2. **`DatagenSource_BatchJobQueue`** $\rightarrow$ Target topic: `job_queue` (use schema from `schemas/job_queue_schema.json`)

### 3. Deploy Flink SQL Pipeline
Execute the following statement set inside your Confluent Cloud Flink SQL Workspace:

```sql
EXECUTE STATEMENT SET
BEGIN

  INSERT INTO routed_orders
  SELECT 
      j.job_id,
      g.region AS target_region,
      CAST(g.carbon_intensity_g_kwh AS DOUBLE) AS carbon_intensity_g_kwh,
      CAST((j.data_size_gb * g.egress_cost_usd_gb) AS DOUBLE) AS estimated_egress_cost_usd,
      CASE 
          WHEN g.region = j.origin_region THEN 'LOCAL_CLEAN_EXECUTION'
          ELSE 'SPATIAL_CARBON_SHIFT'
      END AS routing_strategy
  FROM job_queue j
  CROSS JOIN carbon_grid g
  WHERE (j.data_size_gb * g.egress_cost_usd_gb) <= j.max_cost_budget_usd
    AND g.carbon_intensity_g_kwh < 150.0;

END;
```

---

## Querying Output Stream

To observe live carbon-routed compute jobs:

```sql
SELECT * FROM routed_orders;
```

---

## Tech Stack
- **Streaming Platform:** Confluent Cloud (Apache Kafka)
- **Stream Processing:** Confluent Cloud Flink SQL
- **Schema Management:** Confluent Schema Registry (`JSON_SR`)
- **Data Generation:** Managed Datagen Connectors
```

---

## Setup & Execution Guide

### 1. Topic Creation & Governance
Create three topics in Confluent Cloud:
- `carbon_grid`
- `job_queue`
- `routed_orders`

*Note: Update the Schema Registry compatibility setting to `NONE` for `carbon_grid-value` and `job_queue-value` subjects to allow initial schema creation from Datagen connectors.*

### 2. Managed Connector Configuration
Deploy two **Datagen Source Connectors** in Confluent Cloud using `JSON_SR` output format:
1. **`DatagenSource_CarbonGridMetrics`** $\rightarrow$ Target topic: `carbon_grid` (use schema from `schemas/carbon_grid_schema.json`)
2. **`DatagenSource_BatchJobQueue`** $\rightarrow$ Target topic: `job_queue` (use schema from `schemas/job_queue_schema.json`)

### 3. Deploy Flink SQL Pipeline
Execute the following statement set inside your Confluent Cloud Flink SQL Workspace:

```sql
EXECUTE STATEMENT SET
BEGIN

  INSERT INTO routed_orders
  SELECT 
      j.job_id,
      g.region AS target_region,
      CAST(g.carbon_intensity_g_kwh AS DOUBLE) AS carbon_intensity_g_kwh,
      CAST((j.data_size_gb * g.egress_cost_usd_gb) AS DOUBLE) AS estimated_egress_cost_usd,
      CASE 
          WHEN g.region = j.origin_region THEN 'LOCAL_CLEAN_EXECUTION'
          ELSE 'SPATIAL_CARBON_SHIFT'
      END AS routing_strategy
  FROM job_queue j
  CROSS JOIN carbon_grid g
  WHERE (j.data_size_gb * g.egress_cost_usd_gb) <= j.max_cost_budget_usd
    AND g.carbon_intensity_g_kwh < 150.0;

END;
```

---

## Querying Output Stream

To observe live carbon-routed compute jobs:

```sql
SELECT * FROM routed_orders;
```

---

## Tech Stack
- **Streaming Platform:** Confluent Cloud (Apache Kafka)
- **Stream Processing:** Confluent Cloud Flink SQL
- **Schema Management:** Confluent Schema Registry (`JSON_SR`)
- **Data Generation:** Managed Datagen Connectors
```

---

## Setup & Execution Guide

### 1. Topic Creation & Governance
Create three topics in Confluent Cloud:
- `carbon_grid`
- `job_queue`
- `routed_orders`

*Note: Update the Schema Registry compatibility setting to `NONE` for `carbon_grid-value` and `job_queue-value` subjects to allow initial schema creation from Datagen connectors.*

### 2. Managed Connector Configuration
Deploy two **Datagen Source Connectors** in Confluent Cloud using `JSON_SR` output format:
1. **`DatagenSource_CarbonGridMetrics`** $\rightarrow$ Target topic: `carbon_grid` (use schema from `schemas/carbon_grid_schema.json`)
2. **`DatagenSource_BatchJobQueue`** $\rightarrow$ Target topic: `job_queue` (use schema from `schemas/job_queue_schema.json`)

### 3. Deploy Flink SQL Pipeline
Execute the following statement set inside your Confluent Cloud Flink SQL Workspace:

```sql
EXECUTE STATEMENT SET
BEGIN

  INSERT INTO routed_orders
  SELECT 
      j.job_id,
      g.region AS target_region,
      CAST(g.carbon_intensity_g_kwh AS DOUBLE) AS carbon_intensity_g_kwh,
      CAST((j.data_size_gb * g.egress_cost_usd_gb) AS DOUBLE) AS estimated_egress_cost_usd,
      CASE 
          WHEN g.region = j.origin_region THEN 'LOCAL_CLEAN_EXECUTION'
          ELSE 'SPATIAL_CARBON_SHIFT'
      END AS routing_strategy
  FROM job_queue j
  CROSS JOIN carbon_grid g
  WHERE (j.data_size_gb * g.egress_cost_usd_gb) <= j.max_cost_budget_usd
    AND g.carbon_intensity_g_kwh < 150.0;

END;
```

---

## Querying Output Stream

To observe live carbon-routed compute jobs:

```sql
SELECT * FROM routed_orders;
```

---

## Tech Stack
- **Streaming Platform:** Confluent Cloud (Apache Kafka)
- **Stream Processing:** Confluent Cloud Flink SQL
- **Schema Management:** Confluent Schema Registry (`JSON_SR`)
- **Data Generation:** Managed Datagen Connectors
