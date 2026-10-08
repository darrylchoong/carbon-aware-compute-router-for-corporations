# Carbon-Aware Compute Router for Corporations

A streaming decision system for routing batch workloads to the lowest-carbon, cost-compliant region in near real time. The solution combines Confluent Cloud, Kafka topics, Schema Registry, and Flink SQL to optimize compute placement for ML training, indexing, rendering, and other data-heavy jobs while honoring budget limits.

## Why this matters

Large enterprises often run the same workloads across multiple cloud regions. However, grid carbon intensity varies by geography and time, and egress charges can erase cost savings. This routing pattern helps select the best execution region by combining:

- regional carbon intensity thresholds
- data transfer cost constraints
- business-defined budget caps
- workload locality preferences

## Architecture overview

```text
Datagen carbon_grid --> carbon_grid topic --> [Flink SQL router] --> routed_orders topic
Datagen job_queue   --> job_queue topic   --> [Flink SQL router] --> routed_orders topic
```

### Pipeline responsibilities

- `carbon_grid`: emits current grid carbon intensity and egress cost by region
- `job_queue`: emits workload metadata such as job origin, data size, and max budget
- `routed_orders`: emits the selected execution target region and routing strategy

## Decision logic

A compute job is eligible when:

- the estimated data transfer cost is within budget
- the region carbon intensity is below the policy threshold (e.g. 150 g/kWh)
- a valid target region exists for the workload

The router chooses the best valid region using the lowest carbon intensity, then breaks ties by lowest egress cost. This avoids the weaker pattern of cross-joining all regions and only filtering afterwards, which creates more data movement than necessary and is harder to reason about.

## Repository layout

```text
.
├── README.md
├── schemas/
│   ├── carbon_grid_schema.json
│   └── job_queue_schema.json
├── sql/
│   ├── pipeline.sql
│   └── monitoring_queries.sql
├── sample-data/
│   ├── carbon_grid_example.json
│   └── job_queue_example.json
└── images/
    └── (architecture screenshots)
```

## Setup guide

### 1. Create Kafka topics

Create these topics in Confluent Cloud:

- `carbon_grid`
- `job_queue`
- `routed_orders`

### 2. Configure Schema Registry

Use the JSON schema definitions in `schemas/` for each source topic. When using Datagen connectors for the first time, set compatibility to `NONE` for the two source subjects before the initial data arrives.

### 3. Deploy source connectors

Deploy a Datagen Source for each topic:

1. `DatagenSource_CarbonGridMetrics` -> `carbon_grid`
2. `DatagenSource_BatchJobQueue` -> `job_queue`

The expected field shapes are defined in:

- `schemas/carbon_grid_schema.json`
- `schemas/job_queue_schema.json`

### 4. Deploy the routing SQL

Use the SQL in `sql/pipeline.sql` in your Flink SQL workspace.

## Core routing SQL

```sql
EXECUTE STATEMENT SET
BEGIN

  INSERT INTO routed_orders
  SELECT
      j.job_id,
      best.target_region,
      best.carbon_intensity_g_kwh,
      best.estimated_egress_cost_usd,
      CASE
          WHEN best.target_region = j.origin_region THEN 'LOCAL_CLEAN_EXECUTION'
          ELSE 'SPATIAL_CARBON_SHIFT'
      END AS routing_strategy
  FROM job_queue j
  LEFT JOIN LATERAL (
      SELECT
          g.region AS target_region,
          CAST(g.carbon_intensity_g_kwh AS DOUBLE) AS carbon_intensity_g_kwh,
          CAST((j.data_size_gb * g.egress_cost_usd_gb) AS DOUBLE) AS estimated_egress_cost_usd
      FROM carbon_grid g
      WHERE g.carbon_intensity_g_kwh < 150.0
        AND (j.data_size_gb * g.egress_cost_usd_gb) <= j.max_cost_budget_usd
      ORDER BY g.carbon_intensity_g_kwh ASC,
               (j.data_size_gb * g.egress_cost_usd_gb) ASC
      LIMIT 1
  ) best
  ON TRUE
  WHERE best.target_region IS NOT NULL;

END;
```

This selects the lowest-carbon feasible region for every eligible workload. If no region qualifies, the job is omitted from `routed_orders` so the operational team can investigate policy violations or under-provisioned capacity.

## Example output

```sql
SELECT *
FROM routed_orders;
```

Example rows:

```text
job_id             target_region  carbon_intensity_g_kwh  estimated_egress_cost_usd  routing_strategy
-----------------------------------------------------------------------------------------------------
job-1001           us-east-1      118.4                   5.20                       SPATIAL_CARBON_SHIFT
job-1002           eu-central-1   96.1                    7.10                       LOCAL_CLEAN_EXECUTION
job-1003           us-west-2      128.7                   4.90                       SPATIAL_CARBON_SHIFT
```

## Operational guidance

- Track routing decisions by region and job type to identify systemic cold spots.
- Add alerts when no eligible region exists for more than a few minutes.
- Validate the policy threshold regularly, especially as grid mixes change seasonally.
- Log both selected region and rejected alternatives for debugging and auditing.

## Tech stack

- Confluent Cloud
- Apache Kafka topics
- Schema Registry
- Flink SQL
- Datagen connectors

## Improvement notes

This version improves on the earlier submission by:

- removing duplicated README content
- clarifying the decision rule and policy threshold
- using a practical best-region selection instead of an unconstrained cross join
- documenting actual schema files and example inputs
- making the repository easier to deploy and reason about
