-- Carbon-aware compute router: select the lowest-carbon feasible region
-- for each queued workload while keeping within budget constraints.

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
