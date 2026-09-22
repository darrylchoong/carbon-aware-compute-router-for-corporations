-- ============================================================================
-- Carbon-Aware Compute Router - Continuous Flink SQL Pipeline
-- ============================================================================

EXECUTE STATEMENT SET
BEGIN

  INSERT INTO routed_orders
  SELECT 
      j.job_id,
      g.region AS target_region,
      g.carbon_intensity_g_kwh,
      (j.data_size_gb * g.egress_cost_usd_gb) AS estimated_egress_cost_usd,
      CASE 
          WHEN g.region = j.origin_region THEN 'LOCAL_CLEAN_EXECUTION'
          ELSE 'SPATIAL_CARBON_SHIFT'
      END AS routing_strategy
  FROM job_queue j
  CROSS JOIN carbon_grid g
  WHERE (j.data_size_gb * g.egress_cost_usd_gb) <= j.max_cost_budget_usd
    AND g.carbon_intensity_g_kwh < 150;

END;