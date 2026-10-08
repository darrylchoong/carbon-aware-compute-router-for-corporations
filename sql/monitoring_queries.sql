-- Monitor which regions are being selected the most.
SELECT
    target_region,
    COUNT(*) AS routed_jobs,
    ROUND(AVG(carbon_intensity_g_kwh), 2) AS avg_carbon_intensity_g_kwh,
    ROUND(AVG(estimated_egress_cost_usd), 2) AS avg_egress_cost_usd
FROM routed_orders
GROUP BY target_region
ORDER BY routed_jobs DESC;

-- Inspect jobs with no route selected because of policy or cost constraints.
SELECT
    j.job_id,
    j.origin_region,
    j.data_size_gb,
    j.max_cost_budget_usd
FROM job_queue j
LEFT JOIN routed_orders r ON r.job_id = j.job_id
WHERE r.job_id IS NULL;
