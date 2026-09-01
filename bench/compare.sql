SET join_use_nulls = 1;

WITH
baseline AS (
  SELECT
    scenario.job_name AS job_name,
    scenario.input_name AS input_name,
    scenario.run_time.statistics.ips AS baseline_ips,
    scenario.run_time.statistics.median AS baseline_median_ns
  FROM file('bench/compare/baseline.json', JSONEachRow)
  ARRAY JOIN scenarios AS scenario
),
current AS (
  SELECT
    scenario.job_name AS job_name,
    scenario.input_name AS input_name,
    scenario.run_time.statistics.ips AS current_ips,
    scenario.run_time.statistics.median AS current_median_ns
  FROM file('bench/compare/current.json', JSONEachRow)
  ARRAY JOIN scenarios AS scenario
)
SELECT
  coalesce(current.job_name, baseline.job_name) AS benchmark,
  coalesce(current.input_name, baseline.input_name) AS input,
  round(baseline_ips, 2) AS baseline_ips,
  round(current_ips, 2) AS current_ips,
  multiIf(
    baseline_ips IS NULL, 'new',
    current_ips IS NULL, 'removed',
    abs((current_ips / baseline_ips - 1) * 100) < 5, 'no material change',
    current_ips > baseline_ips, 'faster',
    'slower'
  ) AS result,
  if(
    baseline_ips IS NULL OR current_ips IS NULL,
    NULL,
    round((current_ips / baseline_ips - 1) * 100, 2)
  ) AS change_percent,
  round(baseline_median_ns / 1000000, 3) AS baseline_median_ms,
  round(current_median_ns / 1000000, 3) AS current_median_ms
FROM baseline
FULL OUTER JOIN current USING (job_name, input_name)
ORDER BY benchmark, input
FORMAT Markdown;
