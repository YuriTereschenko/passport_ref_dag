INSERT INTO pasp_rating_indicators(indicator_type_id, indicator_value, period_id, region)
SELECT 8                AS indicator_type_id,
       adult_population AS indicator_value,
       ppc.id           AS period_id,
       pp.region
FROM pasp_population pp
         JOIN pasp_period_calendar ppc USING (rep_year)
WHERE rep_year = EXTRACT('year' from '{rep_date}'::DATE)
AND region <> 99
AND ppc.id = {period_id}