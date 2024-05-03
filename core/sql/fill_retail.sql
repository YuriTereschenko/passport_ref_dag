INSERT INTO pasp_rating_indicators(indicator_type_id, indicator_value, period_id, region)
SELECT {indicator_type_id}            AS indicator_type_id,
       SUM({metric_column_name}) AS indicator_value,
       pc.id        AS period_id,
       ti.region
FROM temp_indicators_retail ti
         JOIN pasp_period_calendar pc ON ti.rep_year = pc.rep_year AND
                                         ti.rep_month BETWEEN EXTRACT('month' FROM pc.date_begin) AND EXTRACT('month' FROM pc.date_end)
         JOIN pasp_regions pr on ti.region = pr.code
WHERE ti.region <> 99
and pc.id = {period_id}
GROUP BY pc.id, ti.region