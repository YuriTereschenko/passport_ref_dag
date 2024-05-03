INSERT INTO pasp_rating_indicators(indicator_type_id, region, period_id, indicator_value)
SELECT i.id AS indicator_type_id,
       r.code,
       {period_id}    AS period_id,
       NULL AS indicator_value
FROM pasp_regions r,
     pasp_rating_indicators_directory i
WHERE i.manual_filling = TRUE
  AND NOT EXISTS(SELECT 1
                 FROM pasp_rating_indicators
                 WHERE indicator_type_id = i.id
                    AND region = r.code
                    AND period_id = {period_id})
  AND r.code <> 99