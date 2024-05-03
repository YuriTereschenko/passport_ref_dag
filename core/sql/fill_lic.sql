INSERT INTO pasp_rating_indicators(indicator_type_id, indicator_value, period_id, region)
SELECT {indicator_type_id}  AS indicator_type_id,
       {field} AS indicator_value,
       period_id,
       region
FROM temp_indicators_lic;