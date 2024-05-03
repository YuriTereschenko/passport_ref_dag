DELETE
FROM pasp_rating_indicators
WHERE period_id = {period_id}
  AND indicator_type_id IN (SELECT id FROM pasp_rating_indicators_directory WHERE manual_filling = FALSE)