SELECT id as period_id,
       date_begin,
       date_end
FROM (SELECT pc.id,
             pc.date_begin,
             pc.date_end,

             ROW_NUMBER() OVER (ORDER BY date_end DESC) AS rw
      FROM pasp_period_calendar pc
      WHERE '{date}'::DATE > date_begin
        AND date_end < '{date}'::DATE) irn
WHERE rw = 1;