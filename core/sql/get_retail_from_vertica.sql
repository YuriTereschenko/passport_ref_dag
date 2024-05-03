SELECT coalesce(SUM(ap_withou_beer), 0)      AS ap_withou_beer,
       coalesce(SUM(wine),0 )                AS wine,
       coalesce(SUM(alcoholic_drink), 0)     AS alcoholic_drink,
       coalesce(SUM(vodka), 0)               AS vodka,
       coalesce(SUM(cognac), 0)              AS cognac,
       coalesce(SUM(beer), 0)                AS beer,
       coalesce(SUM(full_ap) + SUM(beer), 0) AS full_ap,
       rep_year,
       rep_month,
       region::INT
FROM (SELECT SUM(CASE
                     WHEN pc.code = 'АП' AND pc.id <> 7
                         THEN CASE WHEN pcb.vozvrat = TRUE THEN p.Capacity * -1 ELSE p.Capacity END END) AS ap_withou_beer,
             SUM(CASE
                     WHEN pc.id = 3
                         THEN CASE WHEN pcb.vozvrat = TRUE THEN p.Capacity * -1 ELSE p.Capacity END END) AS wine,
             SUM(CASE
                     WHEN pc.id = 1
                         THEN CASE WHEN pcb.vozvrat = TRUE THEN p.Capacity * -1 ELSE p.Capacity END END) AS alcoholic_drink,
             SUM(CASE
                     WHEN ps.id = 1
                         THEN CASE WHEN pcb.vozvrat = TRUE THEN p.Capacity * -1 ELSE p.Capacity END END) AS vodka,
             SUM(CASE
                     WHEN ps.id = 2
                         THEN CASE WHEN pcb.vozvrat = TRUE THEN p.Capacity * -1 ELSE p.Capacity END END) AS cognac,
             SUM(CASE
                     WHEN pc.code = 'АП'
                         THEN CASE WHEN pcb.vozvrat = TRUE THEN p.Capacity * -1 ELSE p.Capacity END END) AS full_ap,

             0                                                                                           AS beer,


             MONTH(pcb.dt)                                                                               AS rep_month,
             YEAR(pcb.dt)                                                                                AS rep_year,
             c.Region_Code                                                                               AS region
      FROM egais_or_cheque.production_chq_btl pcb
               JOIN nsi.NSI_Products p ON pcb.alc_code = p.Alc_Code
               JOIN nsi.Products_subcategory_lnk psl ON p.ProductType_Code = psl.type_code
               JOIN nsi.Products_subcategory ps ON Ps.id = psl.products_subcategory_id
               JOIN nsi.Products_category pc ON Pc.id = Ps.products_category_id
               JOIN nsi.NSI_Clients c ON c.Owner_ID = pcb.ownerId
      WHERE 1 = 1
        AND pc.active = TRUE
        AND pcb.dt::DATE BETWEEN '{start_date}'::DATE AND '{end_date}'::DATE
      GROUP BY MONTH(pcb.dt), YEAR(pcb.dt), c.Region_Code
      UNION ALL

      SELECT 0                       AS ap_withou_beer,
             0                       AS wine,
             0                       AS alcoholic_drink,
             0                       AS vodka,
             0                       AS cognac,
             0                       AS full_ap,
             SUM(aw.Volume_dal) * 10 AS beer,
             MONTH(aw.Date)          AS rep_month,
             YEAR(aw.Date)           AS rep_year,
             c.Region_Code           AS region

      FROM utm.FullActWriteOff AS aw
               JOIN nsi.Products_subcategory_lnk psl ON aw.ProductType_Code = psl.type_code
               JOIN nsi.Products_subcategory ps ON psl.products_subcategory_id = ps.id
               JOIN nsi.NSI_Clients c ON c.Owner_ID = aw.Owner_ID
      WHERE aw.Type = 'Реализация'
        AND ps.products_category_id IN (7, 6)
        AND aw.Date::DATE BETWEEN '{start_date}'::DATE AND '{end_date}'::DATE
      GROUP BY c.Region_Code, MONTH(aw.Date), YEAR(aw.Date)) t
WHERE region <> 99
GROUP BY rep_year, rep_month, region;