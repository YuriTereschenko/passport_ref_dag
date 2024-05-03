SELECT COALESCE(SUM(rpo), 0)           AS rpo,
       COALESCE(SUM(rpa_rpo), 0)       AS rpa_rpo,
       COALESCE(SUM(cancelled_lic), 0) AS cancelled_lic,
       {period_id}                     AS period_id,
       kr.regionCode                   AS region
FROM nsi.kladr_regions kr
         LEFT JOIN
     (SELECT COUNT(DISTINCT c.Owner_ID) AS rpo,
             0                          AS rpa_rpo,
             0                          AS cancelled_lic,
             c.Region_Code
      FROM DWH.nsi.LicenseReestr lr
               JOIN nsi.NSI_Clients c ON lr.inn_org = c.inn AND COALESCE(lr.kpp_unit, '') = COALESCE(c.KPP, '')
      WHERE lr.BigGroup = 800
        AND '{period_end}'::DATE BETWEEN lr.date_begin AND lr.date_end
        AND lr.info_about_lic = 'действующая'
      GROUP BY c.Region_Code

      UNION ALL
      SELECT 0                          AS rpo,
             COUNT(DISTINCT c.Owner_ID) AS rpa_rpo,
             0                          AS cancelled_lic,
             c.Region_Code
      FROM DWH.nsi.LicenseReestr lr
               JOIN nsi.NSI_Clients c ON lr.inn_org = c.inn AND COALESCE(lr.kpp_unit, '') = COALESCE(c.KPP, '')
      WHERE lr.BigGroup IN (799, 800)
        AND '{period_end}'::DATE BETWEEN lr.date_begin AND lr.date_end
        AND lr.info_about_lic = 'действующая'
      GROUP BY c.Region_Code

      UNION ALL

      SELECT 0                    AS rpo,
             0                    AS rpa_rpo,
             COUNT(lr.lic_number) AS cancelled_lic,
             c.Region_Code        AS Region_Code
      FROM DWH.nsi.LicenseReestr lr
               JOIN nsi.NSI_Clients c ON lr.inn_org = c.inn AND COALESCE(lr.kpp_unit, '') = COALESCE(c.KPP, '')

      WHERE lr.BigGroup IN (799, 800)
        AND lr.date_change_info_about_lic BETWEEN '{period_begin}'::DATE AND '{period_end}'::DATE
        AND lr.info_about_lic = 'аннулирована'

      GROUP BY c.Region_Code) t
     ON kr.regionCode = t.Region_Code
WHERE kr.regionCode <> '99'
  AND kr.isActive = TRUE
GROUP BY kr.regionCode;
