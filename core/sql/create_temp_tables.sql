DROP TABLE IF EXISTS temp_indicators_lic;
DROP TABLE IF EXISTS temp_indicators_retail;
CREATE TABLE public.temp_indicators_lic
(
    rpo           BIGINT,
    rpa_rpo       BIGINT,
    cancelled_lic BIGINT,
    period_id     SMALLINT,
    region        SMALLINT
);
CREATE TABLE public.temp_indicators_retail
(
    ap_without_beer NUMERIC(20, 2),
    wine            NUMERIC(20, 2),
    alcoholic_drink NUMERIC(20, 2),
    vodka           NUMERIC(20, 2),
    cognac          NUMERIC(20, 2),
    full_ap         NUMERIC(20, 2),
    beer            NUMERIC(20, 2),
    rep_year        SMALLINT,
    rep_month       SMALLINT,
    region          SMALLINT
);




