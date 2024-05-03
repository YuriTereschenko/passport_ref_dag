from pasport_test_ref.core.domain.interfaces import ITargetRepository
from pasport_test_ref.core.domain.entities.region_lic_inf import RegionLicInf
from pasport_test_ref.core.pg_connect import PgConnect
from pasport_test_ref.core.domain.entities.calendar import Calendar
import datetime
from psycopg2.extras import RealDictCursor
from pasport_test_ref.core.domain.entities.region_retail_inf import RegionRetailInf


class ReportpreprocessPsycopg2Repository(ITargetRepository):
    def __init__(self, conn: PgConnect):
        self._conn = conn

    def save_lic_temp(self, lic_inf: list[RegionLicInf]):
        for region in lic_inf:
            with self._conn.connection() as conn:
                with conn.cursor() as curr:
                    curr.execute(
                        '''
                        INSERT INTO temp_indicators_lic(
                            rpo,
                            rpa_rpo,
                            cancelled_lic,
                            period_id,
                            region
                        )
                        VALUES (
                            %(rpo)s,
                            %(rpa_rpo)s,
                            %(cancelled_lic)s,
                            %(period_id)s,
                            %(region)s
                        )
                        ''',
                        {
                            "rpo": region.rpo,
                            "rpa_rpo": region.rpa_rpo,
                            "cancelled_lic": region.cancelled_lic,
                            "period_id": region.period_id,
                            "region": region.region
                        })

    def execute_sql(self, query) -> None:
        with self._conn.connection() as conn:
            with conn.cursor() as curr:
                curr.execute(query)

    def format_and_execute(self, sql_file_path, **kwargs) -> None:
        with open(sql_file_path) as sql_file:
            sql = sql_file.read()
            if len(kwargs) != 0:
                sql = sql.format(**kwargs)
        self.execute_sql(sql)

    def fill_retail_indicators(self, params: list[(str, int)], sql_file_path) -> None:
        for param in params:
            self.format_and_execute()

    def get_relevant_calendar(self, date: datetime.date) -> Calendar:
        with self._conn.connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as curr:
                with open('/airflow/dags/pasport_test_ref/core/sql/get_calendar.sql', 'r') as f:
                    query = f.read()
                    query = query.format(date=date)
                    print(query)
                curr.execute(query)
                row = curr.fetchone()
                row = dict(row)
                return Calendar(period_id=row['period_id'], date_end=row['date_end'], date_begin=row['date_begin'])

    def save_region_retail_inf(self, retail_inf: list[RegionRetailInf]) -> None:
        with self._conn.connection() as conn:
            with conn.cursor() as curr:
                for region in retail_inf:
                    print(region)
                    curr.execute(
                        '''
                        INSERT INTO temp_indicators_retail(
                            ap_without_beer,
                            wine,
                            alcoholic_drink,
                            vodka,
                            cognac,
                            full_ap,
                            beer,
                            rep_year,
                            rep_month,
                            region
                        )
                        VALUES (
                            %(ap_without_beer)s,
                            %(wine)s,
                            %(alcoholic_drink)s,
                            %(vodka)s,
                            %(cognac)s,
                            %(full_ap)s,
                            %(beer)s,
                            %(rep_year)s,
                            %(rep_month)s,
                            %(region)s
                        )
                        ''',
                        {
                            "ap_without_beer": region.ap_withou_beer,
                            "wine": region.wine,
                            "alcoholic_drink": region.alcoholic_drink,
                            "vodka": region.vodka,
                            "cognac": region.cognac,
                            "full_ap": region.full_ap,
                            "beer": region.beer,
                            "rep_year": region.rep_year,
                            "rep_month": region.rep_month,
                            "region": region.region,
                        })
                    print(f"Inserted region {region.region}")
