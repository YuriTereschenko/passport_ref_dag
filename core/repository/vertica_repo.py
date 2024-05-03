from pasport_test_ref.core.domain.interfaces import ISourceRepository
from pasport_test_ref.core.domain.entities.region_retail_inf import RegionRetailInf
from pasport_test_ref.core.domain.entities.region_lic_inf import RegionLicInf
from pasport_test_ref.core.vertica_connect import VerticaConnect


class VerticaRepository(ISourceRepository):
    def __init__(self, conn: VerticaConnect):
        self._conn = conn

    def load_region_lic_inf(self, date_end, date_begin, period_id) -> list[RegionLicInf]:
        sql_path = '/airflow/dags/pasport_test_ref/core/sql/get_lic_from_vertica.sql'
        with open(sql_path, mode='r', encoding='utf-8') as f:
            query = f.read()
            query = query.format(period_end=date_end, period_begin=date_begin, period_id=str(period_id))

        with self._conn.connection() as conn:
            with conn.cursor() as curr:
                curr.execute(query)
                data = curr.fetchall()
                print(data)
                data = list(data)

        region_lic_inf_list = [
            RegionLicInf(rpo=row[0], rpa_rpo=row[1], cancelled_lic=row[2], period_id=row[3], region=row[4]) for row in
            data]

        return region_lic_inf_list

    def load_region_retail_inf(self, start_date, end_date) -> list[RegionRetailInf]:
        sql_path = '/airflow/dags/pasport_test_ref/core/sql/get_retail_from_vertica.sql'
        with open(sql_path, mode='r', encoding='utf-8') as f:
            query = f.read()
            query = query.format(start_date=start_date, end_date=end_date)

        with self._conn.connection() as conn:
            with conn.cursor() as curr:
                curr.execute(query)
                rows = curr.fetchall()

            region_retail_inf_list = list()
            for row in rows:
                print(row)
                region_retail_inf_list.append(RegionRetailInf(ap_withou_beer=row[0],
                                                              wine=row[1],
                                                              alcoholic_drink=row[2],
                                                              vodka=row[3],
                                                              cognac=row[4],
                                                              beer=row[5],
                                                              full_ap=row[6],
                                                              rep_year=row[7],
                                                              rep_month=row[8],
                                                              region=row[9]))
            return region_retail_inf_list
