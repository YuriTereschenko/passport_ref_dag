from pasport_test_ref.core.domain.statistic import load_lic_by_periods, load_retail_by_perod
from pasport_test_ref.dag.config import DependencyConfig
from airflow.decorators import dag, task, task_group
from datetime import datetime


@dag(
    schedule=None,
    start_date=datetime(2024, 4, 1),
    catchup=False,
    tags=['test'],
    dag_id='new_passport_stat_test',

)
def statistic_dag():
    source_rep = DependencyConfig.Rpository.sorce_repo()
    target_rep = DependencyConfig.Rpository.target_repo()

    @task(task_id='create_temp_tables')
    def create_temp_tables_task():
        sql_path = '/airflow/dags/pasport_test_ref/core/sql/create_temp_tables.sql'
        target_rep.format_and_execute(sql_path)

    @task(task_id='get_calendar', provide_context=True)
    def get_calendar_task(ti, next_ds):
        calendar = target_rep.get_relevant_calendar(next_ds)
        ti.xcom_push(key='date_begin', value=calendar.date_begin)
        ti.xcom_push(key='date_end', value=calendar.date_end)
        ti.xcom_push(key='period_id', value=calendar.period_id)

    @task(task_id='delete_the_same_period_data')
    def delete_the_same_period_data_task(ti):
        period_id = ti.xcom_pull(key='period_id')
        sql_path = '/airflow/dags/pasport_test_ref/core/sql/delete_the_same_period_data.sql'
        target_rep.format_and_execute(sql_path, period_id=period_id)

    @task_group()
    def load_data_from_vertica():

        @task(task_id='load_lic_by_periods')
        def load_lic_by_periods_task():
            load_lic_by_periods('20240331', '20240101', 5, source_rep, target_rep)

        @task(task_id='load_retail_by_periods')
        def load_retail_by_periods_task(ti):
            date_end = ti.xcom_pull(key='date_end')
            date_begin = ti.xcom_pull(key='date_begin')
            load_retail_by_perod(date_end=date_end, date_begin=date_begin, source=source_rep, target=target_rep)

        load_lic_by_periods_task(), load_retail_by_periods_task()

    @task_group()
    def fill_indicators():
        @task(task_id='fill_retail_indicators')
        def fill_retail_indicators_task(ti):
            sql_path = '/airflow/dags/pasport_test_ref/core/sql/fill_retail.sql'
            period_id = ti.xcom_pull(key='period_id')
            for i in [("ap_without_beer", 1), ("wine", 2), ("alcoholic_drink", 3), ("vodka", 4),
                      ("cognac", 5), ("beer", 6), ("full_ap", 13)]:
                target_rep.format_and_execute(sql_path, period_id=period_id,
                                              metric_column_name=i[0], indicator_type_id=i[1])

        @task(task_id='fill_lic_indicator')
        def fill_lic_indicator_task():
            sql_path = '/airflow/dags/pasport_test_ref/core/sql/fill_lic.sql'
            for i in [('rpo', 7), ('rpa_rpo', 10), ('cancelled_lic', 14)]:
                target_rep.format_and_execute(sql_path, indicator_type_id=i[1], field=i[0])

        @task(task_id='fill_population_indicator')
        def fill_population_indicator_task(ti, next_ds):
            sql_path = '/airflow/dags/pasport_test_ref/core/sql/fill_population.sql'
            period_id = ti.xcom_pull(key='period_id')
            target_rep.format_and_execute(sql_path, period_id=period_id, rep_date=next_ds)

        fill_lic_indicator_task(), fill_retail_indicators_task(), fill_population_indicator_task()

    @task(task_id='drop_temp_table')
    def drop_temp_table_task():
        target_rep.execute_sql('DROP TABLE IF EXISTS temp_indicators_lic;')
        target_rep.execute_sql('DROP TABLE IF EXISTS temp_indicators_retail;')

    @task(task_id='add_empty_rows_for_manual_input')
    def add_empty_rows_for_manual_input_task(ti):
        sql_path = '/airflow/dags/pasport_test_ref/core/sql/add_empty_row_fro_manual_input.sql'
        period_id = ti.xcom_pull(key='period_id')
        target_rep.format_and_execute(sql_path, period_id=period_id)

    (
            [create_temp_tables_task(), get_calendar_task()]
            >> delete_the_same_period_data_task()
            >> load_data_from_vertica()
            >> fill_indicators()
            >> drop_temp_table_task()
            >> add_empty_rows_for_manual_input_task()
    )


dag = statistic_dag()
