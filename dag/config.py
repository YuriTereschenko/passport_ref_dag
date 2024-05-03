from airflow.hooks.base import BaseHook
from pasport_test_ref.core.pg_connect import PgConnect
from pasport_test_ref.core.vertica_connect import VerticaConnect
from pasport_test_ref.core.repository.vertica_repo import VerticaRepository
from pasport_test_ref.core.repository.reportpreprocess_psycopg2_repo import ReportpreprocessPsycopg2Repository
from pasport_test_ref.core.domain.interfaces import ITargetRepository, ISourceRepository


class EnvConfig:
    POSTGRES_DB = '103_170_reportpreprocess_dba'  # change to conn_id
    VERTICA_DB = 'vertica_importSSIS'  # change to vertica conn id


class DependencyConfig:

    @staticmethod
    def pg_conn() -> PgConnect:
        connection = BaseHook.get_connection(EnvConfig.POSTGRES_DB)
        return PgConnect(
            str(connection.host),
            str(connection.port),
            str(connection.schema),
            str(connection.login),
            str(connection.password)
        )

    @staticmethod
    def vertica_conn() -> VerticaConnect:
        connection = BaseHook.get_connection(EnvConfig.VERTICA_DB)
        return VerticaConnect(
            str(connection.host),
            str(connection.port),
            str(connection.schema),
            str(connection.login),
            str(connection.password)
        )

    class Rpository:
        @staticmethod
        def sorce_repo() -> ISourceRepository:
            return VerticaRepository(DependencyConfig.vertica_conn())

        @staticmethod
        def target_repo() -> ITargetRepository:
            return ReportpreprocessPsycopg2Repository(DependencyConfig.pg_conn())
