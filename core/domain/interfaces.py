import datetime
from abc import ABC, abstractmethod
from pasport_test_ref.core.domain.entities.region_lic_inf import RegionLicInf
from pasport_test_ref.core.domain.entities.calendar import Calendar
from pasport_test_ref.core.domain.entities.region_retail_inf import RegionRetailInf


class ISourceRepository(ABC):
    @abstractmethod
    def load_region_lic_inf(self, date_end, date_begin, period_id, ) -> list[RegionLicInf]:
        raise NotImplementedError

    @abstractmethod
    def load_region_retail_inf(self, start_date, end_date) -> list[RegionRetailInf]:
        raise NotImplementedError


class ITargetRepository(ABC):
    @abstractmethod
    def save_lic_temp(self, lic_inf: list[RegionLicInf]):
        raise NotImplementedError

    @abstractmethod
    def execute_sql(self, query) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_relevant_calendar(self, date: datetime.date) -> Calendar:
        raise NotImplementedError

    @abstractmethod
    def save_region_retail_inf(self, retail_inf:list[RegionRetailInf]) -> None:
        raise NotImplementedError

    @abstractmethod
    def format_and_execute(self, sql_file_path, **kwargs) -> None:
        raise NotImplementedError
