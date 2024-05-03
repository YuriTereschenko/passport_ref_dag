import logging as lg
from pasport_test_ref.core.domain.interfaces import ISourceRepository, ITargetRepository


def load_lic_by_periods(date_end, date_begin, period_id, source: ISourceRepository, target: ITargetRepository):
    lic_inf_list = source.load_region_lic_inf(date_end, date_begin, period_id)
    target.save_lic_temp(lic_inf_list)


def load_retail_by_perod(date_end, date_begin, source: ISourceRepository, target: ITargetRepository):
    retail_inf_list = source.load_region_retail_inf(date_begin, date_end)
    target.save_region_retail_inf(retail_inf_list)

