# coding=utf-8
import datetime
from nwpc_hpc_model.workload.filter_condition import (
    PropertyFilterCondition,
    create_less_value_checker,
    create_in_value_checker,
    get_property_data)
from nwpc_hpc_model.workload.filter import Filter

from slurm_client.common.filter import Filter as SlurmFilter


class LongTimeJobFilter(SlurmFilter):
    """
    Filter job command
    """
    USE_IN_DEFAULT = True

    def __init__(self):
        SlurmFilter.__init__(self, "long_time_job_filter")

    def apply(self, job_items):
        query_date_condition = PropertyFilterCondition(
            "squeue.submit_time",
            data_checker=create_less_value_checker(datetime.datetime.utcnow() - datetime.timedelta(hours=5)),
            data_parser=get_property_data
        )
        owner_condition = PropertyFilterCondition(
            "squeue.account",
            data_checker=create_in_value_checker(["nwp", "nwp_qu", "nwp_pd", "nwp_sp"])
        )
        a_filter = Filter()
        a_filter.conditions.append(owner_condition)
        a_filter.conditions.append(query_date_condition)

        target_job_items = a_filter.filter(job_items)
        return target_job_items
