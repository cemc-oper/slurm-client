# coding=utf-8
from nwpc_hpc_model.workload.filter_condition import PropertyFilterCondition, get_property_data
from nwpc_hpc_model.workload.filter import Filter
from nwpc_hpc_model.workload.filter_condition import create_value_in_checker

from slurm_client.common.filter import Filter as SlurmFilter


class JobCommandFilter(SlurmFilter):
    """
    Filter job command
    """
    def __init__(self, pattern):
        SlurmFilter.__init__(self, "command_filter")
        self.pattern = pattern

    def apply(self, job_items):
        job_script_condition = PropertyFilterCondition(
            "squeue.command",
            data_checker=create_value_in_checker(self.pattern),
            data_parser=get_property_data
        )
        a_filter = Filter()
        a_filter.conditions.append(job_script_condition)

        target_job_items = a_filter.filter(job_items)
        return target_job_items
