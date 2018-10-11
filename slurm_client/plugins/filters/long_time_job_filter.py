# coding=utf-8
import datetime
from nwpc_hpc_model.workload.filter_condition import \
    PropertyFilterCondition, \
    create_less_value_checker, \
    get_property_data
from nwpc_hpc_model.workload.filter import Filter


filter_info = {
    'name': 'long_time_job_filter',
}


def create_filter():
    query_date_condition = PropertyFilterCondition(
        "squeue.submit_time",
        data_checker=create_less_value_checker(datetime.datetime.utcnow()-datetime.timedelta(hours=5)),
        data_parser=get_property_data
    )
    a_filter = Filter()
    a_filter.conditions.append(query_date_condition)
    return {
        'name': filter_info['name'],
        'filter': a_filter
    }
