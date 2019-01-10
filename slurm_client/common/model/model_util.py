# coding: utf-8

from nwpc_hpc_model.base.query_item import get_property_data
from nwpc_hpc_model.workload.slurm import SlurmQueryCategoryList
from nwpc_hpc_model.workload import QueryCategory
from slurm_client.common.model import record_parser, value_saver


# TODO: move to nwpc hpc model, and do it in a more proper way.
def get_sort_data(job_item, property_id):
    data = get_property_data(job_item, property_id)
    return data


def generate_sort_key_function(sort_keys):
    def sort_key_function(item):
        key_list = []
        for sort_key in sort_keys:
            key_list.append(get_sort_data(item, sort_key))
        return tuple(key_list)
    return sort_key_function


def sort_items(items, sort_keys=None):
    if sort_keys is None:
        return
    items.sort(key=generate_sort_key_function(sort_keys))


def build_category_list(category_list_config):
    category_list = SlurmQueryCategoryList()
    for an_item in category_list_config:
        category = QueryCategory(
            category_id=an_item['id'],
            display_name=an_item['display_name'],
            label=an_item['label'],
            record_parser_class=getattr(record_parser, an_item['record_parser_class']),
            record_parser_arguments=tuple(an_item['record_parser_arguments']),
            value_saver_class=getattr(value_saver, an_item['value_saver_class']),
            value_saver_arguments=tuple(an_item['value_saver_arguments'])
        )
        category_list.append(category)
    return category_list
