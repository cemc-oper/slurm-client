# coding: utf-8
import os
import yaml
import subprocess

from nwpc_hpc_model.workload.slurm import SlurmQueryCategoryList, SlurmQueryModel
from nwpc_hpc_model.workload import QueryCategory, value_saver, record_parser
from nwpc_hpc_model.base.query_item import get_property_data

config_file_name = "slurm.config.yml"
default_config_file_path = os.path.join(os.path.dirname(__file__), "../conf", config_file_name)


def get_config(config_file_path):
    """
    :param config_file_path: path of config file, which should be a YAML file.

    :return: config dict loading from config file.
    """
    config = None
    if not config_file_path:
        config_file_path = default_config_file_path
    with open(config_file_path, 'r') as f:
        config = yaml.load(f)
    return config


def get_user_name() -> str:
    if 'USER' in os.environ:
        return os.environ["USER"]
    else:
        cmquota_command = "whoami"
        pipe = subprocess.Popen([cmquota_command], stdout=subprocess.PIPE, shell=True)
        return pipe.communicate()[0].decode().rstrip()


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


# TODO: move to nwpc hpc model, and do it in a more proper way.
def get_sort_data(job_item, property_id):
    data = get_property_data(job_item, property_id)
    return data


def generate_sort_key_function(sort_keys):
    def sort_key_function(item):
        key_list = []
        for sort_key in sort_keys:
            key_list.append(get_sort_data(item, "squeue."+sort_key))
        return tuple(key_list)
    return sort_key_function


def sort_query_response_items(items, sort_keys=None):
    if sort_keys is None:
        sort_keys = ('state', 'submit_time')
    items.sort(key=generate_sort_key_function(sort_keys))


def run_squeue_command(command="squeue -o %all", params="") -> str:
    """
    :param command:
    :param params:
    :return: command result string
    """
    pipe = subprocess.Popen([command + " " + params], stdout=subprocess.PIPE, shell=True)
    output = pipe.communicate()[0]
    output_string = output.decode()
    return output_string


def get_squeue_query_model(config, params=""):
    """
    get response of squeue query.

    :param config: config dict
        {
            category_list: a list of categories
                [
                    {
                        id: "llq.id",
                        display_name: "Id",
                        label: "Job Step Id",
                        record_parser_class: "DetailLabelParser",
                        record_parser_arguments: ["Job Step Id"],
                        value_saver_class: "StringSaver",
                        value_saver_arguments: []
                    }
                ]
    :param params:
    :return: model, see nwpc_hpc_model.workflow.query_model.QueryModel

    """
    output_lines = run_squeue_command(params=params).split("\n")
    category_list = build_category_list(config['squeue']['category_list'])

    model = SlurmQueryModel.build_from_table_category_list(output_lines, category_list, '|')
    return model


def get_squeue_query_response(config, params=""):
    """
    get response of llq detail query.

    :param config: config dict
        {
            category_list: a list of categories
                [
                    {
                        id: "squeue.job_id",
                        display_name: "Job ID",
                        label: "JOBID",
                        record_parser_class: "TokenRecordParser",
                        record_parser_arguments: [-1, "|"],
                        value_saver_class: "StringSaver",
                        value_saver_arguments: []
                    }
                ]
    :param params:
    :return: model dict, see nwpc_hpc_model.workflow.query_model.QueryModel.to_dict()
        {
            items: job info items, see nwpc_hpc_model.workflow.query_item.QueryItem.to_dict()
        }

    """
    return get_squeue_query_model(config, params).to_dict()
