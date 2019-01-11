# coding: utf-8
import subprocess

from slurm_client.common.model.model_util import build_category_list, sort_items
from nwpc_hpc_model.workload.slurm import SlurmQueryModel


def sort_query_items(items, sort_keys=None):
    if sort_keys is None:
        sort_keys = ('state', 'submit_time')
    sort_keys = ['squeue.'+i for i in sort_keys]
    sort_items(items, sort_keys)


def run_command(command="squeue -o %all", params="") -> str:
    """
    :param command:
    :param params:
    :return: command result string
    """
    pipe = subprocess.Popen([command + " " + params], stdout=subprocess.PIPE, shell=True)
    output = pipe.communicate()[0]
    output_string = output.decode()
    return output_string


def get_query_model(config, params=""):
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
    output_lines = run_command(params=params).split("\n")
    category_list = build_category_list(config['squeue']['category_list'])

    model = SlurmQueryModel.build_from_table_category_list(output_lines, category_list, '|')
    return model


def get_query_response(config, params=""):
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
    return get_query_model(config, params).to_dict()
