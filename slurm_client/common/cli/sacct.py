# coding: utf-8
import subprocess

from slurm_client.common.model.model_util import build_category_list, sort_items
from nwpc_hpc_model.workload.slurm import SlurmQueryModel


def sort_query_response_items(items, sort_keys=None):
    if sort_keys is None:
        sort_keys = ('job_id', )
    sort_keys = ['sacct.'+i for i in sort_keys]
    sort_items(items, sort_keys)


def run_command(config, params="") -> str:
    """
    :param config:
    :param params:
    :return: command result string
    """
    command = config['sacct']['command']
    pipe = subprocess.Popen([command + " " + params], stdout=subprocess.PIPE, shell=True)
    output = pipe.communicate()[0]
    output_string = output.decode()
    return output_string


def get_query_model(config, params=""):
    """
    get response of sacct query.

    :param config: config dict
    :param params:
    :return: model, see nwpc_hpc_model.workflow.query_model.QueryModel

    """
    output_lines = run_command(config, params=params).split("\n")
    category_list = build_category_list(config['sacct']['category_list'])

    model = SlurmQueryModel.build_from_table_category_list(output_lines, category_list, sep='|')
    return model


def get_query_response(config, params=""):
    """
    get response of sinfo query.

    :param config: config dict
    :param params:
    :return: model dict, see nwpc_hpc_model.workflow.query_model.QueryModel.to_dict()

    """
    return get_query_model(config, params).to_dict()
