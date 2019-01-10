# coding: utf-8
from .model_util import build_category_list, sort_items
from nwpc_hpc_model.workload.slurm import SlurmQueryModel
from nwpc_hpc_model.base.query_item import QueryItem


def build_query_model(job_dict, category_list):
    model = SlurmQueryModel()

    for _, job in job_dict.items():
        item = QueryItem.build_from_category_list(job, category_list)
        model.items.append(item)

    return model


def get_query_model(config, params=""):
    """
    get response of job query using pyslurm.

    :param config: config dict
        {
            category_list: a list of categories
                [
                    {

                        id: "squeue.job_id"
                        display_name: "Job Id"
                        label: "JOBID"
                        record_parser_class: "DictRecordParser"
                        record_parser_arguments:[
                          "job_id"
                        ]
                        value_saver_class: "StringSaver"
                        value_saver_arguments: []
                    }
                ]
    :param params:
    :return: model, see nwpc_hpc_model.workflow.query_model.QueryModel

    """
    import pyslurm
    jobs = pyslurm.job()
    job_dict = jobs.get()

    category_list = build_category_list(config['squeue_pyslurm']['category_list'])

    model = build_query_model(job_dict, category_list)
    return model


def get_query_response(config, params=''):
    """
    get response of job query.

    :param config: config dict
    :param params:
    :return: model dict, see nwpc_hpc_model.workflow.query_model.QueryModel.to_dict()
        {
            items: job info items, see nwpc_hpc_model.workflow.query_item.QueryItem.to_dict()
        }

    """
    return get_query_model(config, params).to_dict()
