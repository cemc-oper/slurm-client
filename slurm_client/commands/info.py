# coding: utf-8

import click

from slurm_client.common.config import get_config
from slurm_client.common.model_util import get_property_data
from slurm_client.common.sinfo import sort_query_response_items, get_query_response


@click.command('info', short_help='query partition info')
@click.option('--config-file', help="config file path")
@click.option('-s', '--sort-keys', help="sort keys, split by :, such as status:query_date")
@click.option('-p', '--params', default="", help="sinfo params")
def command(config_file, sort_keys, params):
    """
    Show partition info.
    """
    config = get_config(config_file)

    if params is None:
        params = ''
    if sort_keys:
        sort_keys = tuple(sort_keys.split(":"))
    print_header = True

    model_dict = get_query_response(config, params)

    items = model_dict['items']
    sort_query_response_items(items, sort_keys)

    max_partition_length = 10
    max_nodes_status_length = 20
    max_cpus_status_length = 30
    for an_item in items:
        partition_name = get_property_data(an_item, "sinfo.partition")
        if len(partition_name) > max_partition_length:
            max_partition_length = len(partition_name)
        nodes_status = get_property_data(an_item, "sinfo.nodes")
        if len(nodes_status) > max_nodes_status_length:
            max_nodes_status_length = len(nodes_status)
        cpus_status = get_property_data(an_item, "sinfo.cpus")
        if len(cpus_status) > max_cpus_status_length:
            max_cpus_status_length = len(cpus_status)

    if print_header:
        click.echo("{partition_name} {avail_status} {nodes_status} {cpus_status}".format(
            partition_name=click.style(("{partition_name: <" + str(max_partition_length) + "}")
                                       .format(partition_name='Partition'), bold=True),
            avail_status=click.style("{avail_status: <5}".format(avail_status='Avail'), bold=True),
            nodes_status=click.style(("{nodes_status: >" + str(max_nodes_status_length) + "}")
                                     .format(nodes_status='Nodes(A/I/O/T)'), bold=True),
            cpus_status=click.style(("{cpus_status: >" + str(max_cpus_status_length) + "}")
                                    .format(cpus_status='CPUs(A/I/O/T)'), bold=True)
        ))

    for an_item in items:
        partition_name = get_property_data(an_item, "sinfo.partition")
        avail_status = get_property_data(an_item, "sinfo.avail")
        nodes_status = get_property_data(an_item, "sinfo.nodes")
        cpus_status = get_property_data(an_item, "sinfo.cpus")
        click.echo("{partition_name} {avail_status} {nodes_status} {cpus_status}".format(
            partition_name=click.style(("{partition_name: <" + str(max_partition_length) + "}")
                                       .format(partition_name=partition_name), bold=True),
            avail_status=click.style("{avail_status: <5}".format(avail_status=avail_status), fg='blue'),
            nodes_status=click.style(("{nodes_status: >" + str(max_nodes_status_length) + "}")
                                     .format(nodes_status=nodes_status), fg='cyan'),
            cpus_status=click.style(("{cpus_status: >" + str(max_cpus_status_length) + "}")
                                    .format(cpus_status=cpus_status), fg='magenta')
        ))
