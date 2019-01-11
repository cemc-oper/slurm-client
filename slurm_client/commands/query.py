# coding: utf-8
import click

from slurm_client.common.config import get_config
from slurm_client.common.model.model_util import get_property_data
from slurm_client.common.cli.squeue import sort_query_items, get_query_response


@click.command('query', short_help='squeue short format')
@click.option('--config-file', help="config file path")
@click.option('-u', '--user-list', multiple=True, help="user list")
@click.option('-p', '--partition-list', multiple=True, help="partition list")
@click.option('-s', '--sort-keys', help="sort keys, split by :, such as status:query_date")
@click.option('-p', '--params', default="", help="llq params")
@click.option('-c', '--command-pattern', help="command pattern")
def command(config_file, user_list, partition_list, sort_keys, params, command_pattern):
    """
    Query jobs in Slurm and show in a simple format.
    """
    config = get_config(config_file)

    if params is None:
        params = ''
    if user_list:
        params += ' -u {user_list}'.format(user_list=" ".join(user_list))
    if partition_list:
        params += ' -p {partition_list}'.format(partition_list=" ".join(partition_list))
    if sort_keys:
        sort_keys = tuple(sort_keys.split(":"))

    model_dict = get_query_response(config, params)

    max_class_length = 0
    max_owner_length = 0
    for an_item in model_dict['items']:
        job_partition = get_property_data(an_item, "squeue.partition")
        if len(job_partition) > max_class_length:
            max_class_length = len(job_partition)
        job_account = get_property_data(an_item, "squeue.account")
        if len(job_account) > max_owner_length:
            max_owner_length = len(job_account)

    items = model_dict['items']

    if command_pattern:
        from slurm_client.plugins.filters import command_filter
        a_filter_object = command_filter.create_filter(command_pattern)
        a_filter = a_filter_object['filter']
        items = a_filter.filter(items)

    sort_query_items(items, sort_keys)

    for an_item in items:
        job_id = get_property_data(an_item, "squeue.job_id")
        job_partition = get_property_data(an_item, "squeue.partition")
        job_account = get_property_data(an_item, "squeue.account")
        job_command = get_property_data(an_item, "squeue.command")
        job_state = get_property_data(an_item, "squeue.state")
        job_submit_time = get_property_data(an_item, "squeue.submit_time")
        click.echo("{job_id} {job_state} {job_partition} {job_account} {job_submit_time} {job_command}".format(
            job_id=click.style(job_id, bold=True),
            job_partition=click.style(("{job_partition: <" + str(max_class_length) + "}").format(job_partition=job_partition), fg='blue'),
            job_account=click.style(("{job_account: <" + str(max_owner_length) + "}").format(job_account=job_account), fg='cyan'),
            job_submit_time=click.style(job_submit_time.strftime("%m/%d %H:%M"), fg='blue'),
            job_command=job_command,
            job_state=click.style("{job_state: <2}".format(job_state=job_state), fg='yellow'),
        ))
