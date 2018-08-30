# coding: utf-8
import click

from slurm_client.common.config import get_config
from slurm_client.common.model_util import get_property_data
from slurm_client.common.squeue import sort_query_response_items, get_squeue_query_response


@click.command('detail', short_help='squeue detail format')
@click.option('--config-file', help="config file path")
@click.option('-u', '--user-list', multiple=True, help="user list")
@click.option('-p', '--partition-list', multiple=True, help="partition list")
@click.option('-s', '--sort-keys', help="sort keys, split by :, such as status:query_date")
@click.option('-p', '--params', default="", help="llq params")
def command(config_file, user_list, partition_list, sort_keys, params):
    """
    Query jobs in Slurm and show in a detailed format.
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

    model_dict = get_squeue_query_response(config, params)
    items = model_dict['items']
    sort_query_response_items(items, sort_keys)

    for an_item in items:
        job_id = get_property_data(an_item, "squeue.job_id")
        job_partition = get_property_data(an_item, "squeue.partition")
        job_account = get_property_data(an_item, "squeue.account")
        job_command = get_property_data(an_item, "squeue.command")
        job_state = get_property_data(an_item, "squeue.state")
        job_submit_time = get_property_data(an_item, "squeue.submit_time")
        click.echo("""{job_id} {job_state} {job_partition} {job_account} {job_submit_time}
  Command: {job_command}
""".format(
            job_id=click.style(job_id, bold=True),
            job_partition=click.style(job_partition, fg='blue'),
            job_account=click.style(job_account, fg='cyan'),
            job_submit_time=click.style(job_submit_time.strftime("%m/%d %H:%M"), fg='blue'),
            job_command=job_command,
            job_state=click.style(job_state, fg='yellow')
        ))
