# coding: utf-8
import click

from slurm_client.common import get_config, get_property_data, \
    sort_query_response_items, get_squeue_query_response, \
    get_user_name


def query_user_slq(config, user_name, sort_keys, long=False):
    model_dict = get_squeue_query_response(config)

    max_class_length = 0
    max_owner_length = 0
    for an_item in model_dict['items']:
        job_account = get_property_data(an_item, "squeue.account")
        if user_name not in job_account:
            continue
        job_partition = get_property_data(an_item, "squeue.partition")
        if len(job_partition) > max_class_length:
            max_class_length = len(job_partition)
        if len(job_account) > max_owner_length:
            max_owner_length = len(job_account)

    items = model_dict['items']
    sort_query_response_items(items, sort_keys)

    for an_item in items:
        job_id = get_property_data(an_item, "squeue.job_id")
        job_partition = get_property_data(an_item, "squeue.partition")
        job_account = get_property_data(an_item, "squeue.account")
        if user_name not in job_account:
            continue
        job_command = get_property_data(an_item, "squeue.command")
        job_state = get_property_data(an_item, "squeue.state")
        job_submit_time = get_property_data(an_item, "squeue.submit_time")

        if long:
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
        else:
            click.echo("{job_id} {job_state} {job_partition} {job_account} {job_submit_time} {job_command}".format(
                job_id=click.style(job_id, bold=True),
                job_partition=click.style(
                    ("{job_partition: <" + str(max_class_length) + "}").format(job_partition=job_partition), fg='blue'),
                job_account=click.style(
                    ("{job_account: <" + str(max_owner_length) + "}").format(job_account=job_account), fg='cyan'),
                job_submit_time=click.style(job_submit_time.strftime("%m/%d %H:%M"), fg='blue'),
                job_command=job_command,
                job_state=click.style("{job_state: <2}".format(job_state=job_state), fg='yellow'),
            ))


@click.command('slqn', short_help='query owner jobs')
@click.option('--config-file', help="config file path")
@click.option('-l', '--long', is_flag=True, default=False, help="use long description")
@click.option('-s', '--sort-keys', help="sort keys, split by :, such as status:query_date")
def slqn(config_file, long, sort_keys):
    """
    Query login user's jobs in LoadLeveler.
    """
    config = get_config(config_file)
    user_name = get_user_name()
    if sort_keys:
        sort_keys = tuple(sort_keys.split(":"))

    query_user_slq(config, user_name, sort_keys, long)


@click.command('slqu', short_help='query user jobs')
@click.option('--config-file', help="config file path")
@click.option('-l', '--long', is_flag=True, default=False, help="use long description")
@click.option('-s', '--sort-keys', help="sort keys, split by :, such as status:query_date")
@click.argument('user_name')
def slqu(config_file, user_name, long, sort_keys):
    """
    Query some user's jobs in LoadLeveler.
    """
    config = get_config(config_file)
    if sort_keys:
        sort_keys = tuple(sort_keys.split(":"))

    query_user_slq(config, user_name, sort_keys, long)
