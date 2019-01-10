# coding: utf-8
import click

from slurm_client.common.config import get_config
from slurm_client.common.model.model_util import get_property_data
from slurm_client.common.cli.squeue import sort_query_response_items, get_squeue_query_response


@click.command('filter', short_help='job filter')
@click.option('--config-file', help="config file path")
# @click.option('-f', '--filter', 'filter_name', help="filter name")
# @click.option('-d', '--detail', is_flag=True, default=False, help="show detail information")
def command(config_file):
    """
    Filter slurm jobs using build-in filters.
    """
    config = get_config(config_file)

    model_dict = get_squeue_query_response(config)

    from slurm_client.plugins.filters import long_time_job_filter

    filter_module_list = [
        long_time_job_filter
    ]

    def apply_filters(job_items):
        results = []
        for a_filter_module in filter_module_list:
            a_filter_object = a_filter_module.create_filter()
            cur_filter_name = a_filter_object['name']
            a_filter = a_filter_object['filter']
            target_job_items = a_filter.filter(job_items)
            results.append({
                'name': cur_filter_name,
                'target_job_items': target_job_items
            })
        return results

    filter_results = apply_filters(model_dict['items'])

    for a_filter_result in filter_results:
        click.echo('{filter_name}:'.format(filter_name=click.style(a_filter_result['name'], bold=True)))

        filter_items = a_filter_result['target_job_items']

        max_class_length = 0
        max_owner_length = 0
        for an_item in filter_items:
            job_partition = get_property_data(an_item, "squeue.partition")
            if len(job_partition) > max_class_length:
                max_class_length = len(job_partition)
            job_account = get_property_data(an_item, "squeue.account")
            if len(job_account) > max_owner_length:
                max_owner_length = len(job_account)

        sort_query_response_items(filter_items)

        for an_item in filter_items:
            job_id = get_property_data(an_item, "squeue.job_id")
            job_partition = get_property_data(an_item, "squeue.partition")
            job_account = get_property_data(an_item, "squeue.account")
            job_command = get_property_data(an_item, "squeue.command")
            job_state = get_property_data(an_item, "squeue.state")
            job_submit_time = get_property_data(an_item, "squeue.submit_time")
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
        click.echo()
