# coding: utf-8
import click

from slurm_client.common.config import get_config
from slurm_client.common.model_util import get_property_data
from slurm_client.common.sacct import sort_query_response_items, get_query_response


@click.command('acct', short_help='query partition info')
@click.option('--config-file', help="config file path")
@click.option('-s', '--sort-keys', help="sort keys, split by :, such as status:query_date")
@click.option('-p', '--params', default="", help="sinfo params")
def command(config_file, sort_keys, params):
    """
    Show accounting data for all jobs.
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

    column_config = [
        {
            'title': 'Job ID',
            'id': 'sacct.job_id',
            'col_length': 10,
            'title_align': '>',
            'value_align': '<'
        },
        {
            'title': 'Partition',
            'id': 'sacct.partition',
            'col_length': 10,
            'title_align': '>',
            'value_align': '<'
        },
        {
            'title': 'Account',
            'id': 'sacct.account',
            'col_length': 6,
            'title_align': '<',
            'value_align': '<'
        },
        {
            'title': 'Nodes',
            'id': 'sacct.alloc_nodes',
            'col_length': 8,
            'title_align': '<',
            'value_align': '<'
        },
        {
            'title': 'CPUs',
            'id': 'sacct.alloc_cpus',
            'col_length': 8,
            'title_align': '<',
            'value_align': '<'
        },
        {
            'title': 'State',
            'id': 'sacct.state',
            'col_length': 8,
            'title_align': '<',
            'value_align': '<'
        },
        {
            'title': 'Exit Code',
            'id': 'sacct.exit_code',
            'col_length': 8,
            'title_align': '<',
            'value_align': '<'
        },
    ]

    table_data = []
    for an_item in items:
        row_data = []
        for a_column in column_config:
            value = get_property_data(an_item, a_column['id'])
            col_length = a_column['col_length']
            if len(str(value)) > col_length:
                a_column['col_length'] = len(str(value))
            row_data.append(value)
        table_data.append(row_data)

    if print_header:
        title_token = []
        for a_column in column_config:
            col_title = ("{title: " + a_column['title_align'] + str(a_column['col_length']) + "}").format(
                title=a_column['title']
            )
            title_token.append(col_title)

        click.echo(' '.join(title_token))

    for a_row in table_data:
        row_token = []
        index = 0
        for a_column in column_config:
            col_title = ("{value: " + a_column['value_align'] + str(a_column['col_length']) + "}").format(
                value=a_row[index]
            )
            row_token.append(col_title)
            index += 1

        click.echo(' '.join(row_token))
