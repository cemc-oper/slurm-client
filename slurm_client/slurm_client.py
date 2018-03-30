# coding=utf-8
import os
import subprocess
import yaml
import click

from nwpc_hpc_model.workload.slurm import SlurmQueryCategoryList, SlurmQueryModel
from nwpc_hpc_model.workload import QueryCategory, value_saver, record_parser
from nwpc_hpc_model.loadleveler.filter_condition import get_property_data


config_file_name = "slurm.config.yml"
default_config_file_path = os.path.join(os.path.dirname(__file__), "conf", config_file_name)


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
    category_list = build_category_list(config['category_list'])

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


@click.group()
def cli():
    """
    A command line tool for Slurm.
    """


@cli.command('query', short_help='squeue short format')
@click.option('--config-file', help="config file path")
@click.option('-u', '--user-list', multiple=True, help="user list")
@click.option('-p', '--partition-list', multiple=True, help="partition list")
@click.option('-s', '--sort-keys', help="sort keys, split by :, such as status:query_date")
@click.option('-p', '--params', default="", help="llq params")
@click.option('-c', '--command-pattern', help="command pattern")
def query(config_file, user_list, partition_list, sort_keys, params, command_pattern):
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

    model_dict = get_squeue_query_response(config, params)

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

    sort_query_response_items(items, sort_keys)

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


@cli.command('detail', short_help='squeue detail format')
@click.option('--config-file', help="config file path")
@click.option('-u', '--user-list', multiple=True, help="user list")
@click.option('-p', '--partition-list', multiple=True, help="partition list")
@click.option('-s', '--sort-keys', help="sort keys, split by :, such as status:query_date")
@click.option('-p', '--params', default="", help="llq params")
def show_detail(config_file, user_list, partition_list, sort_keys, params):
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


@cli.command('slqn', short_help='query owner jobs')
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


@cli.command('slqu', short_help='query user jobs')
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


@cli.command('category', short_help="show category list defined in config file.")
@click.option('-d', '--detail', is_flag=True, default=False, help="show detail information")
@click.option('--config-file', help="config file path")
def show_category(detail, config_file):

    """
    show category list defined in config file.
    """
    config = get_config(config_file)
    category_list = config['category_list']
    for a_category in category_list:
        click.echo(click.style(a_category['id'], bold=True))
        if detail:
            click.echo("  display name: {display_name}".format(display_name=a_category['display_name']))
            click.echo("  label: {label}".format(label=a_category['label']))
            click.echo("  record parser: {record_parser_class}".format(
                record_parser_class=a_category['record_parser_class']))
            click.echo("  record parser arguments:")
            for an_arg in a_category['record_parser_arguments']:
                click.echo("    {arg}".format(arg=an_arg))
            click.echo("  value saver: {value_saver_class}".format(
                value_saver_class=a_category['value_saver_class']))
            click.echo("  value saver arguments:")
            for an_arg in a_category['value_saver_arguments']:
                click.echo("    {arg}".format(arg=an_arg))
            click.echo()


if __name__ == "__main__":
    cli()
