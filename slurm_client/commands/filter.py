# coding: utf-8
import sys
import importlib
from pathlib import Path
import click

from slurm_client.common.config import get_config
from slurm_client.common.filter import Filter as SlurmFilter
from slurm_client.common.model.model_util import get_property_data
from slurm_client.common.cli.squeue import sort_query_items, get_query_response


@click.group('filter', short_help='job filter')
def command():
    """
    Filter slurm jobs.
    """
    pass


@command.command('all', short_help='job filter')
@click.option('--config-file', help="config file path")
def all_filters(config_file):
    config = get_config(config_file)

    model_dict = get_query_response(config)

    filters_path = Path(Path(__file__).parent.parent, "filters")
    filters = load_filters([filters_path])
    for filter_name in filters:
        filter_class = filters[filter_name]
        if not filter_class.USE_IN_DEFAULT:
            continue
        filter_object = filter_class()
        filter_items = filter_object.apply(model_dict['items'])

        click.echo('{filter_name}:'.format(filter_name=click.style(filter_object.name, bold=True)))

        max_class_length = 0
        max_owner_length = 0
        for an_item in filter_items:
            job_partition = get_property_data(an_item, "squeue.partition")
            if len(job_partition) > max_class_length:
                max_class_length = len(job_partition)
            job_account = get_property_data(an_item, "squeue.account")
            if len(job_account) > max_owner_length:
                max_owner_length = len(job_account)

        sort_query_items(filter_items)

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


@command.command('list', short_help='job filter')
@click.option('--config-file', help="config file path")
def list_filters(config_file):
    filters_path = Path(Path(__file__).parent.parent, "filters")
    filters = load_filters([filters_path])
    for filter_name in filters:
        print(filter_name)


def load_include_paths(paths):
    for path in paths:
        if not isinstance(path, Path):
            path = Path(path)
        if not path.is_dir():
            continue
        if path not in sys.path:
            sys.path.insert(1, str(path))
        for child in path.iterdir():
            child_path = Path(path, child)
            if child_path.is_dir():
                load_include_paths([child_path])


def load_filters(paths):
    filters = {}
    if paths is None:
        return

    load_include_paths(paths)

    for path in paths:
        if not isinstance(path, Path):
            path = Path(path)

        if not path.exists():
            raise OSError("directory does not exist: {path}".format(path=path))

        if path.name.endswith('tests'):
            continue

        for child in path.iterdir():
            if child.is_dir():
                sub_filters = load_filters([child])
                for item in sub_filters:
                    filters[item] = sub_filters[item]
            elif (child.is_file() and child.name[-3:] == ".py" and
                  child.name[0:4] != "test" and child.name[0] != '.'):
                file_name = child.stem
                module = importlib.import_module(file_name)
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    try:
                        if issubclass(attr, SlurmFilter):
                            # print(attr)
                            if attr != SlurmFilter:
                                filters[attr.__name__] = attr
                    except TypeError:
                        pass
    return filters
