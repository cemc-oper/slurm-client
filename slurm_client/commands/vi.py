# coding: utf-8
import click

from slurm_client.common.config import get_config
from slurm_client.common.model.model_util import get_property_data


def edit_file(config, file_type, job_id):
    key_map = {
        'job': 'squeue.command',
        'out': 'squeue.std_out',
        'err': 'squeue.std_err'
    }

    from slurm_client import HAS_PYSLURM

    if not HAS_PYSLURM:
        if file_type != 'job':
            click.echo('Please install pyslurm to enable this feature.')
            return
        params = job_id
        from slurm_client.common.cli.squeue import get_squeue_query_response
        model_dict = get_squeue_query_response(config, params)
    else:
        from slurm_client.common.api.job import get_query_response
        model_dict = get_query_response(config, jobs=[job_id])

    job_count = len(model_dict['items'])

    if job_count == 0:
        click.echo('There is no job.')
    elif job_count > 1:
        click.echo('There are more than one job: {job_count}'.format(job_count=job_count))
    else:
        an_item = model_dict['items'][0]
        key = key_map[file_type]
        file_path = get_property_data(an_item, key)
        click.edit(filename=file_path)


@click.command('vijob', short_help='edit job script')
@click.option('--config-file', help="config file path")
@click.option('-f', '--file-type', type=click.Choice(['job', 'out', 'err']), default='job', help="file type")
@click.argument('job_id')
def edit_job(config_file, file_type, job_id):
    """
    Edit job command script.
    """

    config = get_config(config_file)
    edit_file(config, file_type, job_id)


@click.command('viout', short_help='edit job stdout file')
@click.option('--config-file', help="config file path")
@click.argument('job_id')
def edit_std_out(config_file, job_id):
    """
    Edit job stdout file.
    """

    config = get_config(config_file)
    edit_file(config, 'out', job_id)


@click.command('vierr', short_help='edit job stderr file')
@click.option('--config-file', help="config file path")
@click.argument('job_id')
def edit_std_err(config_file, job_id):
    """
    Edit job stdout file.
    """

    config = get_config(config_file)
    edit_file(config, 'err', job_id)
