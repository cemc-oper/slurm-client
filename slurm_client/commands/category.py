# coding: utf-8
import click

from slurm_client.common import get_config


@click.command('category', short_help="show category list defined in config file.")
@click.option('-d', '--detail', is_flag=True, default=False, help="show detail information")
@click.option('--config-file', help="config file path")
def command(detail, config_file):

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
