# coding=utf-8
import click

from slurm_client.commands import query, detail, slq, category, info, acct, filter as slclient_filter


@click.group()
def cli():
    """
    A command line tool for Slurm.
    """


cli.add_command(query.command)
cli.add_command(detail.command)
cli.add_command(slq.slqn)
cli.add_command(slq.slqu)
cli.add_command(info.command)
cli.add_command(category.command)
cli.add_command(acct.command)
cli.add_command(slclient_filter.command)


if __name__ == "__main__":
    cli()
