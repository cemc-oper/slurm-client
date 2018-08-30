# coding=utf-8
import click

from .commands import query
from .commands import detail
from .commands import slq
from .commands import category


@click.group()
def cli():
    """
    A command line tool for Slurm.
    """


cli.add_command(query.command)
cli.add_command(detail.command)
cli.add_command(slq.slqn)
cli.add_command(slq.slqu)
cli.add_command(category.command)


if __name__ == "__main__":
    cli()
