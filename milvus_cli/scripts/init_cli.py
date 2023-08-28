import sys
import os
import click

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from Cli import MilvusCli


@click.group(no_args_is_help=False, add_help_option=False, invoke_without_command=True)
@click.pass_context
def cli(ctx):
    """Milvus_CLI"""
    ctx.obj = MilvusCli()
