import sys
import os
import click

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from CliClient import MilvusClientCli

# Global CLI instance to maintain state across command calls
_global_cli_instance = None


def get_milvus_cli_obj():
    """Get the global MilvusClientCli instance."""
    global _global_cli_instance
    if _global_cli_instance is None:
        _global_cli_instance = MilvusClientCli()
    return _global_cli_instance


@click.group(no_args_is_help=False, add_help_option=False, invoke_without_command=True)
@click.pass_context
def cli(ctx):
    """Milvus_CLI based on MilvusClient API"""
    ctx.obj = get_milvus_cli_obj()
