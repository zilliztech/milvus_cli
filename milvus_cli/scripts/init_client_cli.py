import sys
import os
import click

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from CliClient import MilvusClientCli

# Global CLI instance to maintain state across command calls
_global_cli_instance = None

@click.group(no_args_is_help=False, add_help_option=False, invoke_without_command=True)
@click.pass_context
def cli(ctx):
    """Milvus_CLI based on MilvusClient API"""
    global _global_cli_instance
    
    # Use existing instance if available, otherwise create new one
    if _global_cli_instance is None:
        _global_cli_instance = MilvusClientCli()
    
    ctx.obj = _global_cli_instance
