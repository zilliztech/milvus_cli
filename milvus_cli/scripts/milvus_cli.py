from init_cli import cli
from connection_cli import connect, connection
from helper_cli import help, version, clear, show, runCliPrompt


cli.add_command(help)
cli.add_command(version)
cli.add_command(clear)
cli.add_command(show)

cli.add_command(connect)
cli.add_command(connection)


if __name__ == "__main__":
    runCliPrompt()
