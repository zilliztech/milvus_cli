from init_cli import cli
from connection_cli import connect, connection
from helper_cli import help, version, clear, show, runCliPrompt
from database_cli import create_database, list_databases, drop_database, use_database
from collection_cli import create_collection

cli.add_command(help)
cli.add_command(version)
cli.add_command(clear)
cli.add_command(show)

cli.add_command(connect)
cli.add_command(connection)

cli.add_command(create_database)
cli.add_command(list_databases)
cli.add_command(drop_database)
cli.add_command(use_database)

cli.add_command(create_collection)


if __name__ == "__main__":
    runCliPrompt()
