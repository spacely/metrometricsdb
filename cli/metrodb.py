import click
from pubsub import pub




def show_welcome_screen():
    click.clear()
    click.secho("Welcome to MetroDB CLI Tool", fg='blue', bold=True)
    click.echo("MetroDB is a command-line tool for managing GTFS data and queries.\n")
    click.echo("Available Commands:")
    click.echo("  1. load - Load GTFS files from a specified folder.")
    click.echo("  2. query - Execute a SQL query on the loaded GTFS data.")
    click.echo("  3. exit - Exit the application.\n")
    click.echo("Type 'help' for more information on specific commands.")

@click.group(invoke_without_command=True)
@click.pass_context
def cli(ctx):
    """Entry point for MetroDB CLI."""
    if ctx.invoked_subcommand is None:
        show_welcome_screen()
        while True:
            cmd = click.prompt("\nPlease enter a command", type=str)
            if cmd == "exit":
                click.secho("Exiting program.", fg='red')
                break
            elif cmd.startswith('load '):
                folder_path = cmd.split(' ', 1)[1]  # Extract path from command
                ctx.invoke(load, folder_path=folder_path)
            elif cmd.startswith('query '):
                query_str = cmd.split(' ', 1)[1]  # Extract query from command
                ctx.invoke(query, query=query_str)
            elif cmd == "help":
                show_welcome_screen()
            else:
                click.secho("Unknown command. Type 'help' for a list of commands.", fg='red')

@cli.command()
@click.argument('folder_path', type=click.Path(exists=True))
def load(folder_path):
    """Load GTFS files from a specified folder."""
    click.secho(f"Loading GTFS data from {folder_path}", fg='green')
    # Your data loading logic here
    pub.sendMessage('load_gtfs', folder_path=folder_path)
    print("Data loading request has been sent to IOManager.")
    

@cli.command()
@click.argument('query', type=str)
def query(query):
    """Execute a SQL query on the loaded GTFS data."""
    click.secho(f"Executing query: {query}", fg='green')
    # Your query execution logic here

if __name__ == '__main__':
    
    cli()
