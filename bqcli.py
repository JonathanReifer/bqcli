import json
import logging
from google.cloud import bigquery
from prompt_toolkit import PromptSession
from prompt_toolkit.lexers import PygmentsLexer
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from pygments.lexers.sql import SqlLexer
from bq_helper import BigQueryCompleter, add_default_where_clause, add_limit_clause, validate_query, show_schema, show_table_info
from prompt_toolkit.styles import Style  # Correct import

# Configure Logging
logging.basicConfig(filename='bq_cli.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s:%(message)s')

# Initialize BigQuery client
def initialize_client():
    try:
        client = bigquery.Client()
        return client
    except Exception as e:
        print("Authentication Error: ", e)
        sys.exit(1)

client = initialize_client()

def main():
    print("Welcome to the BigQuery Interactive CLI Tool!")
    print("Type 'help' for instructions or 'exit' to quit.\n")

    # Define a custom style for the prompt
    custom_style = Style.from_dict({
        # User input (default text).
        '':          '#ff0066',
        # Prompt.
        'prompt':    'ansigreen bold',
    })

    session = PromptSession(
        lexer=PygmentsLexer(SqlLexer),
        history=FileHistory('.bq_cli_history'),
        auto_suggest=AutoSuggestFromHistory(),
        multiline=True,
        style=custom_style,
    )

    completer = BigQueryCompleter()

    while True:
        try:
            # Get user input for the query
            user_input = session.prompt('bq> ', completer=completer)

            if not user_input.strip():
                continue

            command = user_input.strip().lower()

            if command in ('exit', 'quit'):
                print("Exiting BigQuery CLI Tool.")
                break

            elif command == 'help':
                print_help()
                continue

            elif command.startswith('export'):
                handle_export(command)
                continue

            elif command.startswith('schema'):
                args = user_input.strip().split()
                if len(args) == 2:
                    table_identifier = args[1]
                    show_schema(client, table_identifier)
                else:
                    print("Usage: schema <project.dataset.table>")
                continue

            elif command.startswith('info') or command.startswith('details'):
                args = user_input.strip().split()
                if len(args) == 2:
                    table_identifier = args[1]
                    show_table_info(client, table_identifier)
                else:
                    print("Usage: info <project.dataset.table>")
                continue

            # Remove the trailing semicolon if present
            if user_input.strip().endswith(';'):
                user_input = user_input.strip().rstrip(';')

            # Add default WHERE clause and LIMIT
            if user_input.strip().lower().startswith('select'):
                modified_query = add_default_where_clause(user_input)
                modified_query = add_limit_clause(modified_query)
            else:
                modified_query = user_input

            # Print the modified query for review
            print("\nModified Query for Review:\n")
            print(modified_query)

            # Validate the query (syntax check)
            valid, message = validate_query(client, modified_query)
            if not valid:
                print(f"\nError in Query: {message}")
                continue

            print("\nQuery looks OK. Press Enter to execute, or type 'cancel' to modify.")
            confirmation = input("Execute Query? (Enter to run / 'cancel' to edit): ").strip().lower()
            if confirmation == 'cancel':
                continue

            # If confirmed, run the query
            print("Running query...")
            query_job = client.query(modified_query)
            results = query_job.result()

            # Print the results
            for row in results:
                print(dict(row))

            # Log the query
            logging.info(f"Executed Query: {modified_query}")

        except KeyboardInterrupt:
            continue
        except Exception as e:
            print(f"Error: {e}")
            logging.error(f"Error executing query: {e}")

def print_help():
    help_text = """
Available Commands:
- Standard SQL queries: SELECT, INSERT, UPDATE, DELETE, etc.
- EXPORT TO <format> '<file_path>': Export the last query result to CSV or JSON.
- SCHEMA <project.dataset.table>: Show the schema of the specified table.
- INFO <project.dataset.table>: Show detailed information about the specified table.
- HELP : Show this help message.
- EXIT or QUIT : Exit the CLI tool.

Features:
- Context-aware auto-completion for projects, datasets, tables, and columns.
- Auto-completion includes backticks for identifiers.
- Query history navigation using Up/Down arrow keys.
- Multi-line statement support.
- Automatic addition of WHERE clause for timestamp columns (last 2 days).
- Automatic addition of LIMIT 100 if not specified.
- Query preview and validation before execution.
- Logging of executed queries.
"""
    print(help_text)

def handle_export(command):
    # Implementation remains the same as before
    pass

if __name__ == '__main__':
    main()

