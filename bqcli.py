import json
import logging
import sys
import argparse
from google.cloud import bigquery
from prompt_toolkit import PromptSession
from prompt_toolkit.lexers import PygmentsLexer
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.styles import Style
from prompt_toolkit.key_binding import KeyBindings
from pygments.lexers.sql import SqlLexer
from bq_helper import (
    BigQueryCompleter,
    add_default_where_clause,
    add_limit_clause,
    validate_query,
    show_schema,
    show_table_info,
)

# Argument Parsing for --dev flag
def parse_arguments():
    parser = argparse.ArgumentParser(description='BigQuery Interactive CLI Tool')
    parser.add_argument('--dev', action='store_true', help='Enable developer mode with debug output')
    args = parser.parse_args()
    return args

# Initialize BigQuery client
def initialize_client():
    try:
        client = bigquery.Client()
        return client
    except Exception as e:
        print("Authentication Error: ", e)
        sys.exit(1)

def main():
    args = parse_arguments()
    dev_mode = args.dev

    # Configure Logging
    log_level = logging.DEBUG if dev_mode else logging.INFO
    logging.basicConfig(
        filename='bq_cli_debug.log' if dev_mode else 'bq_cli.log',
        level=log_level,
        format='%(asctime)s %(levelname)s:%(message)s',
    )

    client = initialize_client()

    print("Welcome to the BigQuery Interactive CLI Tool!")
    print("Type 'help' for instructions or 'exit' to quit.\n")

    # Define a custom style for the prompt
    custom_style = Style.from_dict(
        {
            # User input (default text).
            '': '#ff0066',
            # Prompt.
            'prompt': 'ansigreen bold',
        }
    )

    completer = BigQueryCompleter(client, dev_mode=dev_mode)  # Pass the client and dev_mode to the completer

    # Define key bindings
    kb = KeyBindings()

    @kb.add('enter')
    def _(event):
        buffer = event.app.current_buffer
        text = buffer.document.text.strip()
        single_line_commands = ['exit', 'quit', 'help', 'schema', 'info', 'details']
        if text.lower() in single_line_commands or any(text.lower().startswith(cmd + ' ') for cmd in single_line_commands):
            buffer.validate_and_handle()
        elif text.endswith(';'):
            buffer.validate_and_handle()
        elif not text:
            buffer.validate_and_handle()
        else:
            buffer.insert_text('\n')

    session = PromptSession(
        lexer=PygmentsLexer(SqlLexer),
        history=FileHistory('.bq_cli_history'),
        auto_suggest=AutoSuggestFromHistory(),
        style=custom_style,
        completer=completer,
        multiline=True,
        key_bindings=kb,
    )

    while True:
        try:
            # Get user input for the query
            user_input = session.prompt('bq> ')

            if not user_input.strip():
                continue

            command = user_input.strip().lower()

            if command in ('exit', 'quit'):
                print("Exiting BigQuery CLI Tool.")
                break

            elif command == 'help':
                print_help()
                continue

            elif command.lower().startswith('export'):
                handle_export(command)
                continue

            elif command.lower().startswith('schema'):
                args = user_input.strip().split(maxsplit=1)
                if len(args) == 2:
                    table_identifier = strip_backticks(args[1].strip())
                    show_schema(client, table_identifier)
                else:
                    print("Usage: schema <project.dataset.table>")
                continue

            elif command.lower().startswith('info') or command.lower().startswith('details'):
                args = user_input.strip().split(maxsplit=1)
                if len(args) == 2:
                    table_identifier = strip_backticks(args[1].strip())
                    show_table_info(client, table_identifier)
                else:
                    print("Usage: info <project.dataset.table>")
                continue

            # Remove the trailing semicolon if present
            if user_input.strip().endswith(';'):
                user_input = user_input.strip().rstrip(';')

            # Add default WHERE clause and LIMIT
            if user_input.strip().lower().startswith('select'):
                modified_query = add_default_where_clause(user_input, client)
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
            print("\nExiting BigQuery CLI Tool.")
            sys.exit(0)  # Exit the script immediately
        except Exception as e:
            print(f"Error: {e}")
            logging.error(f"Error executing query: {e}")

def strip_backticks(identifier):
    return identifier.replace('`', '')

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
- Auto-completion for 'schema' and 'info' commands.
- Query history navigation using Up/Down arrow keys.
- Multi-line statement support.
- Automatic addition of WHERE clause for timestamp columns (last 2 days).
- Automatic addition of LIMIT 100 if not specified.
- Query preview and validation before execution.
- Immediate exit on Ctrl+C.
- Logging of executed queries.
- Developer mode for debugging (--dev).
"""
    print(help_text)

def handle_export(command):
    # Implementation remains the same as before
    print("Export functionality is not yet implemented.")
    pass

if __name__ == '__main__':
    main()

