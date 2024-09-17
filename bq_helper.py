import re
from google.cloud import bigquery
from prompt_toolkit.completion import Completer, Completion
import logging
from datetime import datetime, timedelta

# Completer class for BigQuery
class BigQueryCompleter(Completer):
    def __init__(self, client, dev_mode=False):
        self.client = client
        self.dev_mode = dev_mode
        self.projects = get_projects(client)
        self.keywords = [
            'SELECT',
            'FROM',
            'WHERE',
            'LIMIT',
            'ORDER BY',
            'GROUP BY',
            'JOIN',
            'ON',
            'INSERT',
            'UPDATE',
            'DELETE',
            'CREATE',
            'DROP',
            'ALTER',
            'HELP',
            'EXPORT',
            'SCHEMA',
            'INFO',
            'EXIT',
            'QUIT',
        ]
        self.functions = ['COUNT', 'SUM', 'MIN', 'MAX', 'AVG']
        self.all_completions = self.keywords + self.functions
        self.table_aliases = {}  # Cache of table aliases

    def get_completions(self, document, complete_event):
        text_before_cursor = document.text_before_cursor
        word_before_cursor = document.get_word_before_cursor(WORD=True)
        last_token = self.get_last_token(text_before_cursor)

        if self.dev_mode:
            logging.debug(f"get_completions called with word: '{word_before_cursor}'")
            logging.debug(f"Last token: '{last_token}'")
            logging.debug(f"Text before cursor: '{text_before_cursor}'")

        if last_token.upper() in ('FROM', 'JOIN', 'SCHEMA', 'INFO', 'DETAILS'):
            for completion in self.get_table_completions(word_before_cursor):
                yield completion
        elif self.is_in_column_context(text_before_cursor):
            if self.dev_mode:
                logging.debug("Context: Column")
            for completion in self.get_column_completions(word_before_cursor, text_before_cursor):
                yield completion
        elif '.' in word_before_cursor:
            for completion in self.get_partial_identifier_completions(word_before_cursor):
                yield completion
        else:
            for kw in self.all_completions:
                if kw.upper().startswith(word_before_cursor.upper()):
                    yield Completion(kw, start_position=-len(word_before_cursor))

    def get_last_token(self, text):
        tokens = re.findall(r'\b\w+\b', text)
        return tokens[-1] if tokens else ''

    def get_table_completions(self, word):
        parts = word.strip('`').split('.')
        if self.dev_mode:
            logging.debug(f"get_table_completions called with parts: {parts}")
        if len(parts) == 1:
            for project in self.projects:
                if project.startswith(parts[0]):
                    yield Completion(f"`{project}`", start_position=-len(parts[0]))
        elif len(parts) == 2:
            project_id = parts[0].strip('`')
            dataset_prefix = parts[1]
            datasets = get_datasets(self.client, project_id)
            for dataset in datasets:
                if dataset.startswith(dataset_prefix):
                    yield Completion(f"`{project_id}`.`{dataset}`", start_position=-len(word))
        elif len(parts) == 3:
            project_id = parts[0].strip('`')
            dataset_id = parts[1].strip('`')
            table_prefix = parts[2]
            tables = get_tables(self.client, project_id, dataset_id)
            for table in tables:
                if table.startswith(table_prefix):
                    yield Completion(f"`{project_id}`.`{dataset_id}`.`{table}`", start_position=-len(word))

    def get_partial_identifier_completions(self, word):
        parts = word.strip('`').split('.')
        if self.dev_mode:
            logging.debug(f"get_partial_identifier_completions called with parts: {parts}")
        if len(parts) == 1:
            for project in self.projects:
                if project.startswith(parts[0]):
                    yield Completion(f"`{project}`", start_position=-len(parts[0]))
        elif len(parts) == 2:
            project_id = parts[0].strip('`')
            dataset_prefix = parts[1]
            datasets = get_datasets(self.client, project_id)
            for dataset in datasets:
                if dataset.startswith(dataset_prefix):
                    yield Completion(f"`{project_id}`.`{dataset}`", start_position=-len(word))
        elif len(parts) == 3:
            project_id = parts[0].strip('`')
            dataset_id = parts[1].strip('`')
            table_prefix = parts[2]
            tables = get_tables(self.client, project_id, dataset_id)
            for table in tables:
                if table.startswith(table_prefix):
                    yield Completion(f"`{project_id}`.`{dataset_id}`.`{table}`", start_position=-len(word))

    def get_column_completions(self, word, text_before_cursor):
        table_aliases = self.extract_table_aliases(text_before_cursor)
        if self.dev_mode:
            logging.debug(f"Extracted table aliases: {table_aliases}")
        columns = []
        for alias, table_full_name in table_aliases.items():
            # Remove backticks from each part of the table name
            parts = [part.replace('`', '').strip() for part in table_full_name.split('.')]
            if len(parts) == 3:
                project_id, dataset_id, table_id = parts
                if self.dev_mode:
                    logging.debug(f"Fetching columns for {project_id}.{dataset_id}.{table_id}")
                table_columns = get_columns(self.client, project_id, dataset_id, table_id)
                if self.dev_mode:
                    logging.debug(f"Columns for {table_full_name}: {table_columns}")
                if alias != table_full_name:
                    # If there is an alias, prefix the column names with the alias
                    table_columns = [f"{alias}.{col}" for col in table_columns]
                columns.extend(table_columns)
        # Remove duplicates
        columns = list(set(columns))
        for column in columns:
            if column.startswith(word):
                yield Completion(column, start_position=-len(word))

    def extract_table_aliases(self, text):
        # This method extracts table names and their aliases from the query
        table_aliases = {}
        # Handle FROM and JOIN clauses
        pattern = re.compile(
            r'(?:FROM|JOIN)\s+(`[^`]+`|\S+)(?:\s+(?:AS\s+)?(\w+))?',
            re.IGNORECASE | re.MULTILINE
        )
        matches = pattern.finditer(text)
        if self.dev_mode:
            logging.debug(f"Extracting table aliases with pattern: {pattern}")
            logging.debug(f"Text to search: '{text}'")
        for match in matches:
            # Remove backticks from the table name
            table_name = match.group(1).replace('`', '').strip()
            # Remove backticks from the alias, if present
            alias = match.group(2).replace('`', '').strip() if match.group(2) else table_name
            if self.dev_mode:
                logging.debug(f"Matched table: '{table_name}', alias: '{alias}'")
            table_aliases[alias] = table_name
        return table_aliases

    def is_in_column_context(self, text):
        # Determine if the cursor is in a context where column names are expected
        column_context_keywords = ['SELECT', 'WHERE', 'AND', 'OR', 'ON', 'GROUP BY', 'ORDER BY', 'HAVING', 'BY']
        # Remove strings and comments
        text = re.sub(r"(['\"])(?:(?=(\\?))\2.)*?\1", '', text)  # Remove strings
        text = re.sub(r'--.*', '', text)  # Remove single line comments

        tokens = re.findall(r'\b\w+\b', text.upper())
        if not tokens:
            return False

        last_token = tokens[-1]

        if last_token in column_context_keywords:
            return True

        # Also, if the last character is a comma, it's likely we're entering a column name
        if text.strip() and text.strip()[-1] == ',':
            return True

        return False

# Helper functions
def get_projects(client):
    try:
        projects = list(client.list_projects())
        return [project.project_id for project in projects]
    except Exception as e:
        logging.error(f"Failed to list projects: {e}")
        return []

def get_datasets(client, project_id):
    try:
        datasets = list(client.list_datasets(project=project_id))
        return [dataset.dataset_id for dataset in datasets]
    except Exception as e:
        logging.error(f"Failed to list datasets for project {project_id}: {e}")
        return []

def get_tables(client, project_id, dataset_id):
    try:
        dataset_ref = client.dataset(dataset_id.replace('`', ''), project=project_id.replace('`', ''))
        tables = list(client.list_tables(dataset_ref))
        return [table.table_id for table in tables]
    except Exception as e:
        logging.error(f"Failed to list tables for dataset {dataset_id}: {e}")
        return []

def get_columns(client, project_id, dataset_id, table_id):
    try:
        # Remove backticks from identifiers
        project_id = project_id.replace('`', '')
        dataset_id = dataset_id.replace('`', '')
        table_id = table_id.replace('`', '')
        table_ref = client.dataset(dataset_id, project=project_id).table(table_id)
        table = client.get_table(table_ref)
        return [field.name for field in table.schema]
    except Exception as e:
        logging.error(f"Failed to get columns for table {table_id}: {e}")
        return []

def add_default_where_clause(query, client):
    if 'where' not in query.lower():
        timestamp_column = find_timestamp_column(query, client)
        if timestamp_column:
            two_days_ago = (datetime.utcnow() - timedelta(days=2)).strftime('%Y-%m-%d %H:%M:%S')
            where_clause = f" WHERE `{timestamp_column}` >= '{two_days_ago}'"
            if 'limit' in query.lower():
                idx = query.lower().rfind('limit')
                query = query[:idx] + where_clause + ' ' + query[idx:]
            else:
                query += where_clause
    return query

def add_limit_clause(query):
    if 'limit' not in query.lower():
        query += ' LIMIT 100'
    return query

def validate_query(client, query):
    try:
        query_job = client.query(query, job_config=bigquery.QueryJobConfig(dry_run=True))
        query_job.result()
        return True, None
    except Exception as e:
        return False, str(e)

def find_timestamp_column(query, client):
    table_full_name = extract_table_name(query)
    if table_full_name:
        parts = [part.replace('`', '').strip() for part in table_full_name.split('.')]
        if len(parts) == 3:
            project_id, dataset_id, table_id = parts
            columns = get_columns_with_types(client, project_id, dataset_id, table_id)
            for column_name, column_type in columns.items():
                if column_type.upper() in ('TIMESTAMP', 'DATETIME', 'DATE'):
                    return column_name
    return None

def extract_table_name(query):
    pattern = re.compile(r'\bFROM\s+([^\s,;]+)', re.IGNORECASE)
    matches = pattern.findall(query)
    if matches:
        table_name = matches[-1].strip().strip(';').replace('`', '')
        return table_name
    return None

def get_columns_with_types(client, project_id, dataset_id, table_id):
    try:
        # Remove backticks from identifiers
        project_id = project_id.replace('`', '')
        dataset_id = dataset_id.replace('`', '')
        table_id = table_id.replace('`', '')
        table_ref = client.dataset(dataset_id, project=project_id).table(table_id)
        table = client.get_table(table_ref)
        return {field.name: field.field_type for field in table.schema}
    except Exception as e:
        logging.error(f"Failed to get columns for table {table_id}: {e}")
        return {}

def show_schema(client, table_identifier):
    try:
        table_ref = client.get_table(table_identifier.replace('`', ''))
        print(f"Schema for table {table_identifier}:")
        for field in table_ref.schema:
            print(f"{field.name} ({field.field_type})")
    except Exception as e:
        print(f"Error retrieving schema for {table_identifier}: {e}")

def show_table_info(client, table_identifier):
    try:
        table_ref = client.get_table(table_identifier.replace('`', ''))
        print(f"Information for table {table_identifier}:")
        print(f"Table ID: {table_ref.full_table_id}")
        print(f"Table Type: {table_ref.table_type}")
        print(f"Creation Time: {table_ref.created}")
        print(f"Last Modified: {table_ref.modified}")
        print(f"Row Count: {table_ref.num_rows}")
        print(f"Size: {table_ref.num_bytes} bytes")
        print(f"Schema:")
        for field in table_ref.schema:
            print(f" - {field.name} ({field.field_type})")
    except Exception as e:
        print(f"Error retrieving information for {table_identifier}: {e}")

