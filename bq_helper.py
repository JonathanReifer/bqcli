import re
from google.cloud import bigquery
from prompt_toolkit.completion import Completer, Completion
import logging
from datetime import datetime, timedelta

client = bigquery.Client()

# Completer class for BigQuery
class BigQueryCompleter(Completer):
    def __init__(self):
        self.projects = get_projects()
        self.keywords = ['SELECT', 'FROM', 'WHERE', 'LIMIT', 'ORDER BY', 'GROUP BY', 'JOIN', 'ON',
                         'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'HELP', 'EXPORT',
                         'SCHEMA', 'INFO', 'EXIT', 'QUIT']
        self.functions = ['COUNT', 'SUM', 'MIN', 'MAX', 'AVG']
        self.all_completions = self.keywords + self.functions

    def get_completions(self, document, complete_event):
        text_before_cursor = document.text_before_cursor
        word_before_cursor = document.get_word_before_cursor(WORD=True)
        last_token = self.get_last_token(text_before_cursor)

        if last_token.upper() in ('FROM', 'JOIN',):
            for completion in self.get_table_completions(word_before_cursor):
                yield completion
        elif self.is_in_select_clause(text_before_cursor):
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
        if len(parts) == 1:
            for project in self.projects:
                if project.startswith(parts[0]):
                    yield Completion(f"`{project}`", start_position=-len(parts[0]))
        elif len(parts) == 2:
            project_id = parts[0].strip('`')
            dataset_prefix = parts[1]
            datasets = get_datasets(project_id)
            for dataset in datasets:
                if dataset.startswith(dataset_prefix):
                    yield Completion(f"`{project_id}`.`{dataset}`", start_position=-len(word))
        elif len(parts) == 3:
            project_id = parts[0].strip('`')
            dataset_id = parts[1].strip('`')
            table_prefix = parts[2]
            tables = get_tables(project_id, dataset_id)
            for table in tables:
                if table.startswith(table_prefix):
                    yield Completion(f"`{project_id}`.`{dataset_id}`.`{table}`", start_position=-len(word))

    def get_partial_identifier_completions(self, word):
        parts = word.strip('`').split('.')
        if len(parts) == 1:
            for project in self.projects:
                if project.startswith(parts[0]):
                    yield Completion(f"`{project}`", start_position=-len(parts[0]))
        elif len(parts) == 2:
            project_id = parts[0].strip('`')
            dataset_prefix = parts[1]
            datasets = get_datasets(project_id)
            for dataset in datasets:
                if dataset.startswith(dataset_prefix):
                    yield Completion(f"`{project_id}`.`{dataset}`", start_position=-len(word))
        elif len(parts) == 3:
            project_id = parts[0].strip('`')
            dataset_id = parts[1].strip('`')
            table_prefix = parts[2]
            tables = get_tables(project_id, dataset_id)
            for table in tables:
                if table.startswith(table_prefix):
                    yield Completion(f"`{project_id}`.`{dataset_id}`.`{table}`", start_position=-len(word))

    def get_column_completions(self, word, text_before_cursor):
        table_full_name = self.extract_table_name(text_before_cursor)
        if table_full_name:
            parts = table_full_name.strip('`').split('.')
            if len(parts) == 3:
                project_id, dataset_id, table_id = parts
                columns = get_columns(project_id, dataset_id, table_id)
                for column in columns:
                    if column.startswith(word):
                        yield Completion(column, start_position=-len(word))

    def extract_table_name(self, text):
        pattern = re.compile(r'\bFROM\s+(.*?)\b', re.IGNORECASE | re.DOTALL)
        matches = pattern.findall(text)
        if matches:
            last_match = matches[-1]
            table_name = last_match.strip().strip('`').split()[0]
            return table_name
        return None

    def is_in_select_clause(self, text):
        select_index = text.upper().rfind('SELECT')
        from_index = text.upper().rfind('FROM')
        if select_index != -1 and (from_index == -1 or select_index > from_index):
            return True
        return False

# Helper functions
def get_projects():
    try:
        projects = list(client.list_projects())
        return [project.project_id for project in projects]
    except Exception as e:
        logging.error(f"Failed to list projects: {e}")
        return []

def get_datasets(project_id):
    try:
        datasets = list(client.list_datasets(project=project_id))
        return [dataset.dataset_id for dataset in datasets]
    except Exception as e:
        logging.error(f"Failed to list datasets for project {project_id}: {e}")
        return []

def get_tables(project_id, dataset_id):
    try:
        dataset_ref = client.dataset(dataset_id, project=project_id)
        tables = list(client.list_tables(dataset_ref))
        return [table.table_id for table in tables]
    except Exception as e:
        logging.error(f"Failed to list tables for dataset {dataset_id}: {e}")
        return []

def get_columns(project_id, dataset_id, table_id):
    try:
        table_ref = client.dataset(dataset_id, project=project_id).table(table_id)
        table = client.get_table(table_ref)
        return [field.name for field in table.schema]
    except Exception as e:
        logging.error(f"Failed to get columns for table {table_id}: {e}")
        return []

def add_default_where_clause(query):
    if 'where' not in query.lower():
        timestamp_column = find_timestamp_column(query)
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

def find_timestamp_column(query):
    table_full_name = extract_table_name(query)
    if table_full_name:
        parts = table_full_name.strip('`').split('.')
        if len(parts) == 3:
            project_id, dataset_id, table_id = parts
            columns = get_columns_with_types(project_id, dataset_id, table_id)
            for column_name, column_type in columns.items():
                if column_type.upper() in ('TIMESTAMP', 'DATETIME', 'DATE'):
                    return column_name
    return None

def extract_table_name(query):
    pattern = re.compile(r'\bFROM\s+(.*?)\b', re.IGNORECASE | re.DOTALL)
    matches = pattern.findall(query)
    if matches:
        last_match = matches[-1]
        table_name = last_match.strip().strip(';').strip()
        return table_name
    return None

def get_columns_with_types(project_id, dataset_id, table_id):
    try:
        table_ref = client.dataset(dataset_id, project=project_id).table(table_id)
        table = client.get_table(table_ref)
        return {field.name: field.field_type for field in table.schema}
    except Exception as e:
        logging.error(f"Failed to get columns for table {table_id}: {e}")
        return {}

def show_schema(client, table_identifier):
    try:
        table_ref = client.get_table(table_identifier)
        print(f"Schema for table {table_identifier}:")
        for field in table_ref.schema:
            print(f"{field.name} ({field.field_type})")
    except Exception as e:
        print(f"Error retrieving schema for {table_identifier}: {e}")

def show_table_info(client, table_identifier):
    try:
        table_ref = client.get_table(table_identifier)
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

