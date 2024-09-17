import re
import logging
from google.cloud import bigquery
from prompt_toolkit.completion import Completer, Completion

# Helper functions
def get_projects(client):
    projects = []
    try:
        for project in client.list_projects():
            projects.append(project.project_id)
    except Exception as e:
        logging.error(f"Failed to list projects: {e}")
    return projects

def get_datasets(client, project_id):
    datasets = []
    project_id = project_id.replace('`', '').strip()
    try:
        for dataset in client.list_datasets(project=project_id):
            datasets.append(dataset.dataset_id)
    except Exception as e:
        logging.error(f"Failed to list datasets in project {project_id}: {e}")
    return datasets

def get_tables(client, project_id, dataset_id):
    tables = []
    project_id = project_id.replace('`', '').strip()
    dataset_id = dataset_id.replace('`', '').strip()
    try:
        dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
        for table in client.list_tables(dataset_ref):
            tables.append(table.table_id)
    except Exception as e:
        logging.error(f"Failed to list tables in dataset {project_id}.{dataset_id}: {e}")
    return tables

def get_columns(client, project_id, dataset_id, table_id):
    columns = []
    try:
        table_ref = client.dataset(dataset_id, project=project_id).table(table_id)
        table = client.get_table(table_ref)
        columns = [field.name for field in table.schema]
    except Exception as e:
        logging.error(f"Failed to get columns for table {project_id}.{dataset_id}.{table_id}: {e}")
    return columns

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

    def get_completions(self, document, complete_event):
        text_before_cursor = document.text_before_cursor
        word_before_cursor = document.get_word_before_cursor(WORD=True)
        last_token = self.get_last_token(text_before_cursor)

        if self.dev_mode:
            logging.debug(f"get_completions called with word: '{word_before_cursor}'")
            logging.debug(f"Last token: '{last_token}'")
            logging.debug(f"Text before cursor: '{text_before_cursor}'")

        # If word contains a dot, use partial identifier completions
        if '.' in word_before_cursor or word_before_cursor.endswith('.'):
            for completion in self.get_partial_identifier_completions(word_before_cursor):
                yield completion
        elif last_token.upper() in ('FROM', 'JOIN', 'SCHEMA', 'INFO', 'DETAILS'):
            if self.dev_mode:
                logging.debug("Context: Table")
            for completion in self.get_table_completions(word_before_cursor):
                yield completion
        elif self.is_in_column_context(document):
            if self.dev_mode:
                logging.debug("Context: Column")
            for completion in self.get_column_completions(word_before_cursor, document):
                yield completion
        else:
            for kw in self.all_completions:
                if kw.upper().startswith(word_before_cursor.upper()):
                    yield Completion(kw, start_position=-len(word_before_cursor))


    def get_last_token(self, text):
        # Remove comments
        text = re.sub(r'--.*', '', text)
        # Remove strings
        text = re.sub(r"(['\"`])(?:\\.|[^\\])*?\1", '', text)
        tokens = re.findall(r'\b\w+\b', text)
        return tokens[-1] if tokens else ''

    def get_table_completions(self, word):
        # Remove backticks from the word
        word_clean = word.replace('`', '')
        # Handle trailing dot
        if word_clean.endswith('.'):
            word_clean = word_clean[:-1]
            trailing_dot = True
        else:
            trailing_dot = False
        # Split the word by '.' to get parts
        parts = word_clean.split('.')
        if trailing_dot:
            parts.append('')
        parts = [part.strip() for part in parts if part.strip()]  # Remove empty strings and whitespace
        if self.dev_mode:
            logging.debug(f"get_table_completions called with word: '{word}', parts: {parts}")

        if len(parts) == 0:
            # No parts typed yet, suggest project IDs
            for project in self.projects:
                if project.startswith(word_clean):
                    yield Completion(f'`{project}`', start_position=-len(word))
        elif len(parts) == 1:
            # Project ID is typed, suggest datasets
            project_id = parts[0]
            datasets = get_datasets(self.client, project_id)
            if self.dev_mode:
                logging.debug(f"Datasets in project '{project_id}': {datasets}")
            for dataset in datasets:
                full_name = f'`{project_id}`.`{dataset}`'
                yield Completion(full_name, start_position=-len(word))
        elif len(parts) == 2:
            project_id, dataset_id = parts
            # Suggest tables in the dataset
            tables = get_tables(self.client, project_id, dataset_id)
            if self.dev_mode:
                logging.debug(f"Tables in dataset '{project_id}.{dataset_id}': {tables}")
            for table in tables:
                full_name = f'`{project_id}`.`{dataset_id}`.`{table}`'
                yield Completion(full_name, start_position=-len(word))
        else:
            # Full table identifier is typed; no further completions
            pass


    def get_partial_identifier_completions(self, word):
        """
        Provide completions for partially typed identifiers.
        This is called when a dot is in the word before the cursor.
        """
        if self.dev_mode:
            logging.debug(f"get_partial_identifier_completions called with word: '{word}'")

        # Remove backticks and split the word by '.'
        parts = re.split(r'\.`?|`?\.', word)
        parts = [part.replace('`', '').strip() for part in parts]
        if self.dev_mode:
            logging.debug(f"Parts after splitting: {parts}")

        # Handle empty parts (e.g., when the user types a period at the end)
        if word.endswith('.'):
            parts.append('')

        if len(parts) == 1:
            # Suggest datasets in the project
            project_id = parts[0]
            datasets = get_datasets(self.client, project_id)
            if self.dev_mode:
                logging.debug(f"Datasets in project '{project_id}': {datasets}")
            for dataset in datasets:
                full_name = f'`{project_id}`.`{dataset}`'
                yield Completion(full_name, start_position=-len(word))
        elif len(parts) == 2:
            project_id, dataset_id = parts[0], parts[1]
            if dataset_id == '':
                # User typed 'project_id.', suggest datasets
                datasets = get_datasets(self.client, project_id)
                if self.dev_mode:
                    logging.debug(f"Datasets in project '{project_id}': {datasets}")
                for dataset in datasets:
                    full_name = f'`{project_id}`.`{dataset}`'
                    yield Completion(full_name, start_position=-len(word))
            else:
                # Suggest tables in the dataset
                tables = get_tables(self.client, project_id, dataset_id)
                if self.dev_mode:
                    logging.debug(f"Tables in dataset '{project_id}.{dataset_id}': {tables}")
                for table in tables:
                    full_name = f'`{project_id}`.`{dataset_id}`.`{table}`'
                    yield Completion(full_name, start_position=-len(word))
        elif len(parts) == 3:
            project_id, dataset_id, table_id = parts
            if table_id == '':
                # User typed 'project_id.dataset_id.', suggest tables
                tables = get_tables(self.client, project_id, dataset_id)
                if self.dev_mode:
                    logging.debug(f"Tables in dataset '{project_id}.{dataset_id}': {tables}")
                for table in tables:
                    full_name = f'`{project_id}`.`{dataset_id}`.`{table}`'
                    yield Completion(full_name, start_position=-len(word))
            else:
                # Full table identifier is typed; no further completions
                pass
        else:
            # Invalid identifier; no completions
            pass


    def get_column_completions(self, word, document):
        table_aliases = self.extract_table_aliases(document.text)
        if self.dev_mode:
            logging.debug(f"Extracted table aliases: {table_aliases}")
        columns = []
        for table_full_name, alias in table_aliases:
            parts = [part.replace('`', '').strip() for part in table_full_name.split('.')]
            if len(parts) == 3:
                project_id, dataset_id, table_id = parts
                if self.dev_mode:
                    logging.debug(f"Fetching columns for {project_id}.{dataset_id}.{table_id}")
                table_columns = get_columns(self.client, project_id, dataset_id, table_id)
                if self.dev_mode:
                    logging.debug(f"Columns for {table_full_name}: {table_columns}")
                if alias:
                    table_columns = [f"{alias}.{col}" for col in table_columns]
                columns.extend(table_columns)
            else:
                if self.dev_mode:
                    logging.debug(f"Invalid table identifier parts: {parts}")
        # Remove duplicates
        columns = list(set(columns))
        for column in columns:
            if column.startswith(word):
                yield Completion(column, start_position=-len(word))

    def extract_table_aliases(self, text):
        table_aliases = []
        pattern = re.compile(
            r'(?:FROM|JOIN)\s+'
            r'((?:`[^`]+`|\w+)(?:\.(?:`[^`]+`|\w+))*?)'
            r'(?:\s+AS\s+(\w+))?'
            r'(?=\s|,|$)',
            re.IGNORECASE | re.MULTILINE
        )
        matches = pattern.finditer(text)
        if self.dev_mode:
            logging.debug(f"Extracting table aliases with pattern: {pattern}")
            logging.debug(f"Text to search: '{text}'")
        for match in matches:
            table_name = match.group(1).strip()
            alias = match.group(2).strip() if match.group(2) else None
            if self.dev_mode:
                logging.debug(f"Matched table: '{table_name}', alias: '{alias}'")
            table_aliases.append((table_name, alias))
        return table_aliases

    def is_in_column_context(self, document):
        text_before_cursor = document.text_before_cursor
        if self.dev_mode:
            logging.debug(f"Text before removing strings/comments: '{text_before_cursor}'")

        # Remove strings enclosed in single quotes, double quotes, or backticks
        text_before_cursor = re.sub(r"(['\"`])(?:\\.|[^\\])*?\1", '', text_before_cursor, flags=re.DOTALL)
        # Remove single-line comments
        text_before_cursor = re.sub(r'--.*', '', text_before_cursor)
        if self.dev_mode:
            logging.debug(f"Text after removing strings/comments: '{text_before_cursor}'")

        # Tokenize the text
        tokens = re.findall(r'\b\w+\b', text_before_cursor.upper())
        if not tokens:
            return False

        if self.dev_mode:
            logging.debug(f"Tokens before cursor: {tokens}")

        # Define keywords indicating column or table context
        column_context_keywords = {'SELECT', 'WHERE', 'AND', 'OR', 'ON', 'BY', 'HAVING', 'GROUP', 'ORDER', 'IN', 'NOT', 'EXISTS'}
        table_context_keywords = {'FROM', 'JOIN', 'INTO', 'UPDATE', 'TABLE', 'DELETE', 'INSERT'}
        operators = {'=', '<', '>', '<=', '>=', '<>', '!=', '+', '-', '*', '/', '%', 'LIKE', 'BETWEEN', 'IS'}

        # Check the last tokens to determine context
        for token in reversed(tokens):
            if token in table_context_keywords:
                # We're in a table context, not a column context
                return False
            elif token in column_context_keywords or token in operators:
                return True
            elif re.match(r'\w+', token):
                # Continue checking previous tokens
                continue

        # Check if the character before the cursor suggests a column context
        if text_before_cursor.strip() and text_before_cursor.strip()[-1] in '.=><!+-*/%(), ':
            return True

        return False


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

