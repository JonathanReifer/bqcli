from google.cloud import bigquery

client = bigquery.Client()
project_id = 'delta-discovery--001'
dataset_id = 'Test_Dataset_001'
table_id = 'Test_Table_001'

table_ref = client.dataset(dataset_id, project=project_id).table(table_id)
table = client.get_table(table_ref)
columns = [field.name for field in table.schema]
print(columns)





