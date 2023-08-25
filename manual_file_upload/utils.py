import boto3
import redshift_connector
import csv

session = boto3.Session(profile_name='Creditkasa_Prod')

local_path = r"D:/Work/Creditkasa/export"
bucket_name = "ck-manual-load"
db_name = "/db/prod/ck_dwh/"


def get_secret_value(value):
    ssm = boto3.client('ssm')
    secret_name = ""
    if value in ("user", "password", "accountid", "copy_to_redshift_arn"):
        secret_name = f'/vryabokon{db_name}{value}'
    else:
        secret_name = f'{db_name}{value}'

    result = ssm.get_parameter(Name=secret_name, WithDecryption=False)['Parameter']['Value']

    return result


def get_redshift_connection():
    host = get_secret_value("host")
    database = get_secret_value("database")
    db_user = get_secret_value("user")
    db_password = get_secret_value("password")
    port = 5439

    conn = redshift_connector.connect(
        host=host,
        database=database,
        port=port,
        user=db_user,
        password=db_password)

    print(f"Connect to {host} established")

    return conn


def execute_query(query):
    conn = get_redshift_connection()
    conn.cursor().execute(query)
    print(f"SQL query {query} completed")
    conn.commit()


def read_file_header(file_name, delimiter):
    local_file_path = f"{local_path}/{file_name}.csv"

    with open(local_file_path, 'r') as file:
        csv_reader = csv.reader(file)
        header = next(csv_reader)
        column_list = delimiter.join(header).split(delimiter)

    return column_list


def generate_create_table_statement(column_list, table_name, schema):
    columns = ', '.join([f'{col} varchar(max)' for col in column_list])
    create_table_statement = f'CREATE TABLE {schema}.{table_name} ({columns})'
    return create_table_statement
