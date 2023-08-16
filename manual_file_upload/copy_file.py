from manual_file_upload.utils import generate_create_table_statement, read_file_header, execute_query, bucket_name, get_secret_value


def copy_file_to_redshift(file_name, table_name, schema='public'):
    column_list = read_file_header(file_name)

    dll = generate_create_table_statement(column_list, table_name, schema)
    accountid = get_secret_value("accountid")
    arn = get_secret_value("copy_to_redshift_arn")

    drop_table = f"drop table if exists {schema}.{table_name}"
    copy = f"""COPY {schema}.{table_name} 
               FROM 's3://{bucket_name}/export/{file_name}.csv' 
               IAM_ROLE 'arn:aws:iam::{accountid}:role/{arn}' 
               DELIMITER ';' 
               FORMAT AS CSV"""

    execute_query(drop_table)
    execute_query(dll)
    execute_query(copy)