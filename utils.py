import boto3

session = boto3.Session(profile_name='Creditkasa_Prod')

local_path = r"D:/Work/Creditkasa/export"
bucket_name = "ck-manual-load"
region_name = "eu-central-1"


