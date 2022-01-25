import boto3
import pandas as pd

s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")

bucket = 'data-eng-resources'
prefix = 'python'

def extract(bucket_name, prefix_name):
    bucket_contents = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix_name)
    fish_market_csv = []
    for page in bucket_contents['Contents']:
        if 'fish-market' in page['Key']:
            file = page['Key']
            file_csv = s3_client.get_object(Bucket=bucket_name, Key=file)
            df = pd.read_csv(file_csv['Body'])
            fish_market_csv.append(df)
    fish_market_csv = pd.concat(fish_market_csv, axis=0, ignore_index=True)
    return fish_market_csv

def transform(fish_market_csv):
    transformed_fish_market_csv = fish_market_csv.groupby('Species').mean()
    return transformed_fish_market_csv

def load(transformed_fish_market_csv):
    with open('GuriD.csv', 'w') as csvfile:
        transformed_fish_market_csv.to_csv(csvfile)

    s3_client.upload_file(Filename='GuriD.csv', Bucket=bucket, Key='Data26/fish/GuriD.csv')


# load(transform(extract(bucket, prefix)))