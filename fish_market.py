import boto3
import pandas as pd
import csv
import pymongo

s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")

bucket = 'data-eng-resources'
prefix = 'python'

def extract(bucket_name, prefix_name):
    bucket_contents = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix_name)
    fish_market = []
    for page in bucket_contents['Contents']:
        if 'fish-market' in page['Key']:
            file = page['Key']
            file_csv = s3_client.get_object(Bucket=bucket_name, Key=file)
            df = pd.read_csv(file_csv['Body'])
            fish_market.append(df)
    fish_market = pd.concat(fish_market, axis=0, ignore_index=True)
    return fish_market

def transform(fish_market):
    transformed_fish_market = fish_market.groupby('Species').mean()
    return transformed_fish_market

def load(transformed_fish_market):
    with open('GuriD.csv', 'w') as csvfile:
        transformed_fish_market.to_csv(csvfile)

    s3_client.upload_file(Filename='GuriD.csv', Bucket=bucket, Key='Data26/fish/GuriD.csv')

def load_to_localdb():
    client = pymongo.MongoClient()
    db = client['fishmarket']

    db.drop_collection("averages")
    db.create_collection('averages')
    filename = "GuriD.csv"

    with open(filename, 'r') as data:
        for line in csv.DictReader(data):
            db.averages.insert_one(line)


def load_to_ec2():
    client = pymongo.MongoClient("mongodb://18.194.99.217:27017/Sparta")
    db = client['fishmarkets']

    db.drop_collection("averages")
    db.create_collection('averages')
    filename = "GuriD.csv"

    with open(filename, 'r') as data:
        for line in csv.DictReader(data):
            db.averages.insert_one(line)



