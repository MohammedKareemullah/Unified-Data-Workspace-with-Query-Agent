# import boto3

# ec2 = boto3.client('ec2')
# response = ec2.describe_instance_types()

# for instance in response['InstanceTypes']:
#     print(instance)

import boto3
import json

pricing = boto3.client(
    "pricing",
    region_name="us-east-1"
)

paginator = pricing.get_paginator("get_products")

for page in paginator.paginate(
    ServiceCode="AmazonEC2",
    Filters=[
        {
            "Type": "TERM_MATCH",
            "Field": "location",
            "Value": "US East (N. Virginia)"
        }
    ],
    FormatVersion="aws_v1",
    MaxResults=100
):

    for product_string in page["PriceList"]:

        product = json.loads(product_string)

        print(product)