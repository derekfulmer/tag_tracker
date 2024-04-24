#!/usr/bin/env python3

import boto3
import argparse
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
import string
from pprint import pprint


def arg_parse():
    """
    Defines arguments for user input
    """
    parser = argparse.ArgumentParser(
        prog="tagtracker", description="Find instances based on tags"
    )
    parser.add_argument(
        "--tagkey",
        action="store",
        dest="tagkey",
        help="The key, or primary identifier, of the tag",
        required=True,
    )
    parser.add_argument(
        "--tagvalue",
        action="store",
        dest="tagvalue",
        help="The value of the tag in relation to the key",
    ),
    parser.add_argument(
        "--regionid",
        action="store",
        dest="regionid",
        help="The region key id where the host resides",
    ),
    parser.add_argument(
        "--awsregion",
        action="store",
        dest="awsregion",
        help="The AWS region to query hosts",
    ),
    parser.add_argument(
        "--environment", "-e",
        action="store",
        dest="environment",
        help="The account/environment name e.g sandbox|dev|qa|stage|prod",
    )
    args = parser.parse_args()
    return args


# DynamoDB Helper Functions
def _dynamo_obj_to_python_obj(dynamo_obj: dict) -> dict:
    deserializer = TypeDeserializer()
    return _replace_decimals({
        k: deserializer.deserialize(v)
        for k, v in dynamo_obj.items()
    })


def _replace_decimals(obj):
    if isinstance(obj, list):
        for i in range(len(obj)):
            obj[i] = _replace_decimals(obj[i])
        return obj
    elif isinstance(obj, dict):
        for k in obj.keys():
            obj[k] = _replace_decimals(obj[k])
        return obj
    elif isinstance(obj, Decimal):
        if obj % 1 == 0:
            return int(obj)
        else:
            return float(obj)
    else:
        return obj


def _python_obj_to_dynamo_obj(python_obj: dict) -> dict:
    serializer = TypeSerializer()
    return {
        k: serializer.serialize(v)
        for k, v in python_obj.items()
    }
# End of Helper Functions

def list_instances_by_tags(ec2client, fargs=None):
    response = ec2client.describe_instances(
        Filters=[
            {
                "Name": f"tag:{fargs.tagkey}",
                "Values": [fargs.tagvalue]
            }
        ]
    )

    instance_list = []
    for i in response['Reservations']:
        for j in i['Instances']:
            instance_list.append(j['PublicIpAddress'])
	
    return instance_list


def get_region(ddb, table_name, region_id):
    items = ddb.query(
        TableName = table_name,
        KeyConditionExpression = "#pkey = :value",
        ExpressionAttributeValues = {
            ":value": {"N": region_id}
        },
        ExpressionAttributeNames = {
            "#pkey": "RegionId"
        }
    )
    return _dynamo_obj_to_python_obj(items['Items'][0])


def build_json(region, instance_list):
    region_id = region['RegionId']
    region['Nodes'] = []
    alphabet = string.ascii_lowercase
    idx = 0
    for ipv4 in instance_list:
        part1 = ipv4.split('.')[0]     
        part2 = ipv4.split('.')[1]
        hostname = f"{part1}-{part2}.net"
        name = f"{region_id}{alphabet[idx]}"
        node = {'RegionId': region_id, 'HostName': hostname, 'IPv4': ipv4, 'Name': name}
        region['Nodes'].append(node)
        idx +=1
    return region


def post_to_dynamodb(ddb, table_name, region):
    response = ddb.put_item(
        TableName=table_name,
        Item=_python_obj_to_dynamo_obj(region)
    )


def main():
    args = arg_parse()

    ec2client = boto3.client("ec2", region_name=args.awsregion)
    ddb = boto3.client('dynamodb', region_name="us-east-2")
    table_name = f"{args.environment}-regions"

    # Get all instance public IPs from designated Region ID
    ids = list_instances_by_tags(ec2client, fargs=args)
    print(ids)

    # Get Current Region entry from DDB
    region = get_region(ddb, table_name, args.regionid)
    print("\nOLD REGION\n==========")
    pprint(region)
    print("\nNEW REGION\n==========")
    new_region = build_json(region, ids)
    pprint(new_region)

    # Post to DynamoDB
    post_to_dynamodb(ddb, table_name, new_region)

# Run main Code
if __name__ == "__main__":
    main()
