from datetime import datetime
from typing import List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    ResourceDefinition,
    String,
    graph,
    op,
)
from workspaces.config import REDIS, S3, S3_FILE
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    config_schema = {"s3_key": str},
    required_resource_keys = {"s3"},
    tags = {"kind": "s3"},
    out = {"stocks": Out(dagster_type = List[Stock],
    description = "Get a list of stock data from s3_key")}
)

    #The information we are working with in our pipeline comes now from our S3 resource,
    #in order to extract it we will use  get_data and we will convert it to our desired output.


def get_s3_data(context):

    file_s3 = context.op_config["s3_key"]
    s3_data = context.resources.s3.get_data(file_s3)

    stocks_list = []

    for row in s3_data:
        stock = Stock.from_list(row)
        stocks_list.append(stock)

    return stocks_list


@op(description = "Process of data in order to get the date that contains the higher stock value",
    ins = {"stocks": In(dagster_type = List[Stock],
            description = "Stocks")},
    out = {"higher_value_data": Out(dagster_type = Aggregation,
            description = "Two fields, Date that contains the higher value and the higher value")}
)

    #Given a list of stocks from different dates, we will get the date that has
    #the higher stock value.
    #The input we will gave to the op is a list, and we want the result as the Aggregation format.


def process_data(context, stocks):
    
    higher = max(stocks, key = lambda x: x.high)

    return Aggregation(date = higher.date, high= higher.high)


    #We will use lambda in order to get the higher value and its corresponding date.



@op(
    required_resource_keys = {"redis"},
    tags = {"kind": "redis"},
    description = "Upload data into Redis",
    ins = {"higher_value_data": In(dagster_type = Aggregation)}
)
def put_redis_data(context, higher_value_data):
    #OpExecutionContext
    higher_date = str(higher_value_data.date)
    higher_value = str(higher_value_data.high)

    context.resources.redis.put_data(name = higher_date, value = higher_value)

@op(
    required_resource_keys = {"s3"},
    tags = {"kind": "s3"},
    description = "Upload data into s3",
    ins = {"higher_value_data": In(dagster_type = Aggregation)}
)
def put_s3_data(context, higher_value_data):
    #OpExecutionContext
    higher_date = str(higher_value_data.date)
    higher_value = higher_value_data.high

    context.resources.s3.put_data(name = higher_date, value = higher_value) 


@graph
def machine_learning_graph():
    all_stocks = get_s3_data()
    highest = process_data(all_stocks)
    put_redis_data(highest)
    put_s3_data(highest)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

docker = {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

machine_learning_job_local = machine_learning_graph.to_job(
    name = "machine_learning_job_local",
    config = local,
    resource_defs = {"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
)

machine_learning_job_docker = machine_learning_graph.to_job(
    name = "machine_learning_job_docker",
    config = docker,
    resource_defs = {"s3": s3_resource, "redis": redis_resource},
)
