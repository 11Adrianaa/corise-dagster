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
    out = {"stocks": Out(dagster_type = List[Stock],
    description = "Get a list of stock data from s3_key")}
)

    #The information we are starting to work with in our pipeline for now comes
    #from a csv file, we will extract it using s3_key context and csv_helper function.
    #The input s3_key is a string, meanwhile the output we will get is a list of stock.


def get_s3_data_op(context):

    file_name = context.op_config["s3_key"]

    return list(csv_helper(file_name))


@op(description = "Process of data in order to get the date that contains the higher stock value",
    ins = {"stocks": In(dagster_type = List[Stock],
            description = "Stocks")},
    out = {"higher_value_data": Out(dagster_type = Aggregation,
            description = "Two fields, Date that contains the higher value and the higher value")}
)

    #Given a list of stocks from different dates, we will get the date that has
    #the higher stock value.
    #The input we will gave to the op is a list, and we want the result as the Aggregation format.


def process_data_op(context, stocks):
    
    higher = max(stocks, key = lambda x: x.high)

    return Aggregation(date = higher.date, high= higher.high)


    #We will use lambda in order to get the higher value and its corresponding date.



@op(
    description = "Upload data into Redis",
    ins = {"higher_value_data": In(dagster_type = Aggregation)}
)
def put_redis_data_op(context, higher_value_data):
    pass


@op(
    description = "Upload data into s3",
    ins = {"higher_value_data": In(dagster_type = Aggregation)}
)
def put_s3_data_op(context, higher_value_data):
    pass


@graph
def machine_learning_graph():
    pass


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
    name="machine_learning_job_local",
)

machine_learning_job_docker = machine_learning_graph.to_job(
    name="machine_learning_job_docker",
)


"""@job
def machine_learning_job():
    higher_value_data = process_data_op(get_s3_data_op())
    put_redis_data_op(higher_value_data)
    put_s3_data_op(higher_value_data)"""