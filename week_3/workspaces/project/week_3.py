from datetime import datetime
from typing import List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SensorEvaluationContext,
    SkipReason,
    graph,
    op,
    schedule,
    sensor,
    static_partitioned_config,
)
from workspaces.config import REDIS, S3
from workspaces.project.sensors import get_s3_keys
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    config_schema = {"s3_key": str},
    required_resource_keys = {"s3"},
    tags = {"kind": "s3"},
    out = {"stocks": Out(dagster_type = List[Stock],
    description = "Get a list of stock data from s3_key")}
)


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

def process_data(context, stocks):
    
    higher = max(stocks, key = lambda x: x.high)

    return Aggregation(date = higher.date, high= higher.high)


@op(
    required_resource_keys = {"redis"},
    tags = {"kind": "redis"},
    description = "Upload data into Redis",
    ins = {"higher_value_data": In(dagster_type = Aggregation)}
)

def put_redis_data(context, higher_value_data):
    
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
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


docker = {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}

@static_partitioned_config(partition_keys = [str(x) for x in range(1, 11)])
def docker_config(partition_key: str):
    return {
        "resources": {
            "s3": {"config": S3},
            "redis": {"config": REDIS},
            },
        "ops": {"get_s3_data": {"config": {"s3_key": f"prefix/stock_{partition_key}.csv"}}},
    }


machine_learning_job_local = machine_learning_graph.to_job(
    name = "machine_learning_job_local",
    config = local,
    resource_defs = {"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
)

# We are configuring that the machine_learning_job_docker retry to run if it fails until it reach 10 times
machine_learning_job_docker = machine_learning_graph.to_job(
    name = "machine_learning_job_docker",
    config = docker_config,
    resource_defs = {"s3": s3_resource, "redis": redis_resource},
    op_retry_policy = RetryPolicy(max_retries = 10, delay = 1)
)

# Scheduling machine-learning_job_local each 15 minutes
machine_learning_schedule_local =  ScheduleDefinition(job = machine_learning_job_local, cron_schedule = "*/15 * * * *")

# Scheduling machine_learning_schedule_docker each hour
@schedule(cron_schedule = "0 * * * *", job = machine_learning_job_docker)
def machine_learning_schedule_docker(context):
    for partition_key in docker_config.get_partition_keys():
        yield RunRequest(
            run_key=partition_key,
            run_config=docker_config.get_run_config_for_partition_key(partition_key)
        )

@sensor(job = machine_learning_job_docker)
def machine_learning_sensor_docker(context):
    s3_keys = get_s3_keys(bucket = "dagster", prefix = "prefix", endpoint_url = "http://localstack:4566")
    # We are going to skip it if tehre are not new s3_keys
    if not s3_keys:
        yield SkipReason("No new s3 files found in bucket.")
        return
    # If there are new s3_keys trigger the RunRequest
    for new_key in s3_keys:
        yield RunRequest(
            run_key=new_key,
            run_config={
               "resources": {
                    "s3": {"config": S3},
                    "redis": {"config": REDIS},
                },
                "ops": {"get_s3_data": {"config": {"s3_key": new_key}}},
            }
        )