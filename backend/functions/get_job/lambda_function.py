import json
import os
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError


dynamodb = boto3.resource("dynamodb")


def _headers():
    return {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Allow-Methods": "OPTIONS,GET",
    }


def _json_default(value):
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"Type not serializable: {type(value)}")


def _response(status_code: int, body: dict):
    return {
        "statusCode": status_code,
        "headers": _headers(),
        "body": json.dumps(body, default=_json_default),
    }


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _request_method(event: dict) -> str:
    return (
        event.get("httpMethod")
        or event.get("requestContext", {}).get("http", {}).get("method")
        or ""
    )


def lambda_handler(event, context):
    try:
        if _request_method(event) == "OPTIONS":
            return _response(200, {"ok": True})

        job_id = (event.get("pathParameters") or {}).get("jobId")
        if not job_id:
            return _response(400, {"error": "Missing required path parameter: jobId"})

        table_name = _required_env("TABLE_NAME")
        table = dynamodb.Table(table_name)

        try:
            result = table.get_item(Key={"jobId": job_id})
        except ClientError as exc:
            return _response(500, {"error": "Failed to read job", "details": str(exc)})

        item = result.get("Item")
        if not item:
            return _response(404, {"error": "Job not found", "jobId": job_id})

        body = {
            "jobId": item["jobId"],
            "status": item.get("status", "UNKNOWN"),
            "createdAt": item.get("createdAt"),
            "updatedAt": item.get("updatedAt"),
            "errorMessage": item.get("errorMessage"),
            "resultKey": item.get("resultKey"),
        }
        return _response(200, body)
    except Exception as exc:
        return _response(500, {"error": "Failed to read job", "details": str(exc)})
