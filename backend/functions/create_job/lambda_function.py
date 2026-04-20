import json
import os
import uuid
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError


dynamodb = boto3.resource("dynamodb")
s3_client = boto3.client("s3")


# File type mapping
FILE_TYPES = {
    "csv": {"ext": ".csv", "content_type": "text/csv"},
    "xlsx": {"ext": ".xlsx", "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
    "xls": {"ext": ".xls", "content_type": "application/vnd.ms-excel"},
}


def _headers():
    return {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Allow-Methods": "OPTIONS,POST",
    }


def _response(status_code: int, body: dict):
    return {
        "statusCode": status_code,
        "headers": _headers(),
        "body": json.dumps(body),
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


def _parse_body(event: dict) -> dict:
    body = event.get("body", "{}")
    if not body:
        return {}
    if isinstance(body, str):
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return {}
    return body


def lambda_handler(event, context):
    try:
        if _request_method(event) == "OPTIONS":
            return _response(200, {"ok": True})

        table_name = _required_env("TABLE_NAME")
        upload_bucket = _required_env("UPLOAD_BUCKET")
        expires_in = int(os.environ.get("UPLOAD_URL_EXPIRES_SECONDS", "900"))

        table = dynamodb.Table(table_name)

        # Parse request body to get file type
        body = _parse_body(event)
        file_type = body.get("fileType", "csv").lower()

        # Validate and get file type info
        if file_type not in FILE_TYPES:
            file_type = "csv"  # Default fallback

        type_info = FILE_TYPES[file_type]

        now = datetime.now(timezone.utc).isoformat()
        job_id = str(uuid.uuid4())
        upload_key = f"uploads/{job_id}{type_info['ext']}"

        item = {
            "jobId": job_id,
            "status": "CREATED",
            "uploadKey": upload_key,
            "fileType": file_type,
            "createdAt": now,
            "updatedAt": now,
        }

        try:
            table.put_item(Item=item)
            upload_url = s3_client.generate_presigned_url(
                "put_object",
                Params={
                    "Bucket": upload_bucket,
                    "Key": upload_key,
                    "ContentType": type_info["content_type"],
                },
                ExpiresIn=expires_in,
            )
        except ClientError as exc:
            return _response(500, {"error": "Failed to create job", "details": str(exc)})

        return _response(
            201,
            {
                "jobId": job_id,
                "status": "CREATED",
                "upload": {
                    "url": upload_url,
                    "method": "PUT",
                    "headers": {"Content-Type": type_info["content_type"]},
                    "key": upload_key,
                    "expiresInSeconds": expires_in,
                },
            },
        )
    except Exception as exc:
        return _response(500, {"error": "Failed to create job", "details": str(exc)})
