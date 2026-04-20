import json
import os
import re
import math
from collections import defaultdict
from datetime import datetime, timezone
from io import BytesIO

import boto3
from botocore.exceptions import ClientError


dynamodb = boto3.resource("dynamodb")
s3_client = boto3.client("s3")


def _headers():
    return {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
        "Access-Control-Allow-Methods": "OPTIONS,POST",
        "Access-Control-Max-Age": "600",
    }


def _response(status_code: int, body):
    return {
        "statusCode": status_code,
        "headers": _headers(),
        "body": json.dumps(body),
    }


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
    if isinstance(body, dict):
        return body
    return {}


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _call_bedrock(prompt: str, max_tokens: int = 1024, temperature: float = 0.7) -> str:
    """Call Bedrock Converse API for Nova-style text generation models."""
    bedrock = boto3.client("bedrock-runtime")
    model_id = os.environ.get("BEDROCK_MODEL_ID", "amazon.nova-lite-v1:0")

    response = bedrock.converse(
        modelId=model_id,
        messages=[
            {
                "role": "user",
                "content": [{"text": prompt}],
            }
        ],
        inferenceConfig={
            "maxTokens": max_tokens,
            "temperature": temperature,
            "topP": 0.9,
        },
    )

    content_blocks = (
        response.get("output", {})
        .get("message", {})
        .get("content", [])
    )

    texts = []
    for block in content_blocks:
        if isinstance(block, dict) and "text" in block:
            texts.append(block["text"])

    return "\n".join(t for t in texts if t).strip()


def _parse_query_intent(query: str, data_summary: dict, conversation_history: list = None) -> dict:
    """Use Bedrock to parse user query and determine intent with conversation context."""
    bedrock_enabled = os.environ.get("ENABLE_BEDROCK", "false").lower() == "true"
    
    if not bedrock_enabled:
        return _parse_query_fallback(query, data_summary)
    
    try:
        # Build conversation context
        context = ""
        if conversation_history and len(conversation_history) > 0:
            context = "\n\nConversation History:\n"
            for msg in conversation_history[-6:]:  # Last 3 turns
                role = msg.get("role", "user")
                content = msg.get("content", "")[:200]  # Truncate long messages
                context += f"{role.capitalize()}: {content}\n"
        
        prompt = f"""Parse this natural language query about a dataset and return a structured JSON response.

User Query: "{query}"{context}

Dataset Summary:
- Rows: {data_summary.get('totalRows', 0)}
- Columns: {', '.join(data_summary.get('columns', []))}
- Numeric Columns: {', '.join(data_summary.get('numericColumns', []))}
- Categorical Columns: {', '.join(data_summary.get('categoricalColumns', []))}

Advanced Analytics Available:
- Statistical tests (normality, outliers)
- Pattern detection (trends, periodicity)
- Clustering analysis
- Correlation analysis

Return a JSON object with these fields:
- "intent": one of ["aggregate", "filter", "compare", "trend", "top_bottom", "statistics", "distribution", "general", "insights", "correlations", "patterns", "outliers"]
- "columns": list of column names mentioned (empty if none)
- "operation": the operation to perform (e.g., "sum", "average", "count", "max", "min")
- "filters": list of filter conditions (each with "column", "operator", "value")
- "group_by": column to group by (if any)
- "limit": number of results to return (default 10)
- "chart_type": suggested chart type (one of ["bar", "line", "pie", "table", "scatter", "heatmap"])
- "follow_up": suggested follow-up questions (array of strings)

Return ONLY the JSON object, no other text."""

        ai_text = _call_bedrock(prompt, max_tokens=1024, temperature=0.3)
        
        # Extract JSON from response (Titan may include extra text)
        json_match = re.search(r'\{[\s\S]*\}', ai_text)
        if json_match:
            parsed = json.loads(json_match.group())
            parsed["_intentSource"] = "bedrock"
            return parsed

        fallback = _parse_query_fallback(query, data_summary)
        fallback["_intentSource"] = "fallback_no_json"
        return fallback
        
    except Exception as e:
        print(f"Bedrock query parsing failed: {e}")
        fallback = _parse_query_fallback(query, data_summary)
        fallback["_intentSource"] = "fallback_error"
        return fallback


def _parse_query_fallback(query: str, data_summary: dict) -> dict:
    """Fallback query parsing using keyword matching."""
    query_lower = query.lower()
    
    # Detect intent from keywords
    intent = "general"
    if any(word in query_lower for word in ["insight", "key finding", "important", "summary"]):
        intent = "insights"
    elif any(phrase in query_lower for phrase in ["what is this data about", "what is data about", "describe the data", "tell me about this data", "about this dataset", "dataset description"]):
        intent = "general"
    elif any(word in query_lower for word in ["correlation", "relationship", "related"]):
        intent = "correlations"
    elif any(word in query_lower for word in ["pattern", "trend", "seasonal", "periodic"]):
        intent = "patterns"
    elif any(word in query_lower for word in ["outlier", "anomaly", "unusual", "extreme"]):
        intent = "outliers"
    elif any(word in query_lower for word in ["sum", "total", "average", "mean", "count"]):
        intent = "aggregate"
    elif any(word in query_lower for word in ["top", "highest", "maximum", "largest"]):
        intent = "top_bottom"
    elif any(word in query_lower for word in ["bottom", "lowest", "minimum", "smallest"]):
        intent = "top_bottom"
    elif any(word in query_lower for word in ["trend", "over time", "change"]):
        intent = "trend"
    elif any(word in query_lower for word in ["compare", "difference", "between"]):
        intent = "compare"
    elif any(word in query_lower for word in ["show", "display", "list", "distribution"]):
        intent = "distribution"
    
    # Detect operation
    operation = "count"
    if "sum" in query_lower or "total" in query_lower:
        operation = "sum"
    elif "average" in query_lower or "mean" in query_lower:
        operation = "average"
    elif "max" in query_lower or "highest" in query_lower:
        operation = "max"
    elif "min" in query_lower or "lowest" in query_lower:
        operation = "min"
    
    # Extract mentioned columns
    mentioned_columns = []
    for col in data_summary.get("columns", []):
        if col.lower() in query_lower:
            mentioned_columns.append(col)
    
    # Suggest chart type
    chart_type = "table"
    if intent in ["top_bottom", "distribution"]:
        chart_type = "bar"
    elif intent == "trend":
        chart_type = "line"
    elif intent == "aggregate":
        chart_type = "table"
    
    return {
        "intent": intent,
        "columns": mentioned_columns,
        "operation": operation,
        "filters": [],
        "group_by": mentioned_columns[0] if len(mentioned_columns) > 0 else None,
        "limit": 10,
        "chart_type": chart_type,
        "_intentSource": "fallback",
    }


def _execute_query_on_data(analytics: dict, intent: dict) -> dict:
    """Execute the parsed query using analytics data."""
    result = {
        "data": [],
        "summary": {},
        "chart_data": None
    }
    
    columns = intent.get("columns", [])
    operation = intent.get("operation", "count")
    limit = intent.get("limit", 10)
    intent_type = intent.get("intent", "general")
    
    # Handle advanced analytics intents
    if intent_type == "insights":
        insights = analytics.get("insights", [])
        result["summary"]["message"] = f"Found {len(insights)} key insights: " + "; ".join(insights[:3])
        result["data"] = insights[:limit]
        return result
    
    elif intent_type == "correlations":
        correlations = analytics.get("correlations", [])
        if columns:
            # Filter by mentioned columns
            filtered = [c for c in correlations if any(col in str(c) for col in columns)]
            correlations = filtered[:limit]
        result["summary"]["message"] = f"Found {len(correlations)} significant correlations"
        result["data"] = correlations[:limit]
        result["chart_data"] = {
            "type": "heatmap",
            "data": correlations[:limit]
        }
        return result
    
    elif intent_type == "patterns":
        pattern_data = []
        for col_name, col_info in analytics.get("columns", {}).items():
            if col_info.get("type") == "numeric" and col_info.get("stats"):
                patterns = col_info["stats"].get("patterns", [])
                if patterns:
                    pattern_data.append({
                        "column": col_name,
                        "patterns": patterns
                    })
        result["summary"]["message"] = f"Detected patterns in {len(pattern_data)} columns"
        result["data"] = pattern_data[:limit]
        return result
    
    elif intent_type == "outliers":
        outlier_data = []
        for col_name, col_info in analytics.get("columns", {}).items():
            if col_info.get("type") == "numeric" and col_info.get("stats"):
                outliers = col_info["stats"].get("zScoreOutliers", [])
                if outliers:
                    outlier_data.append({
                        "column": col_name,
                        "outlier_count": len(outliers),
                        "outliers": outliers[:5]
                    })
        result["summary"]["message"] = f"Found outliers in {len(outlier_data)} columns"
        result["data"] = outlier_data[:limit]
        return result
    
    elif intent_type == "aggregate":
        # Aggregate operations using column stats
        if not columns:
            overview = analytics.get("overview", {})
            result["summary"]["total_rows"] = overview.get("totalRows", 0)
            result["summary"]["total_columns"] = overview.get("totalColumns", 0)
            result["summary"]["message"] = f"Dataset contains {overview.get('totalRows', 0)} rows and {overview.get('totalColumns', 0)} columns"
        else:
            col = columns[0]
            col_info = analytics.get("columns", {}).get(col, {})
            stats = col_info.get("stats", {})
            
            if operation == "sum":
                total = stats.get("sum", 0)
                result["summary"][f"sum_{col}"] = round(total, 2)
                result["summary"]["message"] = f"Sum of {col}: {round(total, 2)}"
            elif operation == "average":
                avg = stats.get("mean", 0)
                result["summary"][f"avg_{col}"] = round(avg, 2)
                result["summary"]["message"] = f"Average of {col}: {round(avg, 2)}"
            elif operation == "max":
                max_val = stats.get("max", 0)
                result["summary"][f"max_{col}"] = round(max_val, 2)
                result["summary"]["message"] = f"Maximum of {col}: {round(max_val, 2)}"
            elif operation == "min":
                min_val = stats.get("min", 0)
                result["summary"][f"min_{col}"] = round(min_val, 2)
                result["summary"]["message"] = f"Minimum of {col}: {round(min_val, 2)}"
            else:
                result["summary"]["message"] = f"Statistics for {col}: Mean={stats.get('mean', 0):.2f}, Std={stats.get('std', 0):.2f}"
    
    elif intent_type == "top_bottom":
        # Top/Bottom values using column stats
        if not columns:
            result["summary"]["message"] = "Please specify a column"
        else:
            col = columns[0]
            col_info = analytics.get("columns", {}).get(col, {})
            top_values = col_info.get("topValues", [])
            
            if top_values:
                result["data"] = top_values[:limit]
                result["summary"]["message"] = f"Top {len(top_values)} values for {col}"
                result["chart_data"] = {
                    "labels": [v.get("value", "")[:20] for v in top_values[:limit]],
                    "values": [v.get("count", 0) for v in top_values[:limit]],
                    "type": intent.get("chart_type", "bar")
                }
            else:
                result["summary"]["message"] = f"No top values found for {col}"
    
    elif intent_type == "distribution":
        # Show distribution of a column
        if not columns:
            result["summary"]["message"] = "Please specify a column"
        else:
            col = columns[0]
            col_info = analytics.get("columns", {}).get(col, {})
            top_values = col_info.get("topValues", [])
            
            if top_values:
                result["data"] = top_values[:limit]
                result["summary"]["message"] = f"Distribution of {col}: {len(top_values)} unique values shown"
                result["chart_data"] = {
                    "labels": [v.get("value", "")[:20] for v in top_values[:limit]],
                    "values": [v.get("count", 0) for v in top_values[:limit]],
                    "type": intent.get("chart_type", "bar")
                }
            else:
                result["summary"]["message"] = f"No distribution data found for {col}"
    
    elif intent_type == "general":
        # General information from overview
        overview = analytics.get("overview", {})
        quality = analytics.get("dataQuality", {})
        columns_info = analytics.get("columns", {})
        inferred_theme = _infer_dataset_theme(list(columns_info.keys()))
        top_category_samples = _top_category_samples(columns_info, max_columns=2, max_values=3)

        result["summary"]["total_rows"] = overview.get("totalRows", 0)
        result["summary"]["total_columns"] = overview.get("totalColumns", 0)
        result["summary"]["columns"] = list(columns_info.keys())
        result["summary"]["completeness"] = quality.get("completeness", 0)
        result["summary"]["numeric_columns"] = overview.get("numericColumns", 0)
        result["summary"]["categorical_columns"] = overview.get("categoricalColumns", 0)
        result["summary"]["datetime_columns"] = overview.get("datetimeColumns", 0)
        result["summary"]["inferred_theme"] = inferred_theme
        result["summary"]["top_category_samples"] = top_category_samples

        message_parts = [
            f"Dataset has {overview.get('totalRows', 0)} rows and {overview.get('totalColumns', 0)} columns",
            f"with {quality.get('completeness', 0)}% data completeness",
            f"({overview.get('numericColumns', 0)} numeric, {overview.get('categoricalColumns', 0)} categorical, {overview.get('datetimeColumns', 0)} datetime).",
        ]
        if inferred_theme:
            message_parts.append(f"It appears to be about {inferred_theme}.")
        if top_category_samples:
            sample_text = "; ".join([f"{k}: {', '.join(v)}" for k, v in top_category_samples.items()])
            message_parts.append(f"Example categories: {sample_text}.")

        result["summary"]["message"] = " ".join(message_parts)
        result["data"] = list(columns_info.keys())[:limit]
    
    return result


def _generate_natural_response(query: str, query_result: dict, data_summary: dict, intent: dict):
    """Generate a natural language response using Bedrock or fallback."""
    bedrock_enabled = os.environ.get("ENABLE_BEDROCK", "false").lower() == "true"
    
    if not bedrock_enabled:
        return _generate_response_fallback(query, query_result, data_summary, intent), "fallback_disabled"
    
    try:
        prompt = f"""Generate a natural, conversational response to this user query about their dataset.

User Query: "{query}"

Query Result:
{json.dumps(query_result.get('summary', {}), indent=2)}

    Top Result Data (if available):
    {json.dumps(query_result.get('data', [])[:5], indent=2)}

Dataset Context:
- Total Rows: {data_summary.get('totalRows', 0)}
- Columns: {', '.join(data_summary.get('columns', []))}

Intent: {intent.get('intent', 'general')}
Operation: {intent.get('operation', 'none')}

Guidelines:
- Be helpful and conversational
- Provide specific numbers and insights
- If data is available, highlight key findings
- For queries like "what is this data about" or "describe the data", explain the likely business/domain context from column names and top categories
- Keep response concise but informative
- Use professional but friendly tone
- If no data is available, suggest what the user could ask instead

Return ONLY the response text, no other formatting."""

        ai_text = _call_bedrock(prompt, max_tokens=512, temperature=0.7)
        
        if ai_text.strip():
            return ai_text.strip(), "bedrock"
        
        return _generate_response_fallback(query, query_result, data_summary, intent), "fallback_empty"
        
    except Exception as e:
        print(f"Bedrock response generation failed: {e}")
        return _generate_response_fallback(query, query_result, data_summary, intent), "fallback_error"


def _generate_response_fallback(query: str, query_result: dict, data_summary: dict, intent: dict) -> str:
    """Fallback response generation."""
    query_lower = (query or "").lower()
    summary = query_result.get("summary", {})

    if any(phrase in query_lower for phrase in ["what is this data about", "what is data about", "describe the data", "tell me about this data", "about this dataset"]):
        inferred_theme = summary.get("inferred_theme")
        cols = data_summary.get("columns", [])
        top_samples = summary.get("top_category_samples", {})

        response_parts = [
            f"This dataset has {data_summary.get('totalRows', 0)} rows across {len(cols)} columns"
        ]
        if inferred_theme:
            response_parts.append(f"and appears to describe {inferred_theme}")
        response_parts[-1] = response_parts[-1] + "."

        if cols:
            response_parts.append(f"Main fields include: {', '.join(cols[:6])}{'...' if len(cols) > 6 else ''}.")

        if top_samples:
            sample_text = "; ".join([f"{k} ({', '.join(v)})" for k, v in top_samples.items()])
            response_parts.append(f"Sample categories suggest: {sample_text}.")

        return " ".join(response_parts)

    if summary.get("message"):
        return summary["message"]
    
    intent_type = intent.get("intent", "general")
    
    if intent_type == "insights":
        return f"I found several key insights in your data. The analysis revealed patterns and relationships that could be valuable for your understanding."
    elif intent_type == "correlations":
        return f"I analyzed the correlations between columns in your dataset. The results show significant relationships that might be worth exploring further."
    elif intent_type == "patterns":
        return f"I detected various patterns in your numeric columns, including trends and periodic behaviors that could indicate underlying patterns in your data."
    elif intent_type == "outliers":
        return f"I identified outliers in your dataset using statistical methods. These unusual values might indicate data quality issues or interesting anomalies worth investigating."
    elif intent_type == "aggregate":
        col = intent.get("columns", ["data"])[0]
        return f"Based on the query about {col}, I've calculated the relevant statistics from your dataset."
    else:
        return f"I analyzed your data based on the query: '{query}'. The dataset contains {data_summary.get('totalRows', 0)} rows with {len(data_summary.get('columns', []))} columns."


def _infer_dataset_theme(columns: list) -> str:
    """Infer a likely dataset theme using common business keywords in column names."""
    if not columns:
        return ""

    joined = " ".join([str(c).lower() for c in columns])
    theme_keywords = [
        ("sales and revenue transactions", ["sales", "revenue", "amount", "price", "quantity", "order", "invoice", "profit"]),
        ("customer and CRM activity", ["customer", "client", "segment", "email", "lead", "account"]),
        ("inventory and product operations", ["product", "sku", "stock", "inventory", "warehouse", "item"]),
        ("financial and accounting records", ["balance", "expense", "cost", "payment", "budget", "ledger"]),
        ("HR and workforce data", ["employee", "department", "salary", "role", "attendance"]),
        ("marketing campaign performance", ["campaign", "channel", "click", "impression", "conversion"]),
    ]

    best_theme = ""
    best_score = 0
    for theme, keywords in theme_keywords:
        score = sum(1 for kw in keywords if kw in joined)
        if score > best_score:
            best_score = score
            best_theme = theme

    if best_score == 0:
        return "a structured business dataset"
    return best_theme


def _top_category_samples(columns_info: dict, max_columns: int = 2, max_values: int = 3) -> dict:
    """Extract compact top-value samples from categorical columns for better descriptions."""
    samples = {}
    for col_name, col_info in columns_info.items():
        if col_info.get("type") != "categorical":
            continue

        top_values = col_info.get("topValues", [])
        if not top_values:
            continue

        values = []
        for entry in top_values[:max_values]:
            value = str(entry.get("value", "")).strip()
            if value:
                values.append(value)

        if values:
            samples[col_name] = values

        if len(samples) >= max_columns:
            break

    return samples


def _generate_follow_up_suggestions(intent: dict, data_summary: dict, analytics: dict) -> list:
    """Generate contextual follow-up suggestions based on the current query."""
    suggestions = []
    intent_type = intent.get("intent", "general")
    columns = data_summary.get("columns", [])
    numeric_columns = data_summary.get("numericColumns", [])
    
    # Base suggestions
    if intent_type == "insights":
        suggestions.extend([
            "What are the correlations?",
            "Show me the patterns",
            "Find outliers"
        ])
    elif intent_type == "correlations":
        suggestions.extend([
            "What are the key insights?",
            "Show trends and patterns",
            "Analyze specific columns"
        ])
    elif intent_type == "patterns":
        suggestions.extend([
            "What are the key insights?",
            "Find outliers",
            "Show correlations"
        ])
    elif intent_type == "outliers":
        suggestions.extend([
            "What are the key insights?",
            "Show data distribution",
            "Analyze patterns"
        ])
    elif intent_type == "aggregate":
        if numeric_columns:
            suggestions.extend([
                f"Show distribution of {numeric_columns[0]}",
                "Find outliers",
                "What are the correlations?"
            ])
    else:
        suggestions.extend([
            "What are the key insights?",
            "Show me data summary",
            "What are the correlations?"
        ])
    
    # Add column-specific suggestions
    if columns and len(columns) > 1:
        suggestions.append(f"Compare {columns[0]} and {columns[1]}")
    
    return suggestions[:5]  # Return top 5 suggestions


def lambda_handler(event, context):
    """Lambda handler for chat queries about data."""
    # Handle CORS preflight OPTIONS request
    if _request_method(event) == "OPTIONS":
        return _response(200, {"ok": True})
    
    table_name = _required_env("TABLE_NAME")
    report_bucket = _required_env("REPORT_BUCKET")
    
    table = dynamodb.Table(table_name)
    
    # Parse event
    path_parameters = event.get("pathParameters") or {}
    request_body = _parse_body(event)

    job_id = path_parameters.get("jobId")
    if not job_id:
        # Try from body
        job_id = request_body.get("jobId")
    
    if not job_id:
        return _response(400, {"error": "jobId is required"})
    
    query = request_body.get("query", "")
    conversation_history = request_body.get("history", [])
    
    if not query:
        return _response(400, {"error": "query is required"})
    
    try:
        # Get job status
        job_response = table.get_item(Key={"jobId": job_id})
        job = job_response.get("Item")
        
        if not job:
            return _response(404, {"error": "Job not found"})
        
        if job.get("status") != "COMPLETED":
            return _response(400, {"error": "Job not completed yet"})
        
        # Get result from S3
        result_key = job.get("resultKey")
        if not result_key:
            return _response(400, {"error": "No result found for this job"})
        
        s3_response = s3_client.get_object(Bucket=report_bucket, Key=result_key)
        result_data = json.loads(s3_response["Body"].read())
        
        # Extract analytics data
        analytics = result_data.get("analytics", result_data)
        columns = analytics.get("columns", {})
        
        # Build data summary for query parsing
        numeric_columns = [k for k, v in columns.items() if v.get("type") == "numeric"]
        categorical_columns = [k for k, v in columns.items() if v.get("type") == "categorical"]
        
        data_summary = {
            "totalRows": analytics.get("overview", {}).get("totalRows", 0),
            "columns": list(columns.keys()),
            "numericColumns": numeric_columns,
            "categoricalColumns": categorical_columns
        }
        
        # Parse query intent with conversation context
        intent = _parse_query_intent(query, data_summary, conversation_history)
        
        # Execute query using analytics data
        query_result = _execute_query_on_data(analytics, intent)
        
        # Generate natural language response
        response_text, response_source = _generate_natural_response(query, query_result, data_summary, intent)
        
        # Generate follow-up suggestions
        follow_up_suggestions = _generate_follow_up_suggestions(intent, data_summary, analytics)
        
        # Generate context suggestions for quick actions
        context_suggestions = []
        if numeric_columns:
            context_suggestions.extend([
                f"Show distribution of {numeric_columns[0]}",
                f"Find outliers in {numeric_columns[0]}",
                f"Analyze {numeric_columns[0]}"
            ])
        if len(numeric_columns) > 1:
            context_suggestions.append(f"Correlation between {numeric_columns[0]} and {numeric_columns[1]}")
        
        return _response(
            200,
            {
                "jobId": job_id,
                "query": query,
                "response": response_text,
                "intent": intent,
                "result": query_result,
                "follow_up_suggestions": follow_up_suggestions,
                "context_suggestions": context_suggestions[:6],
                "ai": {
                    "bedrockEnabled": os.environ.get("ENABLE_BEDROCK", "false").lower() == "true",
                    "modelId": os.environ.get("BEDROCK_MODEL_ID", ""),
                    "intentSource": intent.get("_intentSource", "unknown"),
                    "responseSource": response_source,
                },
                "timestamp": _now_iso()
            },
        )
        
    except Exception as e:
        print(f"Error processing chat query: {e}")
        import traceback
        traceback.print_exc()
        return _response(500, {"error": "Failed to process chat query", "details": str(e)})
