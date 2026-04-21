# API Specification

Base URL:
`https://<api-id>.execute-api.<region>.amazonaws.com/prod`

## 1) Create Analysis Job
- Method: `POST`
- Path: `/jobs`
- Request Body: `{}`
- Success Response: `201`

```json
{
  "jobId": "3f4a2...",
  "status": "CREATED",
  "upload": {
    "url": "https://...signed-url...",
    "method": "PUT",
    "headers": {
      "Content-Type": "text/csv"
    },
    "key": "uploads/<jobId>.csv",
    "expiresInSeconds": 900
  }
}
```

## 2) Get Job Status
- Method: `GET`
- Path: `/jobs/{jobId}`
- Success Response: `200`

```json
{
  "jobId": "3f4a2...",
  "status": "PROCESSING",
  "createdAt": "2026-04-04T10:11:00.123Z",
  "updatedAt": "2026-04-04T10:11:20.000Z",
  "errorMessage": null,
  "resultKey": null
}
```

## 3) Get Job Result
- Method: `GET`
- Path: `/jobs/{jobId}/result`
- Success Response: `200`

```json
{
  "jobId": "3f4a2...",
  "status": "COMPLETED",
  "result": {
    "jobId": "3f4a2...",
    "generatedAt": "2026-04-04T10:12:00.000Z",
    "stats": {
      "rowCount": 6,
      "totalAmount": 914.95,
      "averageAmount": 152.49,
      "minAmount": 45,
      "maxAmount": 500,
      "topCategories": [
        {"category": "Rent", "total": 500}
      ],
      "monthlyTrend": [
        {"month": "2026-03", "total": 252.75},
        {"month": "2026-04", "total": 662.2}
      ]
    },
    "summary": "Processed 6 rows..."
  }
}
```

Failure behavior:
- `404` when job does not exist.
- `409` when result is requested before completion.
- `500` when data retrieval or processing fails.
