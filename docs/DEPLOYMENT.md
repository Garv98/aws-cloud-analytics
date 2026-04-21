# Deployment

## A. Free Tier + Safety Setup
1. Create AWS account from the AWS Free Tier page.
2. Enable MFA for root account.
3. Create one IAM admin user and use that account daily.
4. Keep all resources in one region (example: us-east-1).

## B. Install Required Tools
1. AWS CLI v2
2. AWS SAM CLI
3. Configure credentials:

```powershell
aws configure
```

Provide access key, secret key, default region, and output format `json`.

## C. Build and Deploy Backend
From workspace root:

```powershell
sam build --template-file infrastructure/template.yaml
sam deploy --guided --template-file infrastructure/template.yaml
```

Recommended guided answers:
- Stack Name: `nimbus-insights`
- AWS Region: your chosen region
- Confirm changes before deploy: `Y`
- Allow SAM to create IAM roles: `Y`
- Save arguments to samconfig.toml: `Y`

After deploy, copy output `ApiUrl`.

## D. Configure Frontend
1. Open `frontend/config.js`
2. Set:

```javascript
window.APP_CONFIG = {
  apiBaseUrl: "https://<api-id>.execute-api.<region>.amazonaws.com/prod",
};
```

## E. Host Frontend on S3 (Static Website)
1. Create a new S3 bucket for frontend hosting.
2. Enable Static website hosting for that bucket.
3. Disable "Block all public access" only for this frontend bucket.
4. Add bucket policy to allow `s3:GetObject`.
5. Upload all files from `frontend/`.

Or with CLI:

```powershell
aws s3 sync frontend s3://<your-frontend-bucket> --delete
```

## F. Verify End-to-End
1. Open frontend URL from S3 static hosting.
2. Upload `samples/valid.csv`.
3. Confirm status moves to `COMPLETED`.
4. Confirm summary and stats are visible.

## G. Optional Bedrock AI Mode
By default, deterministic summary is used.
To enable Bedrock:
1. Open Lambda `ProcessUploadFunction`.
2. Set env var `ENABLE_BEDROCK=true`.
3. Keep `BEDROCK_MODEL_ID=amazon.titan-text-lite-v1` (or your approved model).
4. Ensure region has Bedrock access and permissions for `bedrock:InvokeModel`.
