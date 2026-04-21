
# Nimbus Insights

**AI-powered analytics for CSV/Excel data, fully serverless on AWS.**

---

##  What is this?
Nimbus Insights lets you upload a CSV/XLSX/XLS file and instantly get:
- Clean, modern web UI (S3/CloudFront)
- Automated stats, charts, and AI-generated insights
- Chatbot assistant for Q&A about your data
- 100% serverless: Lambda, API Gateway, DynamoDB, S3

---

## Most Important Files (for evaluation)

- **frontend/**
	- `index.html` – Main web UI
	- `app.js` – UI logic, analytics rendering, chat
	- `styles.css` – Modern responsive styles
	- `config.js` – API endpoint config (edit after deploy)
- **backend/functions/**
	- `create_job/lambda_function.py` – Creates analysis jobs, presigned S3 upload
	- `process_upload/lambda_function.py` – Parses file, computes analytics, writes results
	- `get_job/lambda_function.py` – Job status API
	- `get_result/lambda_function.py` – Fetches analysis results
	- `chat_query/lambda_function.py` – Handles chat/AI assistant queries
- **infrastructure/template.yaml** – AWS SAM/CloudFormation template (all resources)
- **samples/valid.csv** – Example input file
- **deploy.ps1** – One-command deployment (Windows)

---

## ⚡ Quick Start

```powershell
# Deploy backend & frontend (Windows)
.\deploy.ps1
```
- Copy API URL from output.
- Edit `frontend/config.js` with your API URL if not auto-configured.

---

## 🖥️ Demo Flow

1. Open `frontend/index.html` (hosted on S3/CloudFront).
2. Upload a sample CSV (see `samples/valid.csv`).
3. View instant analytics, charts, and chat with the AI assistant.

---

## 📄 Documentation

- [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) – Full deployment guide
- [docs/API_SPEC.md](docs/API_SPEC.md) – API reference

---

## 🛡️ Safety & Best Practices

- ⚠️ Do not upload personal/sensitive data
- 🔒 Keep all buckets private except static frontend bucket
- 💰 Set up AWS billing alarms before deploying
- 🧹 Clean up resources after testing to avoid charges
