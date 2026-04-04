# Income Report Process

A comprehensive AWS Lambda function for income report processing that reads CSV files from S3 and stores expense data in DynamoDB, deployed using AWS CDK with GitHub Actions CI/CD.

## Functionality

- **CSV Processing**: Reads CSV files from S3 bucket automatically
- **Data Storage**: Stores expense data in DynamoDB with proper schema
- **Hebrew Support**: Maps Hebrew column names to English field names
- **Event-Driven**: Triggered automatically when CSV files are uploaded to S3
- **API Access**: Provides REST API to query stored expenses
- **Data Validation**: Validates and formats dates and currency amounts

## Architecture

- **AWS Lambda**: Python 3.11 runtime with CSV processing
- **DynamoDB**: "expenses" table with GSI for date queries
- **S3 Bucket**: Secure bucket for CSV file storage
- **API Gateway**: REST API for expense queries
- **CDK**: Infrastructure as Code using AWS CDK (Python)
- **GitHub Actions**: Automated deployment on push to main branch

## DynamoDB Schema

### Table: expenses
- **Partition Key**: `card_id` (String)
- **Sort Key**: `purchase_id` (String)

### Columns:
- `card_id` - Card identifier (from "שם כרטיס")
- `date_purchase` - Purchase date (from "תאריך")
- `date_charging` - Charging date (from "חיוב לתאריך")
- `business_name` - Business name (from "שם בית עסק")
- `payment_current` - Current payment amount (from "סכום חיוב בש''ח")
- `payment_total` - Total purchase amount (from "סכום קנייה")
- `purchase_id` - Purchase authorization (from "אסמכתא")
- `purchase_type` - Purchase type description (from "תאור סוג עסקת אשראי")

### Global Secondary Index:
- **DatePurchaseIndex**: `date_purchase` as partition key

## CSV Column Mapping

| Hebrew Column | English Field | Description |
|---------------|--------------|-------------|
| שם כרטיס | card_id | Card identifier |
| תאריך | date_purchase | Purchase date |
| חיוב לתאריך | date_charging | Charging date |
| שם בית עסק | business_name | Business name |
| סכום חיוב בש''ח | payment_current | Current payment (NIS) |
| סכום קנייה | payment_total | Total purchase amount |
| אסמכתא | purchase_id | Purchase authorization |
| תאור סוג עסקת אשראי | purchase_type | Purchase type description |

## Usage

### 1. Upload CSV Files
Upload CSV files with Hebrew headers to the S3 bucket `income-report-expenses-csv`. The Lambda will automatically process them.

### 2. API Endpoints
- **GET** `/` - View recent expenses (last 10 records)
- **POST** `/` - Manual trigger (for testing)

### 3. Sample CSV Format
```csv
שם כרטיס,תאריך,חיוב לתאריך,שם בית עסק,סכום חיוב בש''ח,סכום קנייה,אסמכתא,תאור סוג עסקת אשראי
ויזה כרטיס אשראי,01/04/2026,02/04/2026,חנות מכולת,50.25,45.50,123456,רכישה רגילה
```

## Local Development

### Prerequisites

- Python 3.11+
- AWS CLI configured
- AWS CDK installed: `pip install aws-cdk`

### Setup

1. Install CDK dependencies:
   ```bash
   cd cd-code
   pip install -r requirements.txt
   ```

2. Bootstrap CDK (first time only):
   ```bash
   cdk bootstrap
   ```

3. Deploy locally:
   ```bash
   cdk deploy
   ```

4. To clean up:
   ```bash
   cdk destroy
   ```

## Automated Deployment

This repository uses GitHub Actions for automated deployment:

### Setup Required

1. **AWS Access Keys**: Add to GitHub repository settings
   - `AWS_ACCESS_KEY_ID`
   - `AWS_SECRET_ACCESS_KEY`

### Deployment Process

1. Push changes to `main` branch
2. GitHub Actions workflow triggers automatically
3. CDK synthesizes and deploys the stack
4. S3 bucket, DynamoDB table, and Lambda function become available

## Environment Variables

The Lambda function receives these environment variables:

- `EXPENSES_TABLE_NAME` - Name of the DynamoDB table
- `CSV_BUCKET_NAME` - Name of the S3 bucket for CSV files

## Security Features

- S3 bucket with encryption and versioning
- DynamoDB with point-in-time recovery
- Lambda with least-privilege IAM permissions
- API Gateway with CORS support
- No public access to S3 bucket

## Monitoring

- Lambda logs in CloudWatch
- DynamoDB metrics in CloudWatch
- S3 access logs
- API Gateway access logs
## Deployment Status
- [ ] Deployment in progress...
