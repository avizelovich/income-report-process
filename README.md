# Income Report Process

A simple AWS Lambda function for income report processing, deployed using AWS CDK with GitHub Actions CI/CD.

## Functionality

- Receives HTTP requests (GET, POST)
- Processes income report data
- Returns JSON responses with CORS support
- No external dependencies

## Architecture

- **AWS Lambda**: Python 3.11 runtime
- **API Gateway**: REST API with CORS
- **CDK**: Infrastructure as Code using AWS CDK (Python)
- **GitHub Actions**: Automated deployment on push to main branch

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
4. API Gateway endpoint becomes available

## Usage

Once deployed, the Lambda function will respond to HTTP requests:

```json
{
  "statusCode": 200,
  "headers": {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS"
  },
  "body": "{\"message\": \"Income report processing Lambda is working\", \"timestamp\": \"request-id-here\"}"
}
```

The API Gateway endpoint will be available after deployment. Check the CDK outputs or AWS Console for the URL.
