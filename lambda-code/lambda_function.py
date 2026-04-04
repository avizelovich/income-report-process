import json
import os
import boto3
import csv
import io
from datetime import datetime
from decimal import Decimal

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

# Get environment variables
TABLE_NAME = os.environ.get('EXPENSES_TABLE_NAME')
BUCKET_NAME = os.environ.get('CSV_BUCKET_NAME')

# Initialize DynamoDB table
table = dynamodb.Table(TABLE_NAME)

def lambda_handler(event, context):
    """
    Lambda function for income report processing
    Processes CSV files from S3 and stores them in DynamoDB
    """
    try:
        print(f"Received event: {json.dumps(event)}")
        print(f"Context: {context}")
        
        # Handle S3 event trigger
        if 'Records' in event:
            return handle_s3_event(event, context)
        
        # Handle direct API requests
        return handle_api_request(event, context)
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'error': str(e)})
        }

def handle_s3_event(event, context):
    """Handle S3 file upload events"""
    try:
        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            
            print(f"Processing file: s3://{bucket}/{key}")
            
            # Get CSV file from S3
            response = s3.get_object(Bucket=bucket, Key=key)
            csv_content = response['Body'].read().decode('utf-8')
            
            # Process CSV and store in DynamoDB
            processed_count = process_csv_content(csv_content)
            
            print(f"Successfully processed {processed_count} records from {key}")
            
            # Optionally delete the processed file
            # s3.delete_object(Bucket=bucket, Key=key)
            
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'message': f'Successfully processed {len(event["Records"])} CSV files',
                'processed_records': processed_count
            })
        }
        
    except Exception as e:
        print(f"Error processing S3 event: {str(e)}")
        raise

def handle_api_request(event, context):
    """Handle direct API requests"""
    try:
        # Example: Get expenses from DynamoDB
        response = table.scan(
            Limit=10
        )
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS'
            },
            'body': json.dumps({
                'message': 'Income report processing Lambda is working',
                'expenses': response.get('Items', []),
                'table_name': TABLE_NAME,
                'bucket_name': BUCKET_NAME
            })
        }
        
    except Exception as e:
        print(f"Error handling API request: {str(e)}")
        raise

def process_csv_content(csv_content):
    """Process CSV content and store in DynamoDB"""
    processed_count = 0
    
    # CSV column mapping (Hebrew to English)
    column_mapping = {
        'שם כרטיס': 'card_id',
        'תאריך': 'date_purchase',
        'חיוב לתאריך': 'date_charging',
        'שם בית עסק': 'business_name',
        'סכום חיוב בש''ח': 'payment_current',
        'סכום קנייה': 'payment_total',
        'אסמכתא': 'purchase_id',
        'תאור סוג עסקת אשראי': 'purchase_type'
    }
    
    # Read CSV content
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            # Map Hebrew columns to English
            mapped_data = {}
            for hebrew_col, english_col in column_mapping.items():
                if hebrew_col in row and row[hebrew_col].strip():
                    mapped_data[english_col] = row[hebrew_col].strip()
            
            # Skip if required fields are missing
            if not mapped_data.get('card_id') or not mapped_data.get('purchase_id'):
                print(f"Skipping row - missing required fields: {row}")
                continue
            
            # Clean and format data
            item = {
                'card_id': mapped_data['card_id'],
                'purchase_id': mapped_data['purchase_id'],
                'date_purchase': format_date(mapped_data.get('date_purchase')),
                'date_charging': format_date(mapped_data.get('date_charging')),
                'business_name': mapped_data.get('business_name', ''),
                'payment_current': convert_to_decimal(mapped_data.get('payment_current')),
                'payment_total': convert_to_decimal(mapped_data.get('payment_total')),
                'purchase_type': mapped_data.get('purchase_type', ''),
                'created_at': datetime.utcnow().isoformat(),
                'source_file': 'csv_upload'
            }
            
            # Store in DynamoDB
            table.put_item(Item=item)
            processed_count += 1
            
            print(f"Stored item: {item['card_id']} - {item['purchase_id']}")
            
        except Exception as e:
            print(f"Error processing row {row}: {str(e)}")
            continue
    
    return processed_count

def format_date(date_str):
    """Format date string to ISO format"""
    if not date_str or date_str.strip() == '':
        return None
    
    try:
        # Handle various date formats (DD/MM/YYYY, YYYY-MM-DD, etc.)
        date_str = date_str.strip()
        
        # Try DD/MM/YYYY format
        if '/' in date_str:
            day, month, year = date_str.split('/')
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        
        # Try YYYY-MM-DD format
        if '-' in date_str:
            return date_str
            
        return date_str
        
    except Exception:
        return date_str

def convert_to_decimal(amount_str):
    """Convert amount string to Decimal"""
    if not amount_str or amount_str.strip() == '':
        return None
    
    try:
        # Remove currency symbols, commas, and spaces
        cleaned = amount_str.replace('₪', '').replace('$', '').replace(',', '').strip()
        
        # Convert to Decimal
        return Decimal(cleaned)
        
    except Exception:
        return None
