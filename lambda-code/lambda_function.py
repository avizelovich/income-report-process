import json
import os
import boto3
import csv
import io
from datetime import datetime, date
from decimal import Decimal

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

# Get environment variables
TABLE_NAME = os.environ.get('TABLE_NAME')
BUCKET_NAME = os.environ.get('BUCKET_NAME')
BUSINESS_CATEGORY_TABLE_NAME = os.environ.get('BUSINESS_CATEGORY_TABLE_NAME')

# Initialize DynamoDB tables
table = dynamodb.Table(TABLE_NAME)
business_category_table = dynamodb.Table(BUSINESS_CATEGORY_TABLE_NAME)

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, date):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return str(obj)
        return super().default(obj)

def lambda_handler(event, context):
    """Main Lambda handler with multiple actions"""
    try:
        # Get action from query string parameters first, then fallback to event body
        action = event.get('queryStringParameters', {}).get('action') or event.get('action', 'process')
        
        if action == 'categorize':
            return handle_categorize_action(event)
        else:
            return handle_process_action(event, context)
            
    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Internal server error',
                'error': str(e)
            })
        }

def handle_categorize_action(event):
    """Handle categorize action - update expenses with categories from business-category table"""
    try:
        print("Starting categorize action...")
        
        # Scan all expense records
        response = table.scan()
        expenses = response.get('Items', [])
        print(f"Found {len(expenses)} expense records to process")
        
        # Statistics
        stats = {
            'total_expenses': len(expenses),
            'already_categorized': 0,
            'updated': 0,
            'no_matching_category': 0,
            'errors': 0
        }
        
        # Process each expense
        for i, expense in enumerate(expenses, 1):
            try:
                # Get current category and details
                current_category = expense.get('category', '').strip()
                business_name = expense.get('business_name', '').strip()
                purchase_id = expense.get('purchase_id', '')
                business_date = expense.get('business_date', '')
                amount = expense.get('payment_current', '')
                
                print(f"Processing item {i}/{len(expenses)}: Purchase ID: {purchase_id}, Amount: {amount}, Business: {business_name}, Current Category: {current_category}")
                
                # Skip if already has a real category (not PENDING and not empty)
                if current_category and current_category.strip() and current_category != 'PENDING' and current_category != 'לא סווג':
                    stats['already_categorized'] += 1
                    print(f"  → Already categorized as: {current_category}")
                    continue
                
                # Skip if no business name
                if not business_name:
                    stats['no_matching_category'] += 1
                    print(f"  → No business name found")
                    continue
                
                # Look up category from business-category table
                category = get_category_from_business_table(business_name)
                
                if category and category != 'PENDING':
                    # Update expense with category
                    response = table.update_item(
                        Key={
                            'purchase_id': purchase_id,
                            'business_date': business_date
                        },
                        UpdateExpression="SET category = :cat",
                        ExpressionAttributeValues={
                            ':cat': category
                        },
                        ReturnValues="UPDATED_NEW"
                    )
                    stats['updated'] += 1
                    print(f"  → Updated category to: {category}")
                else:
                    # Add to business-category table as PENDING if not exists
                    add_business_to_category_table(business_name, 'PENDING')
                    stats['no_matching_category'] += 1
                    print(f"  → No category found, marked as PENDING")
                    
            except Exception as e:
                stats['errors'] += 1
                print(f"  → Error processing expense {purchase_id}: {str(e)}")
        
        # Print summary
        print(f"\n=== CATEGORIZATION SUMMARY ===")
        print(f"Total expenses processed: {stats['total_expenses']}")
        print(f"Already categorized: {stats['already_categorized']}")
        print(f"Updated with new categories: {stats['updated']}")
        print(f"No matching category: {stats['no_matching_category']}")
        print(f"Errors: {stats['errors']}")
        print(f"==============================\n")
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS'
            },
            'body': json.dumps({
                'message': 'Categorization completed successfully',
                'statistics': stats
            }, cls=CustomJSONEncoder)
        }
        
    except Exception as e:
        print(f"Error in categorize action: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS'
            },
            'body': json.dumps({
                'message': 'Error during categorization',
                'error': str(e)
            }, cls=CustomJSONEncoder)
        }

def get_category_from_business_table(business_name):
    """Get category from business-category table with normalized matching"""
    if not business_name:
        return None
    
    try:
        # Normalize business name (trim spaces, consistent casing)
        normalized_business = business_name.strip()
        
        # Try exact match first
        response = business_category_table.get_item(
            Key={'business_name': normalized_business}
        )
        
        if 'Item' in response:
            category = response['Item'].get('category', '').strip()
            if category and category != 'PENDING':
                print(f"Found exact match for '{normalized_business}': {category}")
                return category
        
        # If no exact match, try case-insensitive scan
        scan_response = business_category_table.scan(
            FilterExpression='contains(business_name, :business_name)',
            ExpressionAttributeValues={':business_name': normalized_business}
        )
        
        for item in scan_response.get('Items', []):
            category = item.get('category', '').strip()
            if category and category != 'PENDING':
                print(f"Found partial match for '{normalized_business}': {category}")
                return category
        
        return None
        
    except Exception as e:
        print(f"Error looking up business category for '{business_name}': {str(e)}")
        return None

def add_business_to_category_table(business_name, category):
    """Add business to category table"""
    if not business_name:
        return 'PENDING'
    
    try:
        # Normalize business name (trim spaces, consistent casing)
        normalized_business = business_name.strip()
        
        # Check if business already exists in table
        existing = business_category_table.get_item(
            Key={'business_name': normalized_business}
        )
        
        if 'Item' in existing:
            print(f"Business '{normalized_business}' already exists in table with category: {existing['Item'].get('category', 'PENDING')}")
            return existing['Item'].get('category', 'PENDING')
        
        # Try exact match first
        response = business_category_table.get_item(
            Key={'business_name': normalized_business}
        )
        
        if 'Item' in response:
            category = response['Item'].get('category', '').strip()
            print(f"Found existing category for '{normalized_business}': {category}")
            return category
        
        # If no exact match, try case-insensitive scan
        scan_response = business_category_table.scan(
            FilterExpression='contains(business_name, :business_name)',
            ExpressionAttributeValues={':business_name': normalized_business}
        )
        
        for item in scan_response.get('Items', []):
            category = item.get('category', '').strip()
            if category and category != 'PENDING':
                print(f"Found partial match for '{normalized_business}': {category}")
                return category
        
        # Business not found in table, add as PENDING
        print(f"Business '{normalized_business}' not found in table, adding as PENDING...")
        category = 'PENDING'
        
        # Add to business-category table with PENDING status
        business_category_table.put_item(
            Item={
                'business_name': normalized_business,
                'category': category,
                'created_at': datetime.utcnow().isoformat()
            }
        )
        print(f"Added '{normalized_business}' to business-category table with category: {category}")
        return category
        
    except Exception as e:
        print(f"Error accessing business-category table: {str(e)}")
        return 'PENDING'

def handle_process_action(event, context):
    """Handle original process action - unchanged"""
    try:
        print(f"Received event: {json.dumps(event, cls=CustomJSONEncoder)}")
        print(f"Context: {context}")
        
        # Handle API requests (manual trigger)
        return handle_api_request(event, context)
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'error': str(e)}, cls=CustomJSONEncoder)
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
            }, cls=CustomJSONEncoder)
        }
        
    except Exception as e:
        print(f"Error processing S3 event: {str(e)}")
        raise

def handle_api_request(event, context):
    """Handle API requests and process CSV files"""
    try:
        print(f"API Request received: {json.dumps(event, cls=CustomJSONEncoder)}")
        
        # Check if this is a processing request
        if (event.get('queryStringParameters', {}).get('action') == 'process' or 
            (event.get('httpMethod') == 'GET' and 
             event.get('path', '') == '/process')):
            return process_all_csv_files()
        
        # Return error for unknown actions - don't return expenses data
        return {
            'statusCode': 400,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS'
            },
            'body': json.dumps({
                'message': 'Invalid action. Use action=process or action=categorize',
                'received_action': event.get('queryStringParameters', {}).get('action', 'none'),
                'available_actions': ['process', 'categorize']
            }, cls=CustomJSONEncoder)
        }
        
    except Exception as e:
        print(f"Error handling API request: {str(e)}")
        raise

def process_all_csv_files():
    """Process all CSV files in the S3 bucket"""
    try:
        print(f"Processing CSV files from bucket: {BUCKET_NAME}")
        
        # List all CSV files in bucket
        response = s3.list_objects_v2(Bucket=BUCKET_NAME)
        print(f"S3 response: {json.dumps(response, cls=CustomJSONEncoder)}")
        
        all_objects = response.get('Contents', [])
        print(f"All objects in bucket: {len(all_objects)}")
        
        csv_files = [obj for obj in all_objects if obj['Key'].endswith('.csv')]
        print(f"CSV files found: {len(csv_files)}")
        print(f"CSV file details: {[{'Key': obj['Key'], 'Size': obj['Size']} for obj in csv_files]}")
        
        if not csv_files:
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'message': 'No CSV files found in bucket',
                    'bucket': BUCKET_NAME,
                    'all_objects': [obj['Key'] for obj in all_objects],
                    'debug': 'Check if files have .csv extension'
                }, cls=CustomJSONEncoder)
            }
        
        total_processed = 0
        processed_files = []
        
        for csv_file in csv_files:
            try:
                print(f"Processing file: {csv_file['Key']}")
                
                # Get CSV file from S3
                file_response = s3.get_object(Bucket=BUCKET_NAME, Key=csv_file['Key'])
                csv_content = file_response['Body'].read().decode('utf-8')
                print(f"CSV content length: {len(csv_content)} characters")
                first_500_chars = csv_content[:500].replace('\n', '\\n')
                print(f"First 500 chars: {first_500_chars}")
                
                # Check if file is empty
                if not csv_content.strip():
                    print(f"CSV file {csv_file['Key']} is empty!")
                    processed_files.append({
                        'file': csv_file['Key'],
                        'error': 'File is empty'
                    })
                    continue
                
                # Process CSV and store in DynamoDB
                processed_count = process_csv_content(csv_content)
                total_processed += processed_count
                processed_files.append({
                    'file': csv_file['Key'],
                    'records_processed': processed_count
                })
                
                print(f"Successfully processed {processed_count} records from {csv_file['Key']}")
                
            except Exception as e:
                print(f"Error processing file {csv_file['Key']}: {str(e)}")
                processed_files.append({
                    'file': csv_file['Key'],
                    'error': str(e)
                })
                continue
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'message': f'Successfully processed {len(processed_files)} CSV files',
                'total_records': total_processed,
                'processed_files': processed_files
            }, cls=CustomJSONEncoder)
        }
        
    except Exception as e:
        print(f"Error processing CSV files: {str(e)}")
        raise

def process_csv_content(csv_content):
    """Process CSV content and store in DynamoDB"""
    processed_count = 0
    
    print(f"Starting CSV processing...")
    
    # CSV column mapping (Hebrew to English)
    column_mapping = {
        'שם כרטיס': 'card_id',
        'תאריך': 'date_purchase',
        'חיוב לתאריך': 'date_charging',
        'שם בית עסק': 'business_name',
        'סכום חיוב בש''ח': 'payment_current',
        'סכום קנייה': 'payment_total',
        'אסמכתא': 'purchase_id',
        'תאור סוג עסקת אשראי': 'purchase_type',
        'קטגוריה': 'category'  # New column for category
    }
    
    print(f"Column mapping: {json.dumps(column_mapping)}")
    
    # Read CSV content
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    # Get field names from CSV
    field_names = csv_reader.fieldnames
    print(f"CSV field names: {field_names}")
    
    row_count = 0
    for row in csv_reader:
        row_count += 1
        try:
            print(f"Processing row {row_count}: {json.dumps(row, cls=CustomJSONEncoder)}")
            
            # Map Hebrew columns to English
            mapped_data = {}
            for hebrew_col, english_col in column_mapping.items():
                if hebrew_col in row and row[hebrew_col].strip():
                    mapped_data[english_col] = row[hebrew_col].strip()
                    print(f"Mapped {hebrew_col} -> {english_col}: {row[hebrew_col].strip()}")
            
            print(f"Mapped data: {json.dumps(mapped_data, cls=CustomJSONEncoder)}")
            
            # Skip if required fields are missing
            if not mapped_data.get('card_id') or not mapped_data.get('purchase_id'):
                print(f"Skipping row {row_count} - missing required fields. card_id: {mapped_data.get('card_id')}, purchase_id: {mapped_data.get('purchase_id')}")
                continue
            
            # Clean and format data
            business_name = mapped_data.get('business_name', '')
            category_value = mapped_data.get('category', '') or get_category_from_business_table(business_name)
            date_purchase = format_date(mapped_data.get('date_purchase'))
            purchase_id = mapped_data.get('purchase_id', '')
            card_id = mapped_data.get('card_id', '')
            
            # Ensure category is never empty for GSI
            if not category_value or category_value.strip() == '':
                category_value = 'לא סווג'
            
            # Create composite sort key from business_name and date_purchase
            business_date = f"{business_name}#{date_purchase}"
            
            # Check if item already exists
            try:
                existing_item = table.get_item(
                    Key={
                        'purchase_id': purchase_id,
                        'business_date': business_date
                    }
                )
                
                if 'Item' in existing_item:
                    # Item exists, update category if different
                    existing_category = existing_item['Item'].get('category', 'לא סווג')
                    if existing_category != category_value:
                        print(f"Updating existing item {purchase_id} category from '{existing_category}' to '{category_value}'")
                        table.update_item(
                            Key={
                                'purchase_id': purchase_id,
                                'business_date': business_date
                            },
                            UpdateExpression='set #category = :category, updated_at = :updated_at',
                            ExpressionAttributeNames={
                                '#category': 'category'
                            },
                            ExpressionAttributeValues={
                                ':category': category_value,
                                ':updated_at': datetime.utcnow().isoformat()
                            }
                        )
                        processed_count += 1
                    else:
                        print(f"Item {purchase_id} already exists with same category, skipping")
                    continue
                    
            except Exception as e:
                print(f"Error checking existing item: {str(e)}")
            
            # Create new item
            item = {
                'purchase_id': purchase_id,
                'business_date': business_date,
                'card_id': card_id,
                'date_purchase': date_purchase,
                'date_charging': format_date(mapped_data.get('date_charging')),
                'business_name': business_name,
                'payment_current': convert_to_decimal(mapped_data.get('payment_current')),
                'payment_total': convert_to_decimal(mapped_data.get('payment_total')),
                'purchase_type': mapped_data.get('purchase_type', ''),
                'category': category_value,  # Ensure category is never empty
                'created_at': datetime.utcnow().isoformat(),
                'source_file': 'csv_upload'
            }
            
            print(f"Final item to store: {json.dumps(item, cls=CustomJSONEncoder)}")
            
            # Store in DynamoDB
            table.put_item(Item=item)
            processed_count += 1
            
            print(f"Stored item: {item['card_id']} - {item['purchase_id']}")
            
        except Exception as e:
            print(f"Error processing row {row_count} {row}: {str(e)}")
            continue
    
    print(f"CSV processing complete. Total rows: {row_count}, Processed: {processed_count}")
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

def get_category_from_business_table(business_name):
    """Get category from business-category table, or add new business as PENDING if not found"""
    if not business_name:
        return 'לא סווג'
    
    try:
        # Try to get existing category from business-category table
        response = business_category_table.get_item(
            Key={'business_name': business_name}
        )
        
        if 'Item' in response:
            category = response['Item'].get('category', 'לא סווג')
            if not category or category.strip() == '':
                print(f"Business '{business_name}' found but has empty category, marking as PENDING")
                # Update with PENDING category
                business_category_table.update_item(
                    Key={'business_name': business_name},
                    UpdateExpression='set #category = :category',
                    ExpressionAttributeValues={':category': 'PENDING'}
                )
                return 'PENDING'
            
            print(f"Found existing category for '{business_name}': {category}")
            return category
        else:
            # Business not found in table, add as PENDING
            print(f"Business '{business_name}' not found in table, adding as PENDING...")
            category = 'PENDING'
            
            # Add to business-category table with PENDING status
            business_category_table.put_item(
                Item={
                    'business_name': business_name,
                    'category': category,
                    'created_at': datetime.utcnow().isoformat()
                }
            )
            print(f"Added '{business_name}' to business-category table with category: {category}")
            return category
            
    except Exception as e:
        print(f"Error accessing business-category table: {str(e)}")
        return 'PENDING'


def convert_to_decimal(amount_str):
    """Convert amount string to Decimal and return as string"""
    if not amount_str or amount_str.strip() == '':
        return None
    
    try:
        # Remove currency symbols, commas, and spaces
        cleaned = amount_str.replace('₪', '').replace('$', '').replace(',', '').strip()
        
        # Convert to Decimal and then to string
        return str(Decimal(cleaned))
        
    except Exception:
        return None
