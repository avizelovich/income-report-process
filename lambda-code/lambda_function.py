import json
import os
import boto3
import csv
import io
import requests  # For OpenAI API
from datetime import datetime, date
from decimal import Decimal

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')
secrets_manager = boto3.client('secretsmanager')

# Get environment variables
TABLE_NAME = os.environ.get('EXPENSES_TABLE_NAME')
BUCKET_NAME = os.environ.get('CSV_BUCKET_NAME')

# Get OpenAI API key from Secrets Manager
OPENAI_API_KEY = None
try:
    secret_response = secrets_manager.get_secret_value(
        SecretId='openai-api-key'
    )
    secret_data = json.loads(secret_response['SecretString'])
    OPENAI_API_KEY = secret_data.get('api-key')
    print(f"Successfully retrieved OpenAI API key from Secrets Manager")
except Exception as e:
    print(f"Error retrieving OpenAI API key from Secrets Manager: {str(e)}")
    print("Will use rule-based categorization as fallback")

# Initialize DynamoDB table
table = dynamodb.Table(TABLE_NAME)

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
    """
    Lambda function for income report processing
    Processes CSV files from S3 and stores them in DynamoDB
    Can be triggered manually or via API Gateway
    """
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
        
        # Default: Get expenses from DynamoDB
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
                'bucket_name': BUCKET_NAME,
                'instructions': {
                    'process_csv': 'Add ?action=process to process all CSV files in bucket',
                    'process_csv_alt': 'GET /process to process all CSV files',
                    'upload_csv': f'Upload CSV files to s3://{BUCKET_NAME}/'
                }
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
            category_value = mapped_data.get('category', '') or categorize_by_business_name(business_name)
            
            item = {
                'card_id': mapped_data['card_id'],
                'purchase_id': mapped_data['purchase_id'],
                'date_purchase': format_date(mapped_data.get('date_purchase')),
                'date_charging': format_date(mapped_data.get('date_charging')),
                'business_name': business_name,
                'payment_current': convert_to_decimal(mapped_data.get('payment_current')),
                'payment_total': convert_to_decimal(mapped_data.get('payment_total')),
                'purchase_type': mapped_data.get('purchase_type', ''),
                'category': category_value,  # Auto-categorized if empty
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

def categorize_by_business_name(business_name):
    """Enhanced categorization with multiple strategies"""
    if not business_name:
        return 'לא סווג'  # Default category
    
    business_name = business_name.lower().strip()
    
    # Strategy 1: Direct keyword matching (fastest)
    category_mapping = {
        # Food & Restaurants
        'מסעדן': 'מזון תשלומים', 'קפה': 'מזון תשלומים', 'מסעדת': 'מזון תשלומים',
        'שופ': 'מזון תשלומים', 'בית קפה': 'מזון תשלומים', 'רשת': 'מזון תשלומים',
        'בר': 'מזון תשלומים', 'קפה': 'מזון תשלומים',
        
        # Gas & Fuel
        'תדלן': 'תחבורה ורכב', 'דלק': 'תחבורה ורכב', 'פז': 'תחבורה ורכב',
        'סונת': 'תחבורה ורכב', 'בנזין': 'תחבורה ורכב',
        
        # Shopping & Retail
        'מכולת': 'קניות ורכישים', 'סופר': 'קניות ורכישים', 'שוק': 'קניות ורכישים',
        'רשת קמח': 'קניות ורכישים', 'איקאי': 'קניות ורכישים', 'ייבוא': 'קניות ורכישים',
        'זאפ': 'קניות ורכישים',
        
        # Banks & Financial
        'בנק': 'בנקאות ופיננסים', 'אשראי': 'בנקאות ופיננסים', 'המרכז': 'בנקאות ופיננסים',
        'כרטיס': 'בנקאות ופיננסים',
    }
    
    # Check exact matches first
    for keyword, category in category_mapping.items():
        if keyword in business_name:
            return category
    
    # Strategy 2: Partial matching (fuzzy)
    if any(word in business_name for word in ['מסעד', 'אוכל', 'קפה']):
        return 'מזון תשלומים'
    elif any(word in business_name for word in ['תדל', 'דלק', 'פז']):
        return 'תחבורה ורכב'
    elif any(word in business_name for word in ['מכול', 'סופר', 'שוק', 'רשת']):
        return 'קניות ורכישים'
    elif any(word in business_name for word in ['אוטובוס', 'מונית', 'רכב']):
        return 'תחבורה'
    elif any(word in business_name for word in ['רופא', 'בית חולים', 'פרמציה']):
        return 'בריאות'
    elif any(word in business_name for word in ['בנק', 'אשראי']):
        return 'בנקאות ופיננסים'
    
    # Strategy 3: Pattern matching (sophisticated)
    import re
    
    # Restaurant patterns
    if re.search(r'(מסעד|קפה|שופ|בית.*קפה|רשת|בר|קפה)', business_name):
        return 'מזון תשלומים'
    
    # Gas station patterns  
    elif re.search(r'(תדל|דלק|פז|סונת|בנז)', business_name):
        return 'תחבורה ורכב'
    
    # Shopping patterns
    elif re.search(r'(מכול|סופר|שוק|רשת.*קמח|איקאי|ייבוא|זאפ)', business_name):
        return 'קניות ורכישים'
    
    # Transportation patterns
    elif re.search(r'(אוטובוס|מונית|רכב|אוטובוס)', business_name):
        return 'תחבורה'
    
    # Health patterns
    elif re.search(r'(רופא|בית.*חולים|פרמציה|מרפאה|קופת.*חולים)', business_name):
        return 'בריאות'
    
    # Bank patterns
    elif re.search(r'(בנק|אשראי|המרכז|כרטיס)', business_name):
        return 'בנקאות ופיננסים'
    
    # Strategy 4: AI/ML (OpenAI integration)
    if OPENAI_API_KEY and business_name:
        try:
            response = requests.post(
                'https://api.openai.com/v1/chat/completions',
                headers={
                    'Authorization': f'Bearer {OPENAI_API_KEY}',
                    'Content-Type': 'application/json'
                },
                json={
                    'model': 'gpt-3.5-turbo',
                    'messages': [
                        {
                            'role': 'system', 
                            'content': '''אתה עוזר במערכת לקטגור את העסקות שלי. עליך לסווג את שם העסק לאחת מהקטגוריות הבאות:

מזון תשלומים - מסעדות, מסעדות, בתי קפה, קפה, מסעדות, קפה, רשת, בר, קפה
תחבורה ורכב - תחניות, דלק, פז, סונת, בנזין
קניות ורכישים - מכולת, סופר, שוק, רשת קמח, איקאי, ייבוא, זאפ
תחבורה - אוטובוס, מונית, רכב, אוטובוסיון, מוניות
בריאות - רופא, בית חולים, פרמציה, מרפאה, קופת חולים
פנאי ובידור - קולנוע, סרט, בית קולנוע, ספורט, פארק
חשבונות ותשלומים - חשבון, בזר, סלולר, אינטרנט, הוטל
חינוך - אוניברסיטה, ספריה, קורס, מכללה
תחזוקה וביטחון - הום סעוד, רהטנות, בניין, חומרי, חומר, צבע
נסיעות - מלון, בית מלון, טיסה, איירבנדר
בנקאות ופיננסים - בנק, אשראי, המרכז, כרטיס
אחר - אחר

החזיר את הקטגוריה המתאימה לפי העסקות הללו. החזיר רק את השם המדויק והחזיר את הקטגוריה המתאימה (בעברית: מזון תשלומים, תחבורה ורכב, קניות ורכישים, בריאות, פנאי ובידור, חשבונות ותשלומים, חינוך, תחזוקה וביטחון, נסיעות, בנקאות ופיננסים, אחר).'''
                        },
                        {
                            'role': 'user', 
                            'content': f'שם העסק: {business_name}'
                        }
                    ],
                    'max_tokens': 50,
                    'temperature': 0.1
                }
            )
            
            if response.status_code == 200:
                ai_category = response.json()['choices'][0]['message']['content'].strip()
                print(f"AI categorized '{business_name}' as '{ai_category}'")
                
                # Map AI response to our standard categories
                ai_mapping = {
                    'מזון תשלומים': 'מזון תשלומים',
                    'תחבורה ורכב': 'תחבורה ורכב',
                    'קניות ורכישים': 'קניות ורכישים',
                    'תחבורה': 'תחבורה',
                    'בריאות': 'בריאות',
                    'פנאי ובידור': 'פנאי ובידור',
                    'חשבונות ותשלומים': 'חשבונות ותשלומים',
                    'חינוך': 'חינוך',
                    'תחזוקה וביטחון': 'תחזוקה וביטחון',
                    'נסיעות': 'נסיעות',
                    'בנקאות ופיננסים': 'בנקאות ופיננסים',
                    'אחר': 'אחר'
                }
                
                return ai_mapping.get(ai_category, 'אחר')
            
        except Exception as e:
            print(f"AI categorization failed: {str(e)}")
            return 'אחר'  # Default if AI fails

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
