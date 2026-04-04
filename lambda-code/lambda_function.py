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
BUSINESS_CATEGORY_TABLE_NAME = os.environ.get('BUSINESS_CATEGORY_TABLE_NAME')

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
            category_value = mapped_data.get('category', '') or get_category_from_business_table(business_name)
            
            # Ensure category is never empty for GSI
            if not category_value or category_value.strip() == '':
                category_value = 'לא סווג'
            
            item = {
                'card_id': mapped_data['card_id'],
                'purchase_id': mapped_data['purchase_id'],
                'date_purchase': format_date(mapped_data.get('date_purchase')),
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
    """Get category from business-category table, or add new business if not found"""
    if not business_name:
        return 'לא סווג'
    
    try:
        # Try to get existing category from business-category table
        response = business_category_table.get_item(
            Key={'business_name': business_name}
        )
        
        if 'Item' in response:
            category = response['Item'].get('category', 'לא סווג')
            print(f"Found existing category for '{business_name}': {category}")
            return category
        else:
            # Business not found in table, categorize it and add to table
            print(f"Business '{business_name}' not found in table, categorizing and adding...")
            category = categorize_business_with_ai(business_name)
            
            # Add to business-category table
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
        return categorize_business_with_ai(business_name)

def categorize_business_with_ai(business_name):
    """Categorize business using AI or rule-based fallback"""
    business_name_lower = business_name.lower().strip()
    
    # Strategy 1: Direct keyword matching (fastest)
    category_mapping = {
        # Food & Restaurants
        'מסעדן': 'מזון תשלומים', 'קפה': 'מזון תשלומים', 'מסעדת': 'מזון תשלומים',
        'שופ': 'מזון תשלומים', 'בית קפה': 'מזון תשלומים', 'רשת': 'מזון תשלומים',
        'בר': 'מזון תשלומים', 'מסעדה': 'מזון תשלומים', 'מאפיה': 'מזון תשלומים',
        'גלידריה': 'מזון תשלומים', 'פיצריה': 'מזון תשלומים', 'סושי': 'מזון תשלומים',
        
        # Gas & Fuel
        'תדלן': 'תחבורה ורכב', 'דלק': 'תחבורה ורכב', 'פז': 'תחבורה ורכב',
        'סונת': 'תחבורה ורכב', 'בנזין': 'תחבורה ורכב', 'חניון': 'תחבורה ורכב',
        'parking': 'תחבורה ורכב', 'חניה': 'תחבורה ורכב',
        
        # Shopping & Retail
        'מכולת': 'קניות ורכישים', 'סופר': 'קניות ורכישים', 'שוק': 'קניות ורכישים',
        'רשת קמח': 'קניות ורכישים', 'איקאי': 'קניות ורכישים', 'ייבוא': 'קניות ורכישים',
        'זאפ': 'קניות ורכישים', 'שופרסל': 'קניות ורכישים', 'מגה': 'קניות ורכישים',
        'ויקטוריה': 'קניות ורכישים', 'סטיילש': 'קניות ורכישים', 'השישי': 'קניות ורכישים',
        'פוקס': 'קניות ורכישים', 'בי וואי': 'קניות ורכישים', 'אופיס': 'קניות ורכישים',
        
        # Banks & Financial
        'בנק': 'בנקאות ופיננסים', 'אשראי': 'בנקאות ופיננסים', 'המרכז': 'בנקאות ופיננסים',
        'כרטיס': 'בנקאות ופיננסים', 'הפועלים': 'בנקאות ופיננסים', 'לאומי': 'בנקאות ופיננסים',
        'דיסקונט': 'בנקאות ופיננסים', 'מזרחי': 'בנקאות ופיננסים',
        
        # Education
        'matific': 'חינוך', 'אוניברסיטה': 'חינוך', 'קורס': 'חינוך', 'ספר': 'חינוך',
        'מכללה': 'חינוך', 'גן': 'חינוך', 'בית ספר': 'חינוך', 'חוג': 'חינוך',
        
        # Entertainment & Streaming
        'netflix': 'פנאי ובידור', 'hot': 'פנאי ובידור', 'yes': 'פנאי ובידור',
        'סטרימינג': 'פנאי ובידור', 'קולנוע': 'פנאי ובידור', 'סרט': 'פנאי ובידור',
        'תיאטרון': 'פנאי ובידור', 'הופעה': 'פנאי ובידור', 'ספורט': 'פנאי ובידור',
        
        # Bills & Utilities
        'חשמל': 'חשבונות ותשלומים', 'מים': 'חשבונות ותשלומים', 'ארנונה': 'חשבונות ותשלומים',
        'טלפון': 'חשבונות ותשלומים', 'אינטרנט': 'חשבונות ותשלומים', 'סלולר': 'חשבונות ותשלומים',
        'בזר': 'חשבונות ותשלומים', 'גז': 'חשבונות ותשלומים', 'ביטוח': 'חשבונות ותשלומים',
        
        # Health
        'רופא': 'בריאות', 'פרמציה': 'בריאות', 'בית חולים': 'בריאות', 'מרפאה': 'בריאות',
        'קופת חולים': 'בריאות', 'מכבי': 'בריאות', 'מאוחדת': 'בריאות', 'לאומית': 'בריאות',
        'ויטמין': 'בריאות', 'תרופה': 'בריאות',
        
        # Transportation
        'אוטובוס': 'תחבורה', 'מונית': 'תחבורה', 'רכבת': 'תחבורה', 'דן': 'תחבורה',
        'אגד': 'תחבורה', 'מטרופולין': 'תחבורה', 'קו': 'תחבורה',
        
        # Home & Maintenance
        'הום סנטר': 'תחזוקה וביטחון', 'אשכול': 'תחזוקה וביטחון', 'צבע': 'תחזוקה וביטחון',
        'חשמלאי': 'תחזוקה וביטחון', 'שרברב': 'תחזוקה וביטחון', 'ניקיון': 'תחזוקה וביטחון',
        
        # Travel
        'מלון': 'נסיעות', 'טיסה': 'נסיעות', 'אל על': 'נסיעות', 'ארקיע': 'נסיעות',
        'השכרת רכב': 'נסיעות', 'בודג': 'נסיעות', 'איירבנדר': 'נסיעות',
    }
    
    # Check exact matches first
    for keyword, category in category_mapping.items():
        if keyword in business_name_lower:
            return category
    
    # Strategy 2: AI categorization for unknown businesses
    if OPENAI_API_KEY:
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
                            'content': '''אתה מומחה לקטגוריית הוצאות פיננסיות בישראל. עליך לסווג את שם העסק לאחת מהקטגוריות הבאות עם דיוק מרבי:

מזון תשלומים - מסעדות, בתי קפה, מזון מהיר, קפיטריות, בתי מרקחת, קפה, ברים, מאפיות, גלידריות
תחבורה ורכב - תחנות דלק, חניונים, תחבורה ציבורית, מוניות, אוטובוסים, רכב, חלפי רכב
קניות ורכישים - סופרמרקטים, מכולות, חנויות נוחות, מרכזי קניות, חנויות בגדים, אלקטרוניקה, רהיטים
תחבורה - תחבורה ציבורית, מוניות, אוטובוסים, רכבות, רכבת ישראל, נמל תעופה
בריאות - בתי מרקחת, רופאים, מרפאות, בתי חולים, קופות חולים, ציוד רפואי, ויטמינים
פנאי ובידור - סטרימינג (נטפליקס, הוט), קולנוע, תיאטרון, ספורט, מועדונים, פארקים, פעילויות פנאי
חשבונות ותשלומים - חשבונות חשמל, מים, ארנונה, טלפון, אינטרנט, סלולר, גז, ביטוח
חינוך - מוסדות חינוך, קורסים, ספרים, ציוד לימוד, חוגים, אוניברסיטאות, גני ילדים
תחזוקה וביטחון - תיקוני בית, חשמלאי, שרברב, ניקיון, חומרי בניין, ציוד ביטחון
נסיעות - מלונות, טיסות, דירות נופש, סוכנויות נסיעות, השכרת רכב
בנקאות ופיננסים - בנקים, עמלות העברה, שירותים פיננסיים, אשראי, כרטיסי אשראי
אחר - כל מה שלא מתאים לקטגוריות האחרות

דוגמאות לסיווג:
- MATIFIC -> חינוך (פלטפורמת למידה)
- NETFLIX.COM -> פנאי ובידור (סטרימינג)
- פז -> תחבורה ורכב (תחנת דלק)
- שופרסל -> קניות ורכישים (סופרמרקט)
- המרכז -> בנקאות ופיננסים (בנק)

החזר רק את שם הקטגוריה המדויק בעברית מהרשימה למעלה.'''
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
    
    # Default category
    return 'אחר'

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
