import json

def lambda_handler(event, context):
    """
    Lambda function for income report processing
    """
    try:
        print(f"Received event: {json.dumps(event)}")
        print(f"Context: {context}")
        
        response = {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS'
            },
            'body': json.dumps({
                'message': 'Income report processing Lambda is working',
                'timestamp': str(context.aws_request_id) if context else 'unknown'
            })
        }
        
        print(f"Returning response: {json.dumps(response)}")
        return response
        
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
