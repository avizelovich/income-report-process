from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as _lambda,
    aws_lambda_event_sources as _lambda_event_sources,
    aws_apigateway as apigateway,
    aws_dynamodb as dynamodb,
    aws_s3 as s3,
    aws_iam as iam,
    RemovalPolicy
)
from constructs import Construct
import os

class IncomeReportProcessStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # DynamoDB table for expenses
        expenses_table = dynamodb.Table(
            self, "ExpensesTable",
            table_name="expenses",
            partition_key=dynamodb.Attribute(
                name="card_id",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="purchase_id",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY if os.environ.get('CDK_ENV') == 'dev' else RemovalPolicy.RETAIN,
            point_in_time_recovery=True
        )

        # Add GSI for date-purchase queries
        expenses_table.add_global_secondary_index(
            index_name="DatePurchaseIndex",
            partition_key=dynamodb.Attribute(
                name="date_purchase",
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )

        # Add GSI for category queries
        expenses_table.add_global_secondary_index(
            index_name="CategoryIndex",
            partition_key=dynamodb.Attribute(
                name="category",
                type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL
        )

        # S3 bucket for CSV files (use existing bucket)
        csv_bucket = s3.Bucket.from_bucket_name(
            self, "ExpensesCsvBucket",
            "income-report-expenses-csv"
        )

        # Lambda function
        lambda_function = _lambda.Function(
            self, "IncomeReportProcessFunction",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(path=os.path.join(os.path.dirname(__file__), "../../lambda-code")),
            environment={
                'EXPENSES_TABLE_NAME': expenses_table.table_name,
                'CSV_BUCKET_NAME': csv_bucket.bucket_name
            },
            timeout=Duration.minutes(5),
            memory_size=256
        )

        # Grant permissions to Lambda
        expenses_table.grant_read_write_data(lambda_function)
        csv_bucket.grant_read(lambda_function)
        csv_bucket.grant_delete(lambda_function)

        # Add S3 event trigger for CSV files
        s3_trigger = _lambda_event_sources.S3EventSource(
            bucket=csv_bucket.node.default_child,
            events=[s3.EventType.OBJECT_CREATED],
            filters=[s3.NotificationKeyFilter(prefix="", suffix=".csv")]
        )
        lambda_function.add_event_source(s3_trigger)

        # API Gateway
        api = apigateway.RestApi(
            self, "IncomeReportProcessApi",
            rest_api_name="Income Report Process API",
            description="API for Income Report Process",
            deploy_options=apigateway.StageOptions(
                stage_name="prod"
            ),
            default_cors_preflight_options=apigateway.CorsOptions(
                allow_origins=apigateway.Cors.ALL_ORIGINS,
                allow_methods=apigateway.Cors.ALL_METHODS,
                allow_headers=apigateway.Cors.DEFAULT_HEADERS,
            )
        )

        # Create a specific resource and method instead of proxy
        lambda_integration = apigateway.LambdaIntegration(lambda_function)
        
        # Add GET method to root
        api.root.add_method(
            "GET", 
            lambda_integration,
            api_key_required=False
        )
        
        # Add POST method to root
        api.root.add_method(
            "POST", 
            lambda_integration,
            api_key_required=False
        )
