from aws_cdk import (
    Stack,
    RemovalPolicy,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    Duration,
    Size,
    CfnParameter,
    Fn,
    CfnOutput
)
from constructs import Construct

class LambdaStack(Stack):
    """Stack for Lambda function and related resources"""

    def __init__(self, scope: Construct, construct_id: str, expenses_table_name: str, business_category_table_name: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # S3 bucket for CSV files
        bucket_name = CfnParameter(
            self, "BucketName",
            type="String",
            description="Name of the S3 bucket for CSV files",
            default="income-report-expenses-csv"
        )
        
        bucket = s3.Bucket(
            self, "ExpensesBucket",
            bucket_name=bucket_name.value_as_string,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        # Lambda function
        lambda_function = _lambda.Function(
            self, "IncomeReportProcessLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("../../lambda-code"),
            timeout=Duration.minutes(10),
            memory_size=512,
            environment={
                'TABLE_NAME': expenses_table_name,
                'BUSINESS_CATEGORY_TABLE_NAME': business_category_table_name,
                'BUCKET_NAME': bucket_name.value_as_string
            }
        )

        # Grant Lambda permissions to S3
        bucket.grant_read_write(lambda_function)

        # Grant Lambda permissions to DynamoDB expenses table
        expenses_table_arn = Fn.import_value("ExpensesTableArn")
        lambda_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "dynamodb:GetItem",
                    "dynamodb:PutItem",
                    "dynamodb:UpdateItem",
                    "dynamodb:DeleteItem",
                    "dynamodb:BatchWriteItem",
                    "dynamodb:Scan",
                    "dynamodb:Query"
                ],
                resources=[expenses_table_arn]
            )
        )

        # Grant Lambda permissions to DynamoDB business-category table
        business_category_table_arn = Fn.import_value("BusinessCategoryTableArn")
        lambda_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "dynamodb:GetItem",
                    "dynamodb:PutItem",
                    "dynamodb:UpdateItem",
                    "dynamodb:DeleteItem",
                    "dynamodb:Scan",
                    "dynamodb:Query"
                ],
                resources=[business_category_table_arn]
            )
        )

        # Add S3 trigger for CSV processing
        notification = s3n.LambdaDestination(lambda_function)
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            notification,
            s3.NotificationKeyFilter(suffix=".csv")
        )

        # API Gateway for manual triggering
        api = _lambda.HttpApi(
            self, "IncomeReportApi",
            default_function=lambda_function
        )

        # CloudFormation outputs
        CfnOutput(
            self, "ApiUrl",
            value=api.url,
            export_name="IncomeReportApiUrl"
        )
        
        CfnOutput(
            self, "BucketName",
            value=bucket_name.value_as_string,
            export_name="ExpensesBucketName"
        )
