from aws_cdk import (
    Stack,
    RemovalPolicy,
    aws_dynamodb as dynamodb,
    CfnOutput
)
from constructs import Construct

class BusinessCategoryStack(Stack):
    """Stack for business-category DynamoDB table"""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # DynamoDB table for business categories
        business_category_table = dynamodb.Table(
            self, "BusinessCategoryTable",
            table_name="business-category",
            partition_key=dynamodb.Attribute(name="business_name", type=dynamodb.AttributeType.STRING),
            removal_policy=RemovalPolicy.DESTROY,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST
        )

        # Output for other stacks to reference
        self.business_category_table_name = business_category_table.table_name
        
        # CloudFormation output
        CfnOutput(
            self, "BusinessCategoryTableName",
            value=business_category_table.table_name,
            export_name="BusinessCategoryTableName"
        )
        
        CfnOutput(
            self, "BusinessCategoryTableArn",
            value=business_category_table.table_arn,
            export_name="BusinessCategoryTableArn"
        )
