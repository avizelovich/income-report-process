from aws_cdk import (
    Stack,
    RemovalPolicy,
    aws_dynamodb as dynamodb,
    CfnOutput,
    Fn
)
from constructs import Construct

class ExpensesStack(Stack):
    """Stack for expenses DynamoDB table"""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # DynamoDB table for expenses
        expenses_table = dynamodb.Table(
            self, "ExpensesTable",
            table_name="expenses",
            partition_key=dynamodb.Attribute(
                name="purchase_id",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="business_date",
                type=dynamodb.AttributeType.STRING
            ),
            removal_policy=RemovalPolicy.DESTROY,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST
        )

        # Add Global Secondary Index for category
        expenses_table.add_global_secondary_index(
            index_name="CategoryIndex",
            partition_key=dynamodb.Attribute(name="category", type=dynamodb.AttributeType.STRING),
            projection_type=dynamodb.ProjectionType.ALL
        )

        # Add Global Secondary Index for card_id (for backward compatibility)
        expenses_table.add_global_secondary_index(
            index_name="CardIndex",
            partition_key=dynamodb.Attribute(name="card_id", type=dynamodb.AttributeType.STRING),
            projection_type=dynamodb.ProjectionType.ALL
        )

        # Output for other stacks to reference
        self.expenses_table_name = expenses_table.table_name
        
        # CloudFormation output
        CfnOutput(
            self, "ExpensesTableName",
            value=expenses_table.table_name,
            export_name="ExpensesTableName"
        )
        
        CfnOutput(
            self, "ExpensesTableArn",
            value=expenses_table.table_arn,
            export_name="ExpensesTableArn"
        )
