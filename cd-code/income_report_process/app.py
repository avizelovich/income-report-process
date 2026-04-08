#!/usr/bin/env python3
import os
import aws_cdk as cdk
from aws_cdk import (
    App,
    StackProps,
    Environment
)

from business_category_stack import BusinessCategoryStack
from expenses_stack import ExpensesStack
from lambda_stack import LambdaStack

app = App()

# Stack 1: Business Category Table
business_category_stack = BusinessCategoryStack(
    app, "BusinessCategoryStack",
    env=Environment(
        account=os.getenv('CDK_DEFAULT_ACCOUNT'),
        region=os.getenv('CDK_DEFAULT_REGION')
    )
)

# Stack 2: Expenses Table
expenses_stack = ExpensesStack(
    app, "ExpensesStack",
    env=Environment(
        account=os.getenv('CDK_DEFAULT_ACCOUNT'),
        region=os.getenv('CDK_DEFAULT_REGION')
    )
)

# Stack 3: Lambda Function and Resources
lambda_stack = LambdaStack(
    app, "LambdaStack",
    expenses_table_name=expenses_stack.expenses_table_name,
    business_category_table_name=business_category_stack.business_category_table_name,
    env=Environment(
        account=os.getenv('CDK_DEFAULT_ACCOUNT'),
        region=os.getenv('CDK_DEFAULT_REGION')
    )
)

app.synth()
