from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_apigateway as apigateway,
    RemovalPolicy
)
from constructs import Construct
import os

class IncomeReportProcessStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Lambda function
        lambda_function = _lambda.Function(
            self, "IncomeReportProcessFunction",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(path=os.path.join(os.path.dirname(__file__), "../../lambda-code")),
            environment={
                # Add environment variables if needed
            }
        )

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
