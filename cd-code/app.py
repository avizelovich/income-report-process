#!/usr/bin/env python3
import os

from aws_cdk import App, Environment

from income_report_process.income_report_process_stack import IncomeReportProcessStack

app = App()

IncomeReportProcessStack(app, "IncomeReportProcessStack", {
    'env': Environment(
        'account': os.environ.get('CDK_DEFAULT_ACCOUNT'),
        'region': os.environ.get('CDK_DEFAULT_REGION'),
    )
})

app.synth()
