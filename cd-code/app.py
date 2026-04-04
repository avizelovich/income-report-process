#!/usr/bin/env python3
import os

from aws_cdk import App, Environment

from income_report_process.income_report_process_stack import IncomeReportProcessStack

app = App()

IncomeReportProcessStack(app, "IncomeReportProcessStack")

app.synth()
