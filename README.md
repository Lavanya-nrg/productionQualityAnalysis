AWS Glue ETL Job

This repository contains an AWS Glue ETL job script that integrates data from multiple sources, performs data transformations, and writes aggregated reports to Amazon Redshift.
Table of Contents

    Description
    Prerequisites
    Installation
    Usage
    Folder Structure
    License

Description

The AWS Glue ETL job script automates the extraction, transformation, and loading (ETL) process for data stored in Amazon S3. It processes sales data, complaints data, and production logs, performs quality checks, calculates production statistics, and writes three reports to Amazon Redshift.

Key Features:

    Reads data from Amazon S3 using AWS Glue DynamicFrames and Spark DataFrames.
    Parses and flattens JSON columns from production logs.
    Performs data quality checks to ensure data integrity and validity.
    Calculates defective percentages and aggregates production statistics.
    Writes reports to Amazon Redshift based on predefined business logic.

Prerequisites

Before running this ETL job, ensure the following prerequisites are met:

    AWS Glue environment is set up with necessary IAM roles and permissions.
    Amazon Redshift cluster is accessible and configured with appropriate credentials.
    Data sources (sales_table, complaints_table, production_table) are defined in the AWS Glue Data Catalog.
    Python dependencies (pyspark, awsglue) are installed in your AWS Glue environment.

Installation

    Clone this repository to your local environment:

    bash

    git clone https://github.com/Lavanya-nrg/productionQualityAnalysis.git

    Set up your AWS Glue environment and configure AWS credentials.

    Modify the script jobConfig.json with your specific AWS Glue and Redshift configuration parameters.

Usage

To execute the AWS Glue ETL job:

    Upload the modified script and necessary files to an Amazon S3 bucket.

    Run the AWS Glue job using the AWS Management Console or AWS CLI.

    Monitor the job execution and verify the results in Amazon Redshift.

Folder Structure

aws-glue-etl/
│
├── config/
│   ├── jobConfigg.json     
|── sql/               
│   ├── sqlScript
│
├── python/
│   └── glueJobScript.py   
└── README.md               
License

This project is licensed under the MIT License - see the LICENSE file for details.
