# ETL Pipeline with AWS Services

## Overview

This project demonstrates how to use AWS Services to automate the ETL (Extract, Transform, Load) process for web-scraped data. The data for this project is sourced from [Lamudi Scraper](https://github.com/njolnir/Lamudi_Scraper).

## Architecture

The ETL pipeline leverages the following AWS services:

- **S3**: Storage for raw and processed data.
- **SNS Topic**: Notification service for triggering events.
- **EventBridge**: Event-driven service to route events.
- **Step Functions**: Orchestrates the workflow of the ETL process.
- **Glue Crawlers**: Catalogs the data stored in S3.
- **Glue Job**: Performs the transformation of the data.
