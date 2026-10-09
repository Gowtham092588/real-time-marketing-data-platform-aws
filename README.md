## Real-Time Marketing Data Platform
 
## 📖 Overview

This project implements an end-to-end marketing data platform that combines Wistia, Calendly, marketing spend, and Salesforce CRM data into a centralized analytics solution.
The platform supports both real-time webhook ingestion and scheduled batch ingestion, processes data using AWS Glue, Lambda, and Databricks, stores curated analytics in Amazon Redshift Serverless, and serves business insights through a Streamlit dashboard.

## 🎯 Business Objective
- Build a centralized view of marketing, booking, CRM, and video engagement data.
- Measure channel performance, spend, bookings, and cost per booking.
- Connect Calendly bookings with CRM leads and funnel activity.
- Analyze employee meeting load and booking patterns.
- Measure Wistia video and visitor engagement.
- Provide cross-platform reporting across spend, bookings, CRM leads, and Wistia activity.

## 🚀 Project Highlights
- Real-time ingestion using API Gateway + AWS Lambda.
- Scheduled batch ingestion using Amazon EventBridge.
- Amazon SQS + DLQ for CRM event decoupling and failure handling.
- Amazon S3 for raw/bronze, source, and target data storage.
- AWS Glue for Wistia ingestion and CRM transformation.
- Databricks for Wistia and Calendly transformations.
- Amazon Redshift Serverless with staging and gold schemas.
- Gold layer includes dimensions, facts, aggregates, views, and stored procedures.
- Streamlit Cloud dashboard for marketing analytics.
- GitHub Actions CI/CD for Glue, Lambda, Databricks, Redshift, and Streamlit.
- Secure deployments using GitHub OIDC, IAM roles, and Databricks service principal.

## 🏛️ Architecture

<img width="1443" height="1096" alt="image" src="https://github.com/user-attachments/assets/d3e8eac8-78a3-4154-993f-2aeb66e6cd89" />

## 🔄 Data Pipeline

# Wistia

Wistia API → EventBridge → AWS Glue → Amazon S3 → Databricks → Redshift
- EventBridge triggers scheduled Wistia API ingestion.
- AWS Glue extracts data and stores it in S3.
- Databricks transforms Wistia data and loads it into Redshift.
  
# Calendly

Calendly Webhook → API Gateway → Lambda → Amazon S3 → Databricks → Redshift
- Calendly webhook events are captured through API Gateway and Lambda.
- Marketing spend is loaded separately through scheduled EventBridge/Lambda ingestion.
- Databricks transforms Calendly and marketing-spend data before loading Redshift.
  
# CRM / Salesforce

Salesforce CRM → API Gateway → Lambda → Source S3 → SQS → Lambda Enrichment → Target S3 → AWS Glue → Redshift
- CRM events are stored as raw files in Source S3.
- Amazon SQS decouples ingestion from enrichment and uses a DLQ for failures.
- Lead enrichment uses reference data from S3.
- AWS Glue transforms CRM data and loads it into Redshift.
  
## 🏗️ Redshift Analytics Layer
Amazon Redshift Serverless uses two main schemas:
- staging — ingested and transformed data before final analytics processing.
- gold — business-ready analytics objects including:
  - Dimension tables
  - Fact tables
  - Aggregate tables
  - Views
  - Stored procedures
- Streamlit queries curated Redshift views for the latest dashboard data.

## 📊 Marketing Analytics Dashboard
The Streamlit dashboard includes:
- Calendly booking and channel analysis
- Cost per booking
- Booking volume by day and time
- Employee meeting load
- Calendly-to-CRM lead analysis
- Wistia video and visitor analysis
- Cross-platform marketing summary

## ⚙️ Orchestration & Monitoring
- Amazon EventBridge schedules batch ingestion.
- Databricks Jobs and Pipelines orchestrate Databricks transformations.
- Amazon SQS / DLQ handles CRM event buffering and failures.
- Amazon CloudWatch provides logging and monitoring.
- AWS Secrets Manager stores secure credentials.

## 🚀 CI/CD
GitHub Actions is used to:
- Validate Python, SQL, and Databricks configuration.
- Deploy AWS Glue scripts.
- Deploy AWS Lambda functions.
- Deploy Databricks Asset Bundles.
- Deploy Redshift SQL objects.
- Validate and deploy Streamlit changes.
- Authenticate securely using OIDC without long-lived deployment credentials.
- Enforce pull-request validation and protected main branch deployment.
  
## 🛠️ Technologies Used
- AWS Lambda
- Amazon API Gateway
- Amazon EventBridge
- Amazon SQS / DLQ
- Amazon S3
- AWS Glue
- Databricks
- Amazon Redshift Serverless
- AWS Secrets Manager
- Amazon CloudWatch
- Streamlit & Plotly
- Python & SQL
- Git, GitHub & GitHub Actions
  
## 🚀 Key Engineering Challenges Solved
- Combined real-time webhook and scheduled batch ingestion in one platform.
- Decoupled CRM processing using SQS and DLQ.
- Built lead enrichment using S3 reference data.
- Standardized Wistia, Calendly, and CRM data for unified analytics.
- Designed staging and gold layers in Redshift.
- Created reusable views for Streamlit reporting.
- Implemented CI/CD across multiple AWS and Databricks services.
- Used OIDC and service identities instead of long-lived deployment secrets.
  
## 🔗 Project Links
- GitHub Repository:
https://github.com/Gowtham092588/real-time-marketing-data-platform-aws.git
- Live Streamlit Dashboard:
https://real-time-marketing-data-platform-awsgit-duxzyyawyeztva58gvcpj.streamlit.app/

## 💻 Author
Gowtham Kethineni

[LinkedIn](https://www.linkedin.com/in/gowtham-kethineni)
