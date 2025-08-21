# Setting Up Power BI for Cricket Analytics

This guide will help you set up Power BI to visualize the data from our Cricket Analytics system.

## Prerequisites

1. Download and install [Power BI Desktop](https://powerbi.microsoft.com/en-us/desktop/)
2. Make sure the Cricket Analytics API is running

## Steps to Connect Power BI to the API

1. Open Power BI Desktop
2. Click on "Get Data" in the Home ribbon
3. Select "Web" as the data source
4. Enter the API URL: `http://localhost:5000/api/export/powerbi?type=all&hours=24`
5. Click "OK"
6. If prompted, select "Anonymous" for authentication
7. In the Navigator window, select the data you want to import
8. Click "Load"

## Creating Visualizations

### 1. Viewership Dashboard

#### Total Viewers Card
1. Drag the "Card" visualization from the Visualizations pane
2. Drag the "total_viewers" field to the "Fields" section

#### Viewership by Region
1. Drag the "Stacked Column Chart" visualization
2. Add "region" to the Axis
3. Add "viewers" to the Values

#### Viewership Over Time
1. Drag the "Line Chart" visualization
2. Add "timestamp" to the Axis
3. Add "total_viewers" to the Values

### 2. Sentiment Analysis Dashboard

#### Sentiment Score Card
1. Drag the "Card" visualization
2. Add "compound" to the Fields

#### Sentiment Trends
1. Drag the "Line Chart" visualization
2. Add "timestamp" to the Axis
3. Add "positive", "neutral", and "negative" to the Values

#### Sentiment by Region
1. Drag the "Clustered Column Chart" visualization
2. Add "region" to the Axis
3. Add "positive" to the Values

### 3. Ad Performance Dashboard

#### CTR Card
1. Drag the "Card" visualization
2. Add "ctr" to the Fields

#### Ad Performance by Type
1. Drag the "Clustered Column Chart" visualization
2. Add "type" to the Axis
3. Add "impressions", "clicks", and "conversions" to the Values

#### Ad Revenue Over Time
1. Drag the "Line Chart" visualization
2. Add "timestamp" to the Axis
3. Add "cost" to the Values

## Setting Up Automatic Refresh

1. Click on "Transform Data" in the Home ribbon
2. Click on "Data Source Settings"
3. Select your data source and click "Edit Permissions"
4. Check "Refresh automatically" and set the refresh interval
5. Click "OK"

## Publishing to Power BI Service

1. Click on "Publish" in the Home ribbon
2. Sign in to your Power BI account
3. Select a workspace to publish to
4. Click "Publish"

## Setting Up a Dashboard

1. In Power BI Service, go to your workspace
2. Click on "Create" and select "Dashboard"
3. Name your dashboard "Cricket Analytics Dashboard"
4. Pin visualizations from your reports to the dashboard

## Sharing the Dashboard

1. Click on the dashboard
2. Click "Share" in the top right
3. Enter email addresses of people you want to share with
4. Set permissions
5. Click "Share"

## Step 9: Running the Complete System

Let's create a README file with instructions on how to run the system:

```py project="Sports Analytics" file="README.md" type="code"
# Cricket Analytics System

Real-time Business Intelligence system for the ICC Champions Trophy 2025 Final.

## Overview

This system provides real-time analytics for cricket match streaming, including:

- Viewership analytics
- Engagement metrics
- Sentiment analysis
- Ad performance

## Architecture

- **Data Ingestion**: Kafka, Twitter API, Google Ads API
- **Data Processing**: Apache Spark
- **Storage**: MongoDB (raw data), Snowflake (processed data), Firebase (real-time updates)
- **ML/NLP**: HuggingFace Transformers, Vader, SpaCy
- **Visualization**: Power BI
- **DevOps**: Docker, GitHub Actions

## Prerequisites

- Python 3.9+
- Java 11 (for Apache Spark)
- Docker
- Kafka
- MongoDB
- Snowflake account
- Firebase account
- Twitter Developer account
- Google Ads account
- Power BI Desktop

## Environment Variables

Create a `.env` file with the following variables:

\`\`\`
# MongoDB
MONGODB_CONNECTION_STRING=mongodb://localhost:27017

# Snowflake
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password

# Firebase
FIREBASE_CREDENTIALS_PATH=path/to/credentials.json
FIREBASE_DATABASE_URL=https://your-project.firebaseio.com

# Twitter
TWITTER_BEARER_TOKEN=your_bearer_token

# Google Ads
GOOGLE_ADS_CLIENT_ID=your_client_id
GOOGLE_ADS_CLIENT_SECRET=your_client_secret
GOOGLE_ADS_DEVELOPER_TOKEN=your_developer_token
GOOGLE_ADS_REFRESH_TOKEN=your_refresh_token
GOOGLE_ADS_CUSTOMER_ID=your_customer_id

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
\`\`\`
