# Real-Time Analysis BI

## Project Description
**Real-Time Analysis BI** is a Python-based application that ingests and processes data in real time—simulating streaming data from sources like Google Ads—to generate insights and visualizations using Power BI. The application is built to demonstrate:

- How streaming data pipelines can be implemented in Python
- Docker-based deployment for reproducibility
- Integration with Power BI for visualization and reporting

### Why this application?
This project allows you to explore the complete real-time data pipeline stack—from ingestion to visualization. Whether you're looking to prototype business intelligence workflows, test Power BI dashboards, or practice containerized deployment, this app gives you a working foundation.

### Technologies Used
- **Python** – Core language for data ingestion, processing, and REST APIs (`app.py`, `run_ingestion.py`)
- **Docker** & **docker-compose** – For containerizing the app and dependencies
- **Mock data** – e.g. `mock_google_ads_data.json` to simulate streaming inputs
- **Power BI** – To visualize processed data (see `power_bi_setup.md`)
- **Requirements.txt** – Lists all Python dependencies for quick setup

### Challenges Faced
1. **Simulating streaming data** – Creating realistic mock data flows required crafting mock JSON data and writing logic to iterate through it in a streaming fashion.
2. **Managing dependencies** – Ensuring reproducibility across environments meant pinning versions and configuring Docker carefully.
3. **Power BI connectivity** – Configuring Power BI to fetch data from a dynamic, locally hosted endpoint (or database) took several iterations to get right.

---

## Table of Contents *(Optional)*
- [Project Description](#project-description)
- [Technologies Used](#technologies-used)
- [Challenges Faced](#challenges-faced)
- [Installation & Run](#installation--run)

---

## Installation & Run Instructions

### 1. Clone the Repository
```bash
git clone https://github.com/kalpadas599/Real-time-analysis-BI.git
cd Real-time-analysis-BI
```

### 2. Set Up Environment
Make sure you have Docker and docker-compose installed.

### 3. Running with Docker
```bash
docker-compose up --build
```
This will start:
-A Python container running your ingestion and API logic
-Any required services defined in `docker-compose.yml`

### 4. Running Locally (Without Docker)
If you prefer local setup:
```
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```
Then run:
```
python run_ingestion.py
python app.py
```
This starts your ingestion pipeline and the app API layer.

### 5. Power BI Configuration
Refer to [power_bi_setup.md](https://github.com/kalpadas599/Real-time-analysis-BI/blob/master/power_bi_setup.md) which outlines how to connect your Power BI dashboard to the local API endpoints or data files that the application serves.

## Project Structure
<img width="2931" height="2448" alt="Image" src="https://github.com/user-attachments/assets/baff9fe3-277b-40d2-94cd-2020493a7e4b" />

```
Directory structure:
└── kalpadas599-real-time-analysis-bi/
    ├── README.md
    ├── app.py
    ├── docker-compose.yml
    ├── Dockerfile
    ├── mock_google_ads_data.json
    ├── power_bi_setup.md
    ├── requirements.txt
    ├── run_ingestion.py
    ├── en_core_web_sm-3.8.0/
    │   └── en_core_web_sm/
    │       ├── __init__.py
    │       └── meta.json
    ├── src/
    │   ├── api/
    │   │   └── flask_api.py
    │   ├── data_ingestion/
    │   │   ├── confluent_producer.py
    │   │   ├── google_ads_client.py
    │   │   └── twitter_client.py
    │   ├── data_processing/
    │   │   └── spark_processor.py
    │   ├── ml/
    │   │   └── sentiment_analyzer.py
    │   └── storage/
    │       ├── firebase_client.py
    │       ├── mongodb_client.py
    │       └── snowflake_client.py
    └── .github/
        └── workflows/
            └── main.yml
```

### Contact to know more
[LinkedIn](https://www.linkedin.com/in/kalpadas)



