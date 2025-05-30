# Weather Data Processing with Apache Cassandra

This project utilizes Apache Cassandra as the primary database for storing weather data. Follow these instructions to
set up and use Cassandra with the project.

## Setup and Installation

### Prerequisites
1. Install [Docker](https://www.docker.com/).
2. Install [Docker Compose](https://docs.docker.com/compose/).

### Steps
1. Clone the repository:
   ```bash
   git clone git@github.com:ntrg1317/weather-pipeline.git
   cd weather-pipeline
   ```

2. Set up the `.env` file with the required environment variables:
   ```env
   AIRFLOW__CORE__LOAD_EXAMPLES=False
   AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=2
   AIRFLOW_UID=0
   
   # Airflow Metadata DB
   AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
   AIRFLOW__DATABASE__LOAD_DEFAULT_CONNECTIONS=False
   
   # Airflow Init
   _AIRFLOW_DB_UPGRADE=True
   _AIRFLOW_WWW_USER_CREATE=True
   _AIRFLOW_WWW_USER_USERNAME=admin
   _AIRFLOW_WWW_USER_PASSWORD=admin
   
   # Backend
   POSTGRES_USER=user
   POSTGRES_PASSWORD=user
   POSTGRES_DB=airflow
   
   #MinIO
   MINIO_HOST=minio:9000 # minio: service name in Docker
   MINIO_ACCESS_kEY=
   MINIO_SECRET_KEY=
   ```

3. Build and start the containers:
   ```bash
   docker composer up --build
   ```
   
4. Pull Cassandra and connect to the default network
   ```bash
   docker pull cassandra:latest
   ```
   ```bash
   docker run --rm -d --name cassandra --hostname cassandra --network weather-data-pipeline_default cassandra
   ```

5. Access the services:
   - Airflow: [http://localhost:8080](http://localhost:8080)
   - Spark: [http://localhost:9090](http://localhost:9090)
   - MinIO: [http://localhost:9000](http://localhost:9000)

