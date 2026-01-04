# Pokémon Usage Data Pipeline & Battle Simulator

## Project Overview
This project implements an end-to-end data engineering pipeline designed to ingest, transform, and warehouse competitive Pokémon usage statistics from Smogon (Generation 9 OU). The processed data powers a custom battle simulator that generates battle results between randomly generated or user-selected teams. The system utilizes a lazy-loading storage strategy to efficiently manage the combinatorial complexity of team matchups, ensuring that only active team configurations are stored in the database.

The primary goal of this repository is to demonstrate a scalable architecture capable of handling complex data relationships and high-volume simulation logs within a containerized environment.

## System Architecture
The pipeline follows a standard Extract, Load, Transform (ELT) pattern, orchestrated entirely within Docker containers.

### Ingestion Layer
Apache Airflow orchestrates the extraction of raw JSON usage statistics from Smogon's public repositories. Simultaneously, a secondary ingestion task fetches metadata from the PokéAPI to retrieve static attributes such as evolution stages and type pairings. This raw data is staged in a local data lake using Docker volumes.

### Processing Layer
Apache Spark (PySpark) is utilized to process the raw staging data. The transformation logic joins the Smogon usage statistics with the PokéAPI metadata to filter the dataset, retaining only fully evolved Pokémon. The complex JSON structures representing move sets, item usage, and EV spreads are flattened and normalized into a relational schema.

### Storage Layer
Processed data is loaded into a PostgreSQL data warehouse. The schema is designed to separate dimensional data (Pokémon attributes) from fact tables (usage statistics and battle results).

### Simulation Layer
A Python-based battle simulator uses the warehoused data to construct valid teams based on meta-game statistics. The simulator runs battles and writes the results back to the database in batches. To optimize storage on local hardware, the system records only the battle metadata (winner, loser, turn count) and utilizes a unique hash for every team composition, ensuring efficient storage and retrieval.

## Technology Stack
* **Infrastructure & Orchestration:** Docker and Docker Compose are used for containerization and service orchestration. Apache Airflow manages the workflow scheduling and dependency management.
* **Data Processing & Language:** Python 3.9+ serves as the primary programming language. Apache Spark (PySpark) handles distributed data processing and transformation tasks.
* **Storage:** PostgreSQL functions as the primary data warehouse. Local Docker volumes are used for the raw data lake.

## Prerequisites
To run this project, the host machine must have Docker and Docker Compose installed. No local Python or PostgreSQL installation is required as all services run within isolated containers.

## Installation and Usage

1. Clone the repository to your local machine:
   bash
   
   git clone [https://github.com/tenshi341/pokemon-data-pipeline](https://github.com/tenshi341/pokemon-data-pipeline.git)
   cd pokemon-data-pipeline
   
Build and start the containerized services using Docker Compose. This command initializes Airflow, Postgres, and the Spark master node:

Bash

docker-compose up --build -d

Access the Airflow web interface at http://localhost:8080. Default set user is admin with password: admin. Enable the primary DAG named smogon_elt_pipeline to commence the data ingestion and transformation process.

Once the pipeline has completed a successful run, the battle simulator can be triggered via the provided Python entry point script:

Bash

docker exec -it pokemon-data-pipeline-airflow-scheduler-1 /opt/airflow/bot_env/bin/python /opt/airflow/scripts/bot.py

Database Schema
The PostgreSQL database is organized into three primary tables:

dim_pokemon: Stores static attributes including the Pokémon ID, name, types, and base statistics. This table is enriched using data from the PokéAPI.

fact_usage: Contains the statistical data derived from Smogon. It links to the dimension table and includes usage percentages, most common items, natures, and move combinations.

fact_battles: Records the outcomes of the simulations. It references the unique team hashes involved in the match, the winner, the loser, and the total turn count.

Performance Considerations
This project is optimized for execution on standard personal hardware. The Docker containers are configured with strict resource limits to prevent memory exhaustion. Database writes from the simulator are batched to minimize I/O overhead. The simulation logic eschews storing full textual battle logs in favor of lightweight integer-based metadata to minimize storage requirements.
