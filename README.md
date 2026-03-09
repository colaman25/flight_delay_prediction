# Live Flight Delay Prediction

Use live flight schedule and live aircraft data to generate running departure delay predictions up to actual departure time.

## Live Data Sources
Flight Schedule Data: \
Flight Aware Aero API \
https://www.flightaware.com/commercial/aeroapi/ \
####
Aircraft Live Data: \
OpenSky API \
https://openskynetwork.github.io/opensky-api/


## Usage
Install Docker:\
https://www.docker.com/get-started/

Project folder: proj_docker
####
In bash, run: \
docker compose up --build -d
####
This will spin up all the necessary containers:
- Kafka Instance
- Kafka to Iceberg
- Data Aggregator and Feature Engineer
- Airflow
- Live Predictions

Airflow: localhost:8080 to orchestrate:
- Live Schedule Reader
- Live Flight Reader
- Model Training

## Required Credentials
####proj_docker/.env
FLIGHTAWARE_API \
OPENSKY_CLIENT_ID \
OPENSKY_CLIENT_SECRET \
AWS_ACCESS_KEY_ID \
AWS_SECRET_ACCESS_KEY \
AIRFLOW__WEBSERVER__SECRET_KEY
###
####Airflow
FLIGHTAWARE_API \
OPENSKY_CLIENT_ID \
OPENSKY_CLIENT_SECRET \
AWS_ACCESS_KEY_ID \
AWS_SECRET_ACCESS_KEY
###
####S3 Bucket IAM Roles Needed:
https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraMySQL.Integrating.Authorizing.IAM.S3CreatePolicy.html

##Pre-Built Models
- Linear Regression
- Random Forest