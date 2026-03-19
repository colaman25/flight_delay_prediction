import os
import duckdb
from fastapi import FastAPI, HTTPException
import math

app = FastAPI(title="Flight Delay Prediction API")

WAREHOUSE_PATH = "s3://flight-delay-predictions/iceberg"
TABLE_PATH = f"{WAREHOUSE_PATH}/data/prediction_data"

# Initialize DuckDB
con = duckdb.connect()

# Install and load extensions
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")

con.execute("INSTALL iceberg;")
con.execute("LOAD iceberg;")

# Configure S3 access
con.execute(f"""
SET s3_region='eu-west-2';
SET s3_access_key_id='{os.getenv("AWS_ACCESS_KEY_ID")}';
SET s3_secret_access_key='{os.getenv("AWS_SECRET_ACCESS_KEY")}';
""")

# ✅ Enable unsafe version guessing for Iceberg tables
con.execute("SET unsafe_enable_version_guessing = true;")

def get_prediction_by_ident(ident: str):

    query = f"""
        SELECT *
        FROM iceberg_scan('{TABLE_PATH}')
        WHERE ident = '{ident}'
        ORDER BY scheduled_out DESC
        LIMIT 1
    """

    rows = con.execute(query).fetchall()
    if not rows:
        return None

    cols = [c[0] for c in con.description]

    record = {}
    for col, val in zip(cols, rows[0]):
        if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
            val = None
        record[col] = val

    return record


@app.get("/")
def root():
    return {"message": "Flight Delay Prediction API running"}


@app.get("/prediction/{ident}")
def prediction(ident: str):

    result = get_prediction_by_ident(ident)

    if result is None:
        raise HTTPException(status_code=404, detail="Prediction not found")

    return result


@app.get("/latest")
def latest_predictions(limit: int = 10):

    query = f"""
        SELECT *
        FROM iceberg_scan('{TABLE_PATH}')
        ORDER BY scheduled_out DESC
        LIMIT {limit}
    """

    rows = con.execute(query).fetchall()
    cols = [c[0] for c in con.description]

    records = []
    for row in rows:
        record = {}
        for col, val in zip(cols, row):
            if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
                val = None
            record[col] = val
        records.append(record)

    return records