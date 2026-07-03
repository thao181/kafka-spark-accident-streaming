# kafka-spark-accident-streaming
kafka-spark-accident-streaming already exists in this account

# Kafka Spark Accident Streaming

This project builds a real-time traffic accident streaming pipeline using Apache Kafka, Spark Structured Streaming, and Python visualization tools.

The pipeline simulates live accident records, predicts accident severity with a trained Spark ML pipeline model, writes streaming outputs to Parquet, republishes the processed results to Kafka, and visualizes the results in a notebook dashboard.

## Project Structure

```text
.
├── A2B-Task1_producer.ipynb          # Kafka producer that simulates live accident records
├── A2B-Task2_spark_streaming.ipynb   # Spark Structured Streaming prediction pipeline
├── A2B-Task3_consumer.ipynb          # Kafka consumer and real-time dashboard
├── streaming_collision.csv           # Input collision records used by the producer
├── vehicle.csv                       # Static vehicle information joined with streaming records
└── best_regression_model/            # Saved Spark ML PipelineModel used for severity prediction
```

## Pipeline Overview

```text
streaming_collision.csv
        |
        v
Kafka Producer
        |
        v
Kafka topic: accident_stream
        |
        v
Spark Structured Streaming
        |
        |-- joins static vehicle data from vehicle.csv
        |-- creates time, traffic, weekend, vehicle, and road-condition features
        |-- loads best_regression_model
        |-- predicts accident severity
        |
        v
Parquet streaming outputs
        |
        v
Kafka output topics
        |
        v
Kafka Consumer Dashboard
```

## Notebook Descriptions

### 1. `A2B-Task1_producer.ipynb`

This notebook simulates real-time accident data streaming.

It:

- Reads records from `streaming_collision.csv`
- Sends a random batch of 50 to 100 records every second
- Keeps a pointer so records are streamed in chronological order
- Adds the current timestamp as `accident_ts`
- Publishes records to the Kafka topic `accident_stream`

### 2. `A2B-Task2_spark_streaming.ipynb`

This notebook consumes the raw Kafka stream and performs real-time severity prediction.

It:

- Creates a Spark session with four local cores
- Uses the `Europe/London` timezone
- Reads streaming records from Kafka topic `accident_stream`
- Parses JSON messages into Spark DataFrame columns
- Applies a 30-second watermark on `accident_ts`
- Loads static vehicle data from `vehicle.csv`
- Aggregates vehicle-level information by `collision_index`
- Joins streaming accident records with vehicle features
- Creates additional features such as:
  - `Hour`
  - `Peak_Traffic`
  - `is_weekend`
  - vehicle-type flags
  - crash indicator flags
- Loads the trained Spark ML model from `best_regression_model`
- Predicts accident severity and rounds it into `severity_rating`

The notebook creates three streaming outputs:

- High-severity accidents where `severity_rating > 7`
- Total accident count by severity level
- Accident count by district and severity group: low, medium, high

These outputs are saved as Parquet streams and then republished to Kafka topics:

- `a2b_6a_high_severity`
- `a2b_6b_severity_count`
- `a2b_6c_district_severity`

### 3. `A2B-Task3_consumer.ipynb`

This notebook consumes the processed Kafka topics and displays a real-time dashboard.

It:

- Starts a Kafka consumer in a background thread
- Reads from the three processed Kafka topics
- Stores the latest records in memory
- Updates the dashboard every 5 seconds

The dashboard includes:

- A bar chart showing high-severity accidents over time
- A histogram showing the cumulative distribution of accident severity
- A bubble map showing high-severity accident locations

## Requirements

The notebooks use:

- Python
- Apache Kafka
- Apache Spark / PySpark
- Spark Structured Streaming
- Spark ML
- kafka-python
- pandas
- matplotlib
- plotly
- folium

The Spark notebook also uses the Kafka connector:

```text
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
```

## Required Input Files

Before running the notebooks, make sure the following files or folders are available in the working directory:

- `streaming_collision.csv`
- `vehicle.csv`
- `best_regression_model/`

The `best_regression_model` folder should contain the saved Spark ML `PipelineModel` created from the earlier model-training task.

## Kafka Topics

### Input topic

```text
accident_stream
```

### Output topics

```text
a2b_6a_high_severity
a2b_6b_severity_count
a2b_6c_district_severity
```

## How to Run

Run the notebooks in this order:

1. Start Kafka and make sure it is reachable at `kafka:9092`.
2. Open and run `A2B-Task1_producer.ipynb` to stream raw accident records.
3. Open and run `A2B-Task2_spark_streaming.ipynb` to consume the raw stream, predict severity, save Parquet outputs, and publish processed streams.
4. Open and run `A2B-Task3_consumer.ipynb` to view the real-time dashboard.

## Output Folders

During execution, the Spark streaming notebook creates:

```text
checkpoint/
parquet_output/
```

These folders store streaming checkpoints and Parquet output files. They are generated automatically while the streaming jobs are running.

## Notes

- The project assumes Kafka is available at `kafka:9092`.
- The producer sends one batch every second.
- Spark updates the high-severity stream every 5 seconds.
- Spark updates severity counts every 10 seconds.
- Spark updates district severity counts every 30 seconds.
- The dashboard refreshes every 5 seconds.
- If running outside the original environment, update file paths, Kafka host, and model path as needed.
