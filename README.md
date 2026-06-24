# World Bank Data Pipeline

Transforms raw World Bank data into analytics and machine learning-ready datasets using PySpark. This project builds a scalable data pipeline that cleans, reshapes, validates, and engineers features from World Bank indicators for data science and analytics workflows.
## Run the Spark Pipeline

Start the Spark master and workers, then submit the transformation job:

```sh
make run
make stop
```

## Testing
Local PySpark tests require a Java runtime. Install Java 17 or newer and set `JAVA_HOME` if the shell cannot find it.


## Data
The API client writes extracted World Bank indicator data as a Parquet file:

- `/data/world_bank_api_indicator_data.parquet`

The pipeline writes three partitioned Parquet datasets:

- `/output/world_bank_indicators_long.parquet`
- `/output/world_bank_indicators_indicator_wide.parquet`
- `/output/world_bank_indicators_year_wide.parquet`

Each output is a directory of Spark-written `part-*.parquet` files.

The long output columns are:

```text
Country Name
Country Code
Series Name
Series Code
Year
Value
Topic
```

## Data Source
This project uses publicly available data from the World Bank Open Data initiative.
Source: https://data.worldbank.org/
World Bank data is licensed under the Creative Commons Attribution 4.0 International (CC BY 4.0) License.
© The World Bank. Modified and redistributed under the terms of CC BY 4.0.
