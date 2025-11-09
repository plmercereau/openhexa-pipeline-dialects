# DuckDB and other additional dialects in OpenHexa pipelines

## Full DuckDB example
```python
import pandas as pd
from openhexa.sdk import Dataset, pipeline, parameter
from openhexa.sdk.pipelines import OUTPUT
from openhexa.sdk.logger import LOG_LEVELS, logger


@pipeline("my-pipeline")
@parameter(
    "src_dataset",
    name="Input dataset",
    type=Dataset,
    required=True
)
def my_pipeline(src_dataset: Dataset):
    # The DuckDB task returns a dataframe
    values = aggregate(
        # New "get_file_url" helper that gets the signed URL of the dataset file.
        # The "version" argument is optional and defaults to "latest"
        src_dataset.get_file_url(version="latest", filename="my_file.parquet"),
        'mean'
    )
    print_values(values)


@my_pipeline.task(
    dialect="duckdb",
    # Optional outputs
    outputs=[
        # Print the output dataframe in the pipeline logs
        OUTPUT.logger(LOG_LEVELS.INFO),
        # Write the file into a file of the given dataset
        OUTPUT.dataset_file(
            # New "dataset" helper that returns the dataset defined in the pipeline parameters
            dataset=my_pipeline.dataset('src_dataset'),
            version="latest", # Optional parameter. Defaults to "latest"
            filename='aggregated.parquet' # detects the format from the extension of the file name
        ),
    ]
)
def aggregate(file_url: str, stat: str):
    return (
        """
        SELECT 
          variable, count(*), sum(value) as sum
        FROM read_parquet(?) 
        WHERE stat_type = ?
        GROUP BY variable
        """,
        [file_url, stat]
    )

@my_pipeline.task(dialect="duckdb", source="./aggregation_query.sql")
def aggregate_alternative(file_url: str, stat: str):
    """
    Alternative approach: Load query from an external SQL file.
    
    Benefits:
    - Better IDE support (syntax highlighting, linting)
    - Improved maintainability for complex queries
    - Cleaner separation of concerns
    
    Note: When using 'source', the function returns only the parameters list.
    """
    return [file_url, stat]

@my_pipeline.task
def print_values(values: pd.DataFrame):
    # New helper that handles OpenHexa logs
    logger.info("Columns:")
    for col in values.columns:
        logger.info(f"  {col}: {values[col].dtype}")

    logger.info("\nRows:")
    for idx, row in values.iterrows():
        logger.info(f"  Row {idx}: {row.to_dict()}")
```



## General syntax

### With a @task decorator
```python
@my_pipeline.task(dialect="duckdb")
def task1():
    return """
    select 1 as x
    """
```

### Without decorator
```python
my_pipeline.task(
    name="task2",
    dialect="duckdb",
    content="""
    SELECT * FROM read_parquet(get_dataset_file_url('pathways-senegal-2019-dhs8/sen-2019dhs8-aggregated-metrics/latest/metrics.parquet'));
    """
)
```

### The @dialect decorator
```python
from openhexa.sdk.pipelines import dialect

@my_pipeline.task
@dialect("duckdb")
def task1():
    return """
    select 1 as x
    """
```

## Parameterized queries

Tasks can return:
- A simple string containing the query/code
- A tuple of `(query_string, parameters_list)` for parameterized queries

```python
@my_pipeline.task(dialect="duckdb")
def task1():
    """Simple query returning a string."""
    return "SELECT 1 as x"

@my_pipeline.task(dialect="duckdb")
def task2(threshold: int):
    """Parameterized query using ? placeholders (DuckDB style)."""
    return ("SELECT * FROM table WHERE value > ?", [threshold])
```

## Task arguments

```python
@pipeline("my-pipeline")
@parameter(
    "src_dataset",
    name="Input dataset",
    type=Dataset
)
def my_pipeline(src_dataset: Dataset):
    aggregate(
        src_dataset.get_file_url(version="latest", filename="my_file.parquet"),
        'mean'
    )


@my_pipeline.task(dialect="duckdb")
def aggregate(file_url: str, stat: str):
    return (
        """
        SELECT 
          variable, count(*), sum(value) as sum
        FROM read_parquet(?) 
        WHERE stat_type = ?
        GROUP BY variable
        """,
        [file_url, stat]
    )
```


## Executing code from another file

### Without parameters
```python
my_pipeline.task(
    name="task3",
    dialect="duckdb",
    source="./my-instruction.sql"
)
```

### With parameters
```python
@pipeline("my-pipeline")
@parameter("src_dataset", name="Input dataset", type=Dataset)
def my_pipeline(src_dataset: Dataset):
    # Call the task with parameters
    aggregate_from_file(
        src_dataset.get_file_url(version="latest", filename="data.parquet"),
        'mean'
    )


@my_pipeline.task(dialect="duckdb", source="./aggregation_query.sql")
def aggregate_from_file(file_url: str, stat: str):
    """
    Load query from external SQL file and pass parameters to it.
    
    The SQL file should use ? placeholders (DuckDB) or $1, $2, etc. (Postgres).
    When using 'source', return only the parameters list.
    """
    return [file_url, stat]
```

## Outputs
```python
from openhexa.sdk.pipelines import OUTPUT
from openhexa.sdk.logger import LOG_LEVELS

my_pipeline.task(
    name="task4",
    dialect="duckdb",
    outputs=[
        OUTPUT.logger(LOG_LEVELS.INFO),
        OUTPUT.dataset_file(
            dataset=my_pipeline.dataset('argument_name'),
            version="latest",
            filename='output.parquet'
        ),
        OUTPUT.file(path="path/to/file.csv")
    ]
)
```

## Other dialects

### Return types by dialect

Each dialect returns different data types:
- **DuckDB**: Returns a pandas DataFrame
- **Postgres**: Returns a pandas DataFrame
- **R**: Returns the output printed to stdout as a string
- **GraphQL**: Returns a dictionary/JSON object

### Postgres

#### Query from the database of the workspace
```python
@my_pipeline.task(dialect="postgres")
def task1():
    return """
    select 1 as x
    """
```

#### Query from an external database
```python
@my_pipeline.task(dialect="postgres", connection="postgres://...")
def task2():
    return """
    select 1 as x
    """
```

#### Get Postgres credentials from an OpenHexa connection
```python
from openhexa.sdk.workspaces.connection import CustomConnection
from openhexa.sdk import parameter, pipeline

@pipeline("my-pipeline")
@parameter(
    "database",
    name="An external database",
    type=CustomConnection
)
def my_pipeline(database: CustomConnection):
    task1()


@my_pipeline.task(
    dialect="postgres",
    connection=my_pipeline.connection("database").get("URL")
)
def task1():
    return """
    select 1 as x
    """
```

#### Parameterized Postgres queries
```python
from datetime import date

@my_pipeline.task(dialect="postgres")
def task4(status: str, created_at: date, country: str):
    """
    Parameterized Postgres query using $1, $2, $3 placeholders.
    
    Returns tuple of (query, [parameters]).
    """
    return (
        """
        SELECT id, name, email
        FROM users
        WHERE status = $1
          AND created_at >= $2
          AND country = $3;
        """,
        [status, created_at, country]
    )
```

### R

```python
@pipeline("my-pipeline")
@parameter(
    "src_dataset",
    name="Input dataset",
    type=Dataset
)
def my_pipeline(src_dataset: Dataset):
    task1(src_dataset.get_file_url(version="latest", filename="my_file.parquet"))


@my_pipeline.task(dialect="R")
def task1(url: str):
    """
    R script with parameters passed as command-line arguments.
    
    Returns tuple of (R_code, [arguments]).
    """
    return """
    library(arrow)
    args <- commandArgs(trailingOnly = TRUE)
    url <- args[1]
    df <- read_parquet(url)
    print(df)
    """, [url]
```

### GraphQL

#### Querying the OpenHexa GraphQL API
```python
from openhexa.sdk import pipeline, workspace

@pipeline("my-pipeline")
def my_pipeline():
    json_payload = task1(workspace.current_workspace.slug)


@my_pipeline.task(dialect="graphql")
def task1(workspace_slug: str):
    return (
        """
        query($workspace_slug: String!) {
          workspace(slug: $workspace_slug) {
            members {
              items {
                user {
                  firstName
                }
              }
            }
          }
        }
        """,
        {"workspace_slug": workspace_slug}
    )
```

#### Querying an external GraphQL service
```python
from openhexa.sdk.workspaces.connection import CustomConnection
from openhexa.sdk import parameter, pipeline, workspace

@pipeline("my-pipeline")
@parameter(
    "backend",
    name="A GraphQL backend",
    type=CustomConnection
)
def my_pipeline(backend):
    json_payload = task1()


@my_pipeline.task(
    dialect="graphql",
    connection=my_pipeline.connection("backend").get("URL"),
    method="POST",
    headers={
        "Authorization": f"Bearer {my_pipeline.connection('backend').get('API_KEY')}"
    }
)
def task1():
    return """
    query GetDomains {
      domains(pagination: { pageSize: 500 }, sort: ["code:asc"]) {
        documentId
        code
        name_en
        name_fr
        order
      }
    }
    """
```