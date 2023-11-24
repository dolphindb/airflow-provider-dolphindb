# Airflow-provider-dolphindb

The Airflow-provider-dolphindb enables the execution of DolphinDB scripts (.dos files) within Apache Airflow workflows. It provides custom DolphinDB Hooks and Operators that allow users to seamlessly run DolphinDB scripts including insertion of data, ETL transformations, data modeling, etc.

## Installing airflow-provider-dolphindb

```sh
pip install airflow-provider-dolphindb
```

## Running a DAG Example

The following example demonstrates the DolphinDB provider with a sample DAG for creating a DolphinDB database and table, and executing a .dos script to insert data. Follow these steps to run it:

1. Copy [example_dolphindb.py](https://github.com/dolphindb/airflow-provider-dolphindb/blob/main/example_dags/example_dolphindb.py) and [insert_data.dos](https://github.com/dolphindb/airflow-provider-dolphindb/blob/main/example_dags/insert_data.dos) to your DAGs folder. If you use the default airflow configuration *airflow.cfg*, you may need to create the DAGs folder yourself at *AIRFLOW_HOME/dags*.

2. Start your DolphinDB server on port 8848.

3. Navigate to your Airflow project directory and start Airflow in development mode:
   
   ```sh
   cd /path/to/airflow/project
   # Only absolute paths are accepted
   export AIRFLOW_HOME=/your/project/dir/
   export AIRFLOW_CONN_DOLPHINDB_DEFAULT="dolphindb://admin:123456@127.0.0.1:8848"
   python -m airflow standalone
   ```

For detailed instructions, refer to [Airflow - Production Deployment](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/production-deployment.html).

4. Configure a DolphinDB connection

To enable communication between Airflow and DolphinDB, you first need to configure a DolphinDB connection:

(1) Click *Admin -> Connections* ->  **+** icon to add a connection.

![dolphindb-datasource.png](./images/dolphindb-datasource.png)

(2) Enter the following values in the form:

    Connect Id: **dolphindb_default**

    Connection Type: **DolphinDB**

(3) Fill in any additional connection details according to your environment. 

Click the **test** button to test the connection. If successful, it will output "Connection successfully tested". Press the **save** button to save the connection.

Once the connection is created, the example_dolphindb DAG will be able to interface with DolphinDB automatically using this connection. Try triggering the example DAG and inspecting the task logs to see the DolphinDB scripts executing using the configured connection.

![example-dag.png](./images/example-dag.png)

## Developer Guides

This guide covers key steps for developers working on and testing the Airflow DolphinDB provider:

### Installing Apache Airflow

Refer to [Airflow - Quick Start](https://airflow.apache.org/docs/apache-airflow/stable/start.html) for further details.

```sh
# It is recommended to use the current project directory as the airflow working directory
cd /your/source/dir/airflow-provider-dolphindb
# Only absolute paths are accepted
export AIRFLOW_HOME=/your/source/dir/airflow-provider-dolphindb

# Install apache-airflow 2.6.3
AIRFLOW_VERSION=2.6.3
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

You may need to install Kubernetes to eliminate errors in airflow routines:

```sh
pip install kubernetes
```

### Installing airflow-provider-dolphindb for testing

To use the DolphinDB provider in test workflows, install the library in editable mode:

```sh
python -m pip install -e .
```

Refer to [pip documentation](https://pip.pypa.io/en/stable/cli/pip_install/#install-editable) for further details.

### Running Provider Tests

Validate the changes through the test suite:

```sh
cd /your/source/dir/airflow-provider-dolphindb
# Only absolute paths are accepted
export AIRFLOW_HOME=/your/source/dir/airflow-provider-dolphindb
export AIRFLOW_CONN_DOLPHINDB_DEFAULT="dolphindb://admin:123456@127.0.0.1:8848"
pytest
```

### Building a Package

Bundle the provider into a package using:

```sh
python -m build
```
