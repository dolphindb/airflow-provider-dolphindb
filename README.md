# Airflow-provider-dolphindb
## Installing airflow-provider-dolphindb

At the moment, it is not possible to install airflow-provider-dolphindb via PyPI since the package has not been uploaded yet. However, you can install it in the future using the following command:

```sh
pip install airflow-provider-dolphindb
```

## Example DAGs

To create a database and a table in it, and then execute an external .dos script file to insert data, follow these steps:

1. Copy the [example_dolphindb.py](https://github.com/dolphindb/airflow-provider-dolphindb/blob/main/example_dags/example_dolphindb.py) file to your DAGs folder. If you use the default airflow configuration `airflow.cfg`, you may need to create the DAGs folder yourself, which is located in `AIRFLOW_HOME/dags`.
1. Copy the [insert_data.dos](https://github.com/dolphindb/airflow-provider-dolphindb/blob/main/example_dags/insert_data.dos) file to the same directory as `example_dolphindb.py`.
1. Start your DolphinDB server on port 8848.
1. Start airflow in the development environment:

    ```sh
    cd /your/project/dir/
    # Only absolute paths are accepted
    export AIRFLOW_HOME=/your/project/dir/
    export AIRFLOW_CONN_DOLPHINDB_DEFAULT="dolphindb://admin:123456@127.0.0.1:8848"
    python -m airflow standalone
    ```

Please refer to the [Official documentation for the production environment](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/production-deployment.html).

Now, you can find the example_dolphindb DAG on your airflow web page. You can try to trigger it.

## Developer Documentation

### Installing Apache Airflow

Refer to https://airflow.apache.org/docs/apache-airflow/stable/start.html for further details on this topic.

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

Additionally, you may need to install Kubernetes to eliminate errors in airflow routines:

```sh
pip install kubernetes
```

### Installing airflow-provider-dolphindb for testing

Refer to https://pip.pypa.io/en/stable/cli/pip_install/#install-editable for further details on this topic.

```sh
python -m pip install -e .
```

### Testing

Run the following commands to validate the installation procedure above.

```sh
cd /your/source/dir/airflow-provider-dolphindb
# Only absolute paths are accepted
export AIRFLOW_HOME=/your/source/dir/airflow-provider-dolphindb
export AIRFLOW_CONN_DOLPHINDB_DEFAULT="dolphindb://admin:123456@127.0.0.1:8848"
pytest
```

### Packaging airflow-provider-dolphindb

Run the following command.

```sh
python -m build
```