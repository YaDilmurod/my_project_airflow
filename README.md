### Step one: Building the Docker image

Build the Docker container image using the following command:

```bash
./mwaa-local-env build-image
```

**Note**: it takes several minutes to build the Docker image locally.

### Step two: Running Apache Airflow

#### Local runner

Runs a local Apache Airflow environment that is a close representation of MWAA by configuration.

```bash
./mwaa-local-env start
```

To stop the local environment, Ctrl+C on the terminal and wait till the local runner and the postgres containers are stopped.

### Step three: Accessing the Airflow UI

By default, the `bootstrap.sh` script creates a username and password for your local Airflow environment.

- Username: `admin`
- Password: `test`

#### Airflow UI

- Open the Apache Airlfow UI: <http://localhost:8080/>.
























# About aws-mwaa-local-runner

This repository provides a command line interface (CLI) utility that replicates an Amazon Managed Workflows for Apache Airflow (MWAA) environment locally.

*Please note: MWAA/AWS/DAG/Plugin issues should be raised through AWS Support or the Airflow Slack #airflow-aws channel.  Issues here should be focused on this local-runner repository.*


## About the CLI

The CLI builds a Docker container image locally that’s similar to a MWAA production image. This allows you to run a local Apache Airflow environment to develop and test DAGs, custom plugins, and dependencies before deploying to MWAA.

## What this repo contains

```text
dags/
  example_lambda.py
  example_dag_with_taskflow_api.py    
  example_redshift_data_execute_sql.py
docker/
  config/
    airflow.cfg
    constraints.txt
    mwaa-base-providers-requirements.txt
    webserver_config.py
    .env.localrunner
  script/
    bootstrap.sh
    entrypoint.sh
    systemlibs.sh
    generate_key.sh
  docker-compose-local.yml
  docker-compose-resetdb.yml
  docker-compose-sequential.yml
  Dockerfile
plugins/
  README.md
requirements/  
  requirements.txt
.gitignore
CODE_OF_CONDUCT.md
CONTRIBUTING.md
LICENSE
mwaa-local-env
README.md
VERSION
```

## Prerequisites

- **macOS**: [Install Docker Desktop](https://docs.docker.com/desktop/).
- **Linux/Ubuntu**: [Install Docker Compose](https://docs.docker.com/compose/install/) and [Install Docker Engine](https://docs.docker.com/engine/install/).
- **Windows**: Windows Subsystem for Linux (WSL) to run the bash based command `mwaa-local-env`. Please follow [Windows Subsystem for Linux Installation (WSL)](https://docs.docker.com/docker-for-windows/wsl/) and [Using Docker in WSL 2](https://code.visualstudio.com/blogs/2020/03/02/docker-in-wsl2), to get started.
