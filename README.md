# GitHub Repository ETL Pipeline

This project implements an Extract, Transform, Load (ETL) pipeline designed to collect metadata about open-source GitHub repositories using the GitHub GraphQL API and store it in a PostgreSQL database.

## Table of Contents

1.  [Project Overview](#project-overview)
2.  [Features](#features)
3.  [Technologies Used](#technologies-used)
4.  [Setup Instructions](#setup-instructions)
    * [Prerequisites](#prerequisites)
    * [Clone the Repository](#clone-the-repository)
    * [Install Dependencies](#install-dependencies)
    * [GitHub API Token Setup](#github-api-token-setup)
    * [PostgreSQL Database Setup](#postgresql-database-setup)
5.  [How to Run](#how-to-run)
6.  [Project Structure](#project-structure)
7.  [Future Enhancements](#future-enhancements)

## Project Overview

This ETL pipeline automates the process of gathering structured data from GitHub. It's built to be robust, handling API rate limits, transforming raw data, and efficiently loading it into a relational database for analysis or further use. Currently, it focuses on extracting core metadata for specified GitHub repositories.

## Features

* **Extract:** Fetches repository metadata from the GitHub GraphQL API.
    * Handles API rate limits and retries using multiple GitHub Personal Access Tokens (PATs).
* **Transform:** Processes raw JSON data from the API into a flattened, structured format suitable for database insertion. Currently focuses on project-level metadata.
* **Load:** Inserts transformed data into a PostgreSQL database.
    * Utilizes `psycopg2.extras.execute_values` for efficient batch insertions/upserts.
    * Ensures database tables are created if they don't exist.
* **Configuration:** Uses environment variables (`.env` file) for sensitive credentials and configurable parameters.
* **Logging:** Detailed error logging to a `pipeline_failures.log` file for easy debugging.
* **Progress Tracking:** Provides a `tqdm` progress bar in the console for real-time monitoring.

## Technologies Used

* **Python 3.x**
* **`requests`**: For making HTTP requests to the GitHub API.
* **`psycopg2`**: PostgreSQL database adapter for Python.
* **`python-dotenv`**: For managing environment variables.
* **`tqdm`**: For displaying progress bars.
* **PostgreSQL**: Relational database for data storage.

## Setup Instructions

### Prerequisites

* Python 3.8+ installed.
* Git installed.
* PostgreSQL server installed and running locally or accessible remotely.
* A GitHub Personal Access Token (PAT) with `repo` or `public_repo` scope.

### Clone the Repository

First, clone this repository to your local machine:

```bash
git clone [https://github.com/SV592/repo_pipeline.git](https://github.com/SV592/repo_pipeline.git)
cd repo_pipeline

```

### Install Dependencies

With your virtual environment activated, install the required Python packages:

Bash

```
pip install -r requirements.txt

```

### GitHub API Token Setup

1.  **Generate a GitHub PAT:**

    -   Go to your GitHub settings: `Settings` -> `Developer settings` -> `Personal access tokens` -> `Tokens (classic)` -> `Generate new token`.

    -   Give it a descriptive name (e.g., `repo_pipeline_token`).

    -   Grant it the `repo` scope (or `public_repo` if you only plan to access public repositories).

    -   **Copy the token immediately** as you won't be able to see it again.

2.  **Create a `.env` file:** In the root directory of your `repo_pipeline` project (the same directory as `main.py`), create a new file named `.env`.

3.  **Add your GitHub Token to `.env`:** Add the following line to your `.env` file, replacing `YOUR_GITHUB_PAT` with the token you generated:

    Code snippet

    ```
    GITHUB_APP_INSTALLATION_TOKENS="YOUR_GITHUB_PAT"

    ```

    *If you have multiple tokens for higher rate limits, separate them with commas:*

    Code snippet

    ```
    GITHUB_APP_INSTALLATION_TOKENS="ghs_TOKEN1,ghs_TOKEN2,ghs_TOKEN3"

    ```

4.  **Ensure `.env` is in `.gitignore`:** Make sure your `.gitignore` file (also in the project root) contains the line `/.env` to prevent accidentally committing your token to version control.

### PostgreSQL Database Setup

1.  **Ensure PostgreSQL Server is Running:** Verify your PostgreSQL server is active.

2.  **Create the Database:** You need to create a database named `github_data` (or whatever you set `DB_NAME` to in `.env`).

    -   **Using `psql` (command line):**

        Bash

        ```
        psql -U postgres
        # Enter your postgres user password when prompted
        CREATE DATABASE github_data;
        \q

        ```

    -   **Using pgAdmin 4 (GUI):**

        -   Open pgAdmin 4, connect to your server.

        -   Right-click on "Databases" -> "Create" -> "Database...".

        -   Enter `github_data` as the "Database" name and click "Save".

3.  **Configure `.env` for Database Connection:** Add the following lines to your `.env` file, adjusting values if your PostgreSQL setup is different (e.g., different user, password, or host):

    Code snippet

    ```
    DB_HOST="localhost"
    DB_NAME="github_data"
    DB_USER="postgres"
    DB_PASSWORD="your_postgres_password"
    DB_PORT="5432"

    ```

How to Run
----------

1.  **Activate your virtual environment** (if not already active).

2.  **Ensure `repos.csv` exists** in the project root with the correct format (`name`, `num_downloads`, `owners_and_repo`). An example `repos.csv` might look like:

    Code snippet

    ```
    name,num_downloads,owners_and_repo
    supports-color,221147395,chalk/supports-color
    semver,212597945,npm/node-semver

    ```

3.  **Run the main pipeline script:**

    Bash

    ```
    python main.py

    ```

You will see a progress bar in your console. Any errors or detailed logs will be written to `pipeline_failures.log`. Upon successful completion, a summary message will be printed to the console.

Project Structure
-----------------

```
repo_pipeline/
├── .env                  # Environment variables
├── .gitignore            # Specifies files/folders to ignore
├── main.py               # Orchestrates the ETL pipeline
├── config.py             # Loads configurations and environment variables
├── extractor.py          # Handles GitHub GraphQL API extraction
├── data_transformer.py   # Transforms raw GitHub data (project metadata only)
├── pg_loader.py          # Loads transformed data into PostgreSQL
├── repos.csv             # Input CSV file with repositories to process
└── pipeline_failures.log # Log file for errors and pipeline events
```
