import psycopg2
from psycopg2 import extras
import logging
from typing import List, Dict, Any

# Configure logging for the PostgreSQL loader
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class PostgreSQLDataLoader:
    """
    Handles connecting to a PostgreSQL database and loading transformed data.
    Supports batch inserts and upserts for efficient data loading.
    """

    def __init__(self, db_host, db_name, db_user, db_password, db_port):
        """
        Initializes the PostgreSQLDataLoader with database connection parameters.
        """
        self.conn_params = {
            "host": db_host,
            "database": db_name,
            "user": db_user,
            "password": db_password,
            "port": db_port,
        }
        self.conn = None
        logger.info("PostgreSQLDataLoader initialized.")

    def connect(self):
        """
        Establishes a connection to the PostgreSQL database.
        """
        if self.conn is None or self.conn.closed:
            try:
                self.conn = psycopg2.connect(**self.conn_params)
                self.conn.autocommit = False  # Manage transactions manually
                logger.info("Successfully connected to PostgreSQL database.")
            except psycopg2.Error as e:
                logger.error(f"Error connecting to PostgreSQL database: {e}")
                self.conn = None
                raise  # Re-raise for connection failure

    def close(self):
        """
        Closes the database connection.
        """
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("PostgreSQL database connection closed.")

    def _execute_query(self, query: str, params: tuple = None, commit: bool = False):
        """
        Executes a SQL query.

        Args:
            query (str): The SQL query string.
            params (tuple, optional): Parameters for the query. Defaults to None.
            commit (bool, optional): Whether to commit the transaction after execution. Defaults to False.

        Returns:
            list: Fetched rows if it's a SELECT query, otherwise None.
        """
        if self.conn is None or self.conn.closed:
            logger.error("Database connection is not open. Cannot execute query.")
            raise psycopg2.Error("Database connection is not open.")

        try:
            with self.conn.cursor() as cur:
                cur.execute(query, params)
                if commit:
                    self.conn.commit()
                # Attempt to fetch results only if it's a SELECT query
                if cur.description:
                    return cur.fetchall()
                return None
        except psycopg2.Error as e:
            self.conn.rollback()  # Rollback on error
            logger.error(f"Error executing query: {query} - {e}")
            raise  # Re-raise to propagate the error

    def create_tables(self):
        """
        Creates the necessary tables in the PostgreSQL database if they don't exist.
        This is idempotent.
        """
        create_table_sqls = [
            """
            CREATE TABLE IF NOT EXISTS projects (
                id VARCHAR(255) PRIMARY KEY, -- GitHub Node ID
                name VARCHAR(255) NOT NULL,
                owner_login VARCHAR(255) NOT NULL,
                description TEXT,
                stargazer_count INTEGER,
                fork_count INTEGER,
                primary_language VARCHAR(100),
                created_at TIMESTAMP WITH TIME ZONE,
                pushed_at TIMESTAMP WITH TIME ZONE,
                license_name VARCHAR(255),
                is_archived BOOLEAN,
                is_disabled BOOLEAN,
                is_fork BOOLEAN,
                url TEXT,
                last_extracted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS project_topics (
                project_id VARCHAR(255) REFERENCES projects(id) ON DELETE CASCADE,
                topic VARCHAR(255) NOT NULL,
                PRIMARY KEY (project_id, topic)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS project_build_configs (
                id SERIAL PRIMARY KEY,
                project_id VARCHAR(255) REFERENCES projects(id) ON DELETE CASCADE,
                file_path VARCHAR(500) NOT NULL,
                config_type VARCHAR(100), -- e.g., 'package.json', 'pom.xml', 'Dockerfile'
                parsed_content JSONB, -- Store parsed JSON/YAML data here
                raw_content TEXT, -- Store raw file content
                UNIQUE (project_id, file_path)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS project_dependencies (
                id SERIAL PRIMARY KEY,
                project_id VARCHAR(255) REFERENCES projects(id) ON DELETE CASCADE,
                package_name VARCHAR(255) NOT NULL,
                version VARCHAR(255),
                dependency_type VARCHAR(100), -- e.g., 'runtime', 'dev', 'optional'
                UNIQUE (project_id, package_name, dependency_type)
            );
            """,
        ]
        try:
            self.connect()  # Ensure connection is open
            for sql in create_table_sqls:
                self._execute_query(sql, commit=True)
            logger.info("All necessary tables ensured to exist.")
        except psycopg2.Error as e:
            logger.error(f"Failed to create tables: {e}")
            raise

    def load_project_data(self, projects_data: List[Dict]):
        """
        Loads a batch of transformed project data into the 'projects' table.
        Uses UPSERT (INSERT ... ON CONFLICT) to handle existing records.

        Args:
            projects_data (List[Dict]): A list of dictionaries, where each dictionary
                                        represents a transformed project record.
        """
        if not projects_data:
            logger.debug("No project data to load.")
            return

        # Define columns and corresponding values for insertion
        columns = projects_data[0].keys()
        # Create a string of column names
        columns_str = ", ".join(columns)
        # Create a string for value placeholders
        values_str = ", ".join([f"%({col})s" for col in columns])

        # Create a string for ON CONFLICT DO UPDATE SET
        update_set_str = ", ".join(
            [f"{col} = EXCLUDED.{col}" for col in columns if col != "id"]
        )

        upsert_sql = f"""
        INSERT INTO projects ({columns_str})
        VALUES ({values_str})
        ON CONFLICT (id) DO UPDATE SET
            {update_set_str};
        """

        # Prepare the data as a list of tuples or dictionaries for execute_values
        data_to_insert = projects_data

        try:
            self.connect()  # Ensure connection is open
            with self.conn.cursor() as cur:
                # Use execute_values for efficient batch insertion/upsertion
                extras.execute_values(
                    cur, upsert_sql, data_to_insert, template=None, page_size=1000
                )
                self.conn.commit()
            logger.info(
                f"Successfully loaded/updated {len(projects_data)} project records."
            )
        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Error loading project data batch: {e}")
            raise  # Re-raise to propagate the error
        except Exception as e:
            logger.error(
                f"An unexpected error occurred during project data loading: {e}"
            )
            raise

    def load_topics_data(self, topics_data: List[Dict]):
        """
        Loads a batch of transformed topic data into the 'project_topics' table.
        Uses UPSERT to handle existing records.

        Args:
            topics_data (List[Dict]): A list of dictionaries, where each dictionary
                                      represents a transformed topic record.
        """
        if not topics_data:
            logger.debug("No topic data to load.")
            return

        upsert_sql = """
        INSERT INTO project_topics (project_id, topic)
        VALUES (%s, %s)
        ON CONFLICT (project_id, topic) DO NOTHING;
        """

        # Prepare data as list of tuples for execute_values
        data_to_insert = [(item["project_id"], item["topic"]) for item in topics_data]

        try:
            self.connect()
            with self.conn.cursor() as cur:
                extras.execute_values(cur, upsert_sql, data_to_insert, page_size=1000)
                self.conn.commit()
            logger.info(
                f"Successfully loaded/updated {len(topics_data)} topic records."
            )
        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Error loading topic data batch: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred during topic data loading: {e}")
            raise

    def load_build_configs_data(self, configs_data: List[Dict]):
        """
        Loads a batch of transformed build configuration data into the 'project_build_configs' table.
        Uses UPSERT to handle existing records.

        Args:
            configs_data (List[Dict]): A list of dictionaries, where each dictionary
                                       represents a transformed build config record.
        """
        if not configs_data:
            logger.debug("No build config data to load.")
            return

        # Dynamically get columns from the first item, ensuring 'id' is handled for SERIAL PRIMARY KEY
        columns = [key for key in configs_data[0].keys() if key != "id"]
        columns_str = ", ".join(columns)
        values_str = ", ".join([f"%({col})s" for col in columns])

        update_set_str = ", ".join(
            [
                f"{col} = EXCLUDED.{col}"
                for col in columns
                if col not in ["project_id", "file_path"]
            ]
        )

        upsert_sql = f"""
        INSERT INTO project_build_configs ({columns_str})
        VALUES ({values_str})
        ON CONFLICT (project_id, file_path) DO UPDATE SET
            {update_set_str};
        """

        data_to_insert = configs_data

        try:
            self.connect()
            with self.conn.cursor() as cur:
                extras.execute_values(
                    cur, upsert_sql, data_to_insert, template=None, page_size=1000
                )
                self.conn.commit()
            logger.info(
                f"Successfully loaded/updated {len(configs_data)} build config records."
            )
        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Error loading build config data batch: {e}")
            raise
        except Exception as e:
            logger.error(
                f"An unexpected error occurred during build config data loading: {e}"
            )
            raise

    def load_dependencies_data(self, dependencies_data: List[Dict]):
        """
        Loads a batch of transformed dependency data into the 'project_dependencies' table.
        Uses UPSERT to handle existing records.

        Args:
            dependencies_data (List[Dict]): A list of dictionaries, where each dictionary
                                            represents a transformed dependency record.
        """
        if not dependencies_data:
            logger.debug("No dependency data to load.")
            return

        columns = [key for key in dependencies_data[0].keys() if key != "id"]
        columns_str = ", ".join(columns)
        values_str = ", ".join([f"%({col})s" for col in columns])

        update_set_str = ", ".join(
            [
                f"{col} = EXCLUDED.{col}"
                for col in columns
                if col not in ["project_id", "package_name", "dependency_type"]
            ]
        )

        upsert_sql = f"""
        INSERT INTO project_dependencies ({columns_str})
        VALUES ({values_str})
        ON CONFLICT (project_id, package_name, dependency_type) DO UPDATE SET
            {update_set_str};
        """

        data_to_insert = dependencies_data

        try:
            self.connect()
            with self.conn.cursor() as cur:
                extras.execute_values(
                    cur, upsert_sql, data_to_insert, template=None, page_size=1000
                )
                self.conn.commit()
            logger.info(
                f"Successfully loaded/updated {len(dependencies_data)} dependency records."
            )
        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Error loading dependency data batch: {e}")
            raise
        except Exception as e:
            logger.error(
                f"An unexpected error occurred during dependency data loading: {e}"
            )
            raise
