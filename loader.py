# pg_loader.py
import psycopg2
from psycopg2 import extras  # For batch inserts
from psycopg2 import sql  # For safe SQL query composition
import logging
from typing import List, Dict, Any
from datetime import datetime, timezone  # Import timezone for handling datetime objects

# Configure logging for the PostgreSQL loader
# Set level to INFO for internal debugging within the pg_loader module.
# The main.py script will set this logger to CRITICAL for console output.
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
                self.conn.autocommit = False  # We'll manage transactions manually
                logger.info("Successfully connected to PostgreSQL database.")
            except psycopg2.Error as e:
                logger.error(f"Error connecting to PostgreSQL database: {e}")
                self.conn = None
                raise  # Re-raise to indicate connection failure

    def close(self):
        """
        Closes the database connection.
        """
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("PostgreSQL database connection closed.")

    def _execute_query(
        self, query: str | sql.Composed, params: tuple = None, commit: bool = False
    ):
        """
        Executes a SQL query. Can accept a string or a psycopg2.sql.Composed object.

        Args:
            query (str | sql.Composed): The SQL query string or composed object.
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
            for (
                sql_query
            ) in (
                create_table_sqls
            ):  # Renamed variable to avoid conflict with imported 'sql'
                self._execute_query(sql_query, commit=True)
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

        # Explicitly define columns to ensure order and correctness
        project_columns = [
            "id",
            "name",
            "owner_login",
            "description",
            "stargazer_count",
            "fork_count",
            "primary_language",
            "created_at",
            "pushed_at",
            "license_name",
            "is_archived",
            "is_disabled",
            "is_fork",
            "url",
            "last_extracted_at",
        ]

        # Use psycopg2.sql.Identifier for column names for safety
        columns_sql = sql.SQL(", ").join(map(sql.Identifier, project_columns))

        # Build the SET clause for ON CONFLICT DO UPDATE
        update_set_parts = []
        for col in project_columns:
            if col != "id":  # Exclude 'id' from update as it's the primary key
                update_set_parts.append(
                    sql.SQL("{} = EXCLUDED.{}").format(
                        sql.Identifier(col), sql.Identifier(col)
                    )
                )
        update_set_sql = sql.SQL(", ").join(update_set_parts)

        # The main SQL query with a single %s placeholder for execute_values
        upsert_sql = sql.SQL(
            """
            INSERT INTO projects ({})
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                {};
        """
        ).format(columns_sql, update_set_sql)

        # Prepare the data as a list of tuples, ensuring the order matches project_columns
        data_to_insert = [
            tuple(item.get(col) for col in project_columns) for item in projects_data
        ]

        try:
            self.connect()  # Ensure connection is open
            with self.conn.cursor() as cur:
                # execute_values expects the query as a string or Composed object,
                # and the data as a list of tuples.
                extras.execute_values(cur, upsert_sql, data_to_insert, page_size=1000)
                self.conn.commit()
            logger.info(
                f"Successfully loaded/updated {len(projects_data)} project records."
            )
        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Error loading project data batch: {e}")
            raise  # Re-raise to propagate the error
        except Exception as e:
            self.conn.rollback()  # Ensure rollback on unexpected errors too
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

        topic_columns = ["project_id", "topic"]
        columns_sql = sql.SQL(", ").join(map(sql.Identifier, topic_columns))

        upsert_sql = sql.SQL(
            """
            INSERT INTO project_topics ({})
            VALUES %s
            ON CONFLICT (project_id, topic) DO NOTHING;
        """
        ).format(columns_sql)

        data_to_insert = [
            tuple(item.get(col) for col in topic_columns) for item in topics_data
        ]

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
            self.conn.rollback()
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

        build_config_columns = [
            "project_id",
            "file_path",
            "config_type",
            "parsed_content",
            "raw_content",
        ]

        columns_sql = sql.SQL(", ").join(map(sql.Identifier, build_config_columns))

        update_set_parts = []
        for col in build_config_columns:
            if col not in [
                "project_id",
                "file_path",
            ]:  # Exclude unique constraint columns from update
                update_set_parts.append(
                    sql.SQL("{} = EXCLUDED.{}").format(
                        sql.Identifier(col), sql.Identifier(col)
                    )
                )
        update_set_sql = sql.SQL(", ").join(update_set_parts)

        upsert_sql = sql.SQL(
            """
            INSERT INTO project_build_configs ({})
            VALUES %s
            ON CONFLICT (project_id, file_path) DO UPDATE SET
                {};
        """
        ).format(columns_sql, update_set_sql)

        data_to_insert = [
            tuple(item.get(col) for col in build_config_columns)
            for item in configs_data
        ]

        try:
            self.connect()
            with self.conn.cursor() as cur:
                extras.execute_values(cur, upsert_sql, data_to_insert, page_size=1000)
                self.conn.commit()
            logger.info(
                f"Successfully loaded/updated {len(configs_data)} build config records."
            )
        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Error loading build config data batch: {e}")
            raise
        except Exception as e:
            self.conn.rollback()
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

        dependency_columns = [
            "project_id",
            "package_name",
            "version",
            "dependency_type",
        ]

        columns_sql = sql.SQL(", ").join(map(sql.Identifier, dependency_columns))

        update_set_parts = []
        for col in dependency_columns:
            if col not in [
                "project_id",
                "package_name",
                "dependency_type",
            ]:  # Exclude unique constraint columns from update
                update_set_parts.append(
                    sql.SQL("{} = EXCLUDED.{}").format(
                        sql.Identifier(col), sql.Identifier(col)
                    )
                )
        update_set_sql = sql.SQL(", ").join(update_set_parts)

        upsert_sql = sql.SQL(
            """
            INSERT INTO project_dependencies ({})
            VALUES %s
            ON CONFLICT (project_id, package_name, dependency_type) DO UPDATE SET
                {};
        """
        ).format(columns_sql, update_set_sql)

        data_to_insert = [
            tuple(item.get(col) for col in dependency_columns)
            for item in dependencies_data
        ]

        try:
            self.connect()
            with self.conn.cursor() as cur:
                extras.execute_values(cur, upsert_sql, data_to_insert, page_size=1000)
                self.conn.commit()
            logger.info(
                f"Successfully loaded/updated {len(dependencies_data)} dependency records."
            )
        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Error loading dependency data batch: {e}")
            raise
        except Exception as e:
            self.conn.rollback()
            logger.error(
                f"An unexpected error occurred during dependency data loading: {e}"
            )
            raise
