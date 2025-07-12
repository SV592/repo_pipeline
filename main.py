import csv
import logging
from tqdm import tqdm
import sys

from config import GITHUB_GRAPHQL_API_URL, GITHUB_API_TOKENS, FAILURE_LOG_FILE
from extractor import GitHubExtractor
from transformer import GitHubDataTransformer
from loader import PostgreSQLDataLoader
from config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT

# Prevents UnicodeEncodeError on Windows terminals when printing or logging.
try:
    if sys.stdout.encoding != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8")
    if sys.stderr.encoding != "utf-8":
        sys.stderr.reconfigure(encoding="utf-8")
except Exception as e:
    # Cases where reconfigure might not be available
    print(
        f"Warning: Could not reconfigure stdout/stderr to UTF-8: {e}. Output encoding issues might persist."
    )


# Configure logging for the main script
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[],  # Prevents default console handler from being created by basicConfig
)

# Get the logger for this module (e.g., 'main')
logger = logging.getLogger(__name__)

root_logger = logging.getLogger()
for handler in root_logger.handlers[:]:  # Iterate over a copy to safely modify
    root_logger.removeHandler(handler)
root_logger.propagate = False  # Prevent messages from propagating to higher loggers (which might have console handlers)

# Also ensure the specific module logger doesn't propagate if it somehow got a handler
for handler in logger.handlers[:]:
    logger.removeHandler(handler)
logger.propagate = False


# Set the logging level for specific modules to CRITICAL to suppress their console output
logging.getLogger("extractor").setLevel(logging.CRITICAL)
logging.getLogger("data_transformer").setLevel(
    logging.CRITICAL
)  # Keep this at CRITICAL for now
logging.getLogger("pg_loader").setLevel(logging.CRITICAL)


# Add a file handler for logging failures to a specified file
file_handler = logging.FileHandler(FAILURE_LOG_FILE, encoding="utf-8")
file_handler.setLevel(logging.ERROR)  # Only log ERRORs to this file handler
file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)


def load_repositories_from_csv(file_path: str) -> list[dict]:
    """
    Loads repository data from a CSV file with the structure:
    'name', 'num_downloads', 'owners_and_repo'.
    It extracts 'owner' and 'name' from the 'owners_and_repo' column.

    Args:
        file_path (str): The path to the CSV file.

    Returns:
        list[dict]: A list of dictionaries, each containing 'owner' and 'name'.
    """
    repositories = []
    try:
        with open(file_path, mode="r", newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            # Check for expected columns in the new structure
            required_columns = ["name", "num_downloads", "owners_and_repo"]
            if not all(col in reader.fieldnames for col in required_columns):
                raise ValueError(
                    f"CSV file must contain all required columns: {', '.join(required_columns)}"
                )

            for row_num, row in enumerate(
                reader, start=2
            ):  # Start from 2 for line numbers
                owners_and_repo_raw = row.get(
                    "owners_and_repo", ""
                ).strip()  # Use .get() with default for safety

                if not owners_and_repo_raw:
                    # Use debug level
                    logger.debug(
                        f"Skipping row {row_num} due to empty 'owners_and_repo' value."
                    )
                    continue

                if "/" in owners_and_repo_raw:
                    owner, repo_name = owners_and_repo_raw.split(
                        "/", 1
                    )  # Split only on the first '/'

                    # Ensure both owner and repo_name are not empty after splitting
                    if not owner.strip() or not repo_name.strip():
                        # Use debug level
                        logger.debug(
                            f"Skipping row {row_num} due to incomplete 'owner/name' format after split: '{owners_and_repo_raw}'"
                        )
                        continue

                    repositories.append(
                        {"owner": owner.strip(), "name": repo_name.strip()}
                    )
                else:
                    # Use debug level
                    logger.debug(
                        f"Skipping row {row_num} due to invalid 'owners_and_repo' format (no slash found): '{owners_and_repo_raw}'"
                    )

        print(f"Successfully loaded {len(repositories)} repositories from {file_path}.")

    except FileNotFoundError:
        logger.error(f"Error: CSV file not found at {file_path}")
    except ValueError as e:
        logger.error(f"Error reading CSV: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading CSV: {e}")
    return repositories


def main():
    """
    Main function to demonstrate the GitHubExtractor with multiple repositories
    loaded from a CSV file and tqdm.
    """
    if not GITHUB_API_TOKENS:
        logger.error(
            "GitHub API tokens not found. Please set GITHUB_APP_INSTALLATION_TOKENS in your .env file."
        )
        return

    extractor = GitHubExtractor(GITHUB_GRAPHQL_API_URL, GITHUB_API_TOKENS)
    transformer = GitHubDataTransformer()
    db_loader = PostgreSQLDataLoader(DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT)

    # Ensure tables exist before starting the main processing loop
    try:
        db_loader.create_tables()
    except Exception as e:
        logger.critical(f"Failed to create database tables. Exiting: {e}")
        sys.exit(1)  # Exit if table creation fails

    csv_file_path = "repos.csv"
    repositories_to_process = load_repositories_from_csv(csv_file_path)

    # Set the logger level for this module. With propagation disabled and no console handlers,
    logger.setLevel(logging.ERROR)

    if not repositories_to_process:
        logger.error("No repositories to process. Exiting.")
        return

    failed_extraction_count = 0
    failed_transformation_count = 0

    # Counters for successful loads for each table
    successful_project_loads = 0
    successful_topic_loads = 0
    successful_build_config_loads = 0
    successful_dependency_loads = 0

    # Batching for database inserts for each table type
    batch_size = 100  # Adjust based on performance and API limits
    projects_batch = []
    topics_batch = []
    build_configs_batch = []
    dependencies_batch = []

    with tqdm(
        repositories_to_process, desc="Processing Repositories", unit="repo"
    ) as pbar:
        for repo_info in pbar:
            owner = repo_info["owner"]
            name = repo_info["name"]
            repo_identifier = f"{owner}/{name}"

            raw_repo_data = extractor.fetch_repository_metadata(owner, name)

            if raw_repo_data:
                # Use transform_all_data to get all structured data
                transformed_data = transformer.transform_all_data(raw_repo_data)

                if transformed_data[
                    "projects"
                ]:  # Check if core project data transformed successfully
                    projects_batch.append(transformed_data["projects"])
                    topics_batch.extend(transformed_data["topics"])
                    build_configs_batch.extend(transformed_data["build_configs"])
                    dependencies_batch.extend(transformed_data["dependencies"])

                    # Check if any batch is full or if it's the last repository
                    is_last_repo = pbar.n == len(repositories_to_process) - 1

                    # Load projects batch
                    if len(projects_batch) >= batch_size or is_last_repo:
                        try:
                            db_loader.load_project_data(projects_batch)
                            successful_project_loads += len(projects_batch)
                            projects_batch = []
                        except Exception as e:
                            logger.error(
                                f"Failed to load project batch to DB. Error: {e}. Batch size: {len(projects_batch)}"
                            )
                            projects_batch = []  # Clear batch even on failure

                    # Load topics batch
                    if len(topics_batch) >= batch_size or is_last_repo:
                        try:
                            db_loader.load_topics_data(topics_batch)
                            successful_topic_loads += len(topics_batch)
                            topics_batch = []
                        except Exception as e:
                            logger.error(
                                f"Failed to load topics batch to DB. Error: {e}. Batch size: {len(topics_batch)}"
                            )
                            topics_batch = []

                    # Load build configs batch
                    if len(build_configs_batch) >= batch_size or is_last_repo:
                        try:
                            db_loader.load_build_configs_data(build_configs_batch)
                            successful_build_config_loads += len(build_configs_batch)
                            build_configs_batch = []
                        except Exception as e:
                            logger.error(
                                f"Failed to load build configs batch to DB. Error: {e}. Batch size: {len(build_configs_batch)}"
                            )
                            build_configs_batch = []

                    # Load dependencies batch
                    if len(dependencies_batch) >= batch_size or is_last_repo:
                        try:
                            db_loader.load_dependencies_data(dependencies_batch)
                            successful_dependency_loads += len(dependencies_batch)
                            dependencies_batch = []
                        except Exception as e:
                            logger.error(
                                f"Failed to load dependencies batch to DB. Error: {e}. Batch size: {len(dependencies_batch)}"
                            )
                            dependencies_batch = []

                else:
                    failed_transformation_count += 1
                    logger.error(
                        f"Transformation failed for repository: {repo_identifier}. Raw data: {raw_repo_data}"
                    )
            else:
                failed_extraction_count += 1
                logger.error(f"Extraction failed for repository: {repo_identifier}.")

            # Update the postfix of the tqdm bar once per iteration
            pbar.set_postfix_str(
                f"Extract Fail: {failed_extraction_count}, Transform Fail: {failed_transformation_count}, "
                f"Projects Loaded: {successful_project_loads}, Topics Loaded: {successful_topic_loads}, "
                f"Configs Loaded: {successful_build_config_loads}, Deps Loaded: {successful_dependency_loads}"
            )

    # Final check for any remaining items in the batches after the loop finishes
    if projects_batch:
        try:
            db_loader.load_project_data(projects_batch)
            successful_project_loads += len(projects_batch)
        except Exception as e:
            logger.error(
                f"Failed to load final project batch to DB. Error: {e}. Batch size: {len(projects_batch)}"
            )

    if topics_batch:
        try:
            db_loader.load_topics_data(topics_batch)
            successful_topic_loads += len(topics_batch)
        except Exception as e:
            logger.error(
                f"Failed to load final topics batch to DB. Error: {e}. Batch size: {len(topics_batch)}"
            )

    if build_configs_batch:
        try:
            db_loader.load_build_configs_data(build_configs_batch)
            successful_build_config_loads += len(build_configs_batch)
        except Exception as e:
            logger.error(
                f"Failed to load final build configs batch to DB. Error: {e}. Batch size: {len(build_configs_batch)}"
            )

    if dependencies_batch:
        try:
            db_loader.load_dependencies_data(dependencies_batch)
            successful_dependency_loads += len(dependencies_batch)
        except Exception as e:
            logger.error(
                f"Failed to load final dependencies batch to DB. Error: {e}. Batch size: {len(dependencies_batch)}"
            )

    db_loader.close()  # Close the database connection after all processing
    logger.info(
        f"Pipeline finished. "
        f"Extracted Fail: {failed_extraction_count}, Transformed Fail: {failed_transformation_count}, "
        f"Projects Loaded: {successful_project_loads}, Topics Loaded: {successful_topic_loads}, "
        f"Configs Loaded: {successful_build_config_loads}, Deps Loaded: {successful_dependency_loads}"
    )


if __name__ == "__main__":
    main()
