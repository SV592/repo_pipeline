import csv
import logging
from tqdm import tqdm
import sys

from config import GITHUB_GRAPHQL_API_URL, GITHUB_API_TOKENS, FAILURE_LOG_FILE
from extractor import GitHubExtractor
from transformer import GitHubDataTransformer


# Initially set to INFO to allow the "Successfully loaded" message
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Set the logging level for the extractor to CRITICAL.
logging.getLogger("extractor").setLevel(logging.CRITICAL)

# Set the logging level for the data_transformer to CRITICAL.
logging.getLogger("transformer").setLevel(logging.CRITICAL)

# Add a file handler for logging failures to a specified file
file_handler = logging.FileHandler(FAILURE_LOG_FILE)
file_handler.setLevel(logging.ERROR)  # Only log ERRORs to the file
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

    csv_file_path = "repos.csv"
    repositories_to_process = load_repositories_from_csv(csv_file_path)

    # After loading repositories and printing the "Successfully loaded" message,
    logger.setLevel(logging.ERROR)

    if not repositories_to_process:
        logger.error("No repositories to process. Exiting.")
        return

    failed_extraction_count = 0
    failed_transformation_count = 0

    with tqdm(
        repositories_to_process, desc="Processing Repositories", unit="repo"
    ) as pbar:
        for repo_info in pbar:
            owner = repo_info["owner"]
            name = repo_info["name"]
            repo_identifier = f"{owner}/{name}"  # For logging purposes

            repo_data = extractor.fetch_repository_metadata(owner, name)

            if repo_data:
                transformed_repo_data = transformer.transform_repository_metadata(
                    repo_data
                )

                if transformed_repo_data:
                    # Data is now extracted and transformed.
                    pass
                else:
                    failed_transformation_count += 1
                    # Log to file: Transformation failed for this repo
                    logger.error(
                        f"Transformation failed for repository: {repo_identifier}. Raw data: {repo_data}"
                    )
            else:
                failed_extraction_count += 1
                # Log to file: Extraction failed for this repo
                logger.error(f"Extraction failed for repository: {repo_identifier}.")

            # Update the postfix of the tqdm bar once per iteration, after all counts are updated
            pbar.set_postfix_str(
                f"Extract Fail: {failed_extraction_count}, Transform Fail: {failed_transformation_count}"
            )


if __name__ == "__main__":
    main()
