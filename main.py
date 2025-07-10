import csv
import logging
from tqdm import tqdm

from config import GITHUB_GRAPHQL_API_URL, GITHUB_API_TOKENS
from github_extractor import GitHubExtractor

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


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

            for row in reader:
                owners_and_repo = row["owners_and_repo"]
                if "/" in owners_and_repo:
                    owner, repo_name = owners_and_repo.split(
                        "/", 1
                    )  # Split only on the first '/'
                    repositories.append(
                        {"owner": owner.strip(), "name": repo_name.strip()}
                    )
                else:
                    logger.warning(
                        f"Skipping row due to invalid 'owners_and_repo' format: {owners_and_repo}"
                    )

        logger.info(
            f"Successfully loaded {len(repositories)} repositories from {file_path}."
        )
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

    # Path to the CSV file
    csv_file_path = "repos.csv"
    repositories_to_process = load_repositories_from_csv(csv_file_path)

    if not repositories_to_process:
        logger.error("No repositories to process. Exiting.")
        return

    logger.info(
        f"Starting extraction for {len(repositories_to_process)} repositories..."
    )

    # Progress bar
    for repo_info in tqdm(repositories_to_process, desc="Processing Repositories"):
        owner = repo_info["owner"]
        name = repo_info["name"]
        repo_data = extractor.fetch_repository_metadata(owner, name)

        if repo_data:
            pass  # Keep logging minimal
        else:
            logger.warning(f"Skipped {owner}/{name} due to extraction failure.")

    logger.info("Finished processing all specified repositories.")


if __name__ == "__main__":
    main()
