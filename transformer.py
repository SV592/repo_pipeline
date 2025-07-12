# data_transformer.py
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional

# Configure logging for the data transformer
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class GitHubDataTransformer:
    """
    Transforms raw GitHub GraphQL API response data into a structured format
    suitable for loading into a PostgreSQL database. This version focuses only
    on core project metadata.
    """

    def __init__(self):
        logger.info("GitHubDataTransformer initialized.")

    def transform_repository_metadata(
        self, raw_repo_data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Transforms raw repository metadata into a structured format for the 'projects' table.

        Args:
            raw_repo_data (Dict[str, Any]): The raw dictionary containing repository data
                                             from the GitHub GraphQL API.
                                             (This is expected to be the 'repository' object itself,
                                             as returned by the extractor).

        Returns:
            Optional[Dict[str, Any]]: A dictionary with transformed project data,
                                      or None if essential data is missing or transformation fails.
        """
        if not raw_repo_data:
            logger.warning("No raw repository data provided for transformation.")
            return None

        # raw_repo_data is already the repository object, so use it directly.
        repository = raw_repo_data

        try:
            # Extracting core project data
            project_data = {
                "id": repository.get("id"),
                "name": repository.get("name"),
                "owner_login": repository.get("owner", {}).get("login"),
                "description": repository.get("description"),
                "stargazer_count": repository.get("stargazerCount"),
                "fork_count": repository.get("forkCount"),
                "primary_language": (
                    repository.get("primaryLanguage", {}).get("name")
                    if repository.get("primaryLanguage")
                    else None
                ),
                "created_at": (
                    datetime.strptime(
                        repository["createdAt"], "%Y-%m-%dT%H:%M:%SZ"
                    ).replace(tzinfo=timezone.utc)
                    if repository.get("createdAt")
                    else None
                ),
                "pushed_at": (
                    datetime.strptime(
                        repository["pushedAt"], "%Y-%m-%dT%H:%M:%SZ"
                    ).replace(tzinfo=timezone.utc)
                    if repository.get("pushedAt")
                    else None
                ),
                "license_name": (
                    repository.get("licenseInfo", {}).get("name")
                    if repository.get("licenseInfo")
                    else None
                ),
                "is_archived": repository.get("isArchived"),
                "is_disabled": repository.get("isDisabled"),
                "is_fork": repository.get("isFork"),
                "url": repository.get("url"),
                "last_extracted_at": datetime.now(
                    timezone.utc
                ),  # Timestamp of when this data was extracted/transformed
            }

            # Basic validation for essential fields
            if not all(
                [project_data["id"], project_data["name"], project_data["owner_login"]]
            ):
                logger.warning(
                    f"Essential project data missing after transformation attempt for repository: {repository.get('url')}"
                )
                return None

            return project_data

        except KeyError as e:
            logger.error(
                f"Missing expected key in raw repository data during project transformation: {e}. Data received: {repository}"
            )
            return None
        except ValueError as e:
            logger.error(
                f"Date parsing error during project transformation: {e}. Data received: {repository}"
            )
            return None
        except Exception as e:
            logger.error(
                f"An unexpected error occurred during project transformation: {e}. Data received: {repository}"
            )
            return None
