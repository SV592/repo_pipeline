import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class GitHubDataTransformer:
    """
    Transforms raw GitHub GraphQL API response data into a standardized format
    suitable for loading into a relational database.
    """

    def __init__(self):
        logger.info("GitHubDataTransformer initialized.")

    def transform_repository_metadata(self, raw_repo_data: dict) -> dict | None:
        """
        Transforms raw repository metadata from GitHub GraphQL into a flat dictionary
        suitable for a 'projects' table.

        Args:
            raw_repo_data (dict): The raw dictionary of repository data from the GraphQL API.

        Returns:
            dict | None: A dictionary with transformed data, or None if transformation fails.
        """
        if not raw_repo_data:
            logger.debug("No raw repository data provided for transformation.")
            return None

        try:
            transformed_data = {
                "id": raw_repo_data.get("id"),
                "name": raw_repo_data.get("name"),
                "owner_login": raw_repo_data.get("owner", {}).get("login"),
                "description": raw_repo_data.get("description"),
                "stargazer_count": raw_repo_data.get("stargazerCount"),
                "fork_count": raw_repo_data.get("forkCount"),
                "primary_language": raw_repo_data.get("primaryLanguage", {}).get(
                    "name"
                ),
                "created_at": self._parse_datetime(raw_repo_data.get("createdAt")),
                "pushed_at": self._parse_datetime(raw_repo_data.get("pushedAt")),
                "license_name": raw_repo_data.get("licenseInfo", {}).get("name"),
                "is_archived": raw_repo_data.get("isArchived"),
                "is_disabled": raw_repo_data.get("isDisabled"),
                "is_fork": raw_repo_data.get("isFork"),
                "url": raw_repo_data.get("url"),
                # Timestamp for when this data was last processed
                "last_extracted_at": datetime.now(datetime.now().astimezone().tzinfo),
            }
            logger.debug(f"Transformed data for repo ID {transformed_data.get('id')}.")
            return transformed_data
        except Exception as e:
            repo_id = raw_repo_data.get("id", "Unknown ID")
            repo_name = raw_repo_data.get("name", "Unknown Name")
            logger.error(
                f"Error transforming repository metadata for {repo_name} (ID: {repo_id}): {e}"
            )
            return None

    def _parse_datetime(self, datetime_str: str) -> datetime | None:
        """
        Parses an ISO 8601 datetime string into a datetime object.
        Handles GitHub's Z suffix for UTC.

        Args:
            datetime_str (str): The datetime string from GitHub API.

        Returns:
            datetime | None: A timezone-aware datetime object, or None if parsing fails.
        """
        if not datetime_str:
            return None
        try:
            # Replace 'Z' with '+00:00' for datetime.fromisoformat
            return datetime.fromisoformat(datetime_str.replace("Z", "+00:00"))
        except ValueError as e:
            logger.warning(f"Could not parse datetime string '{datetime_str}': {e}")
            return None
