import requests
import json
import time
import logging
from datetime import datetime, timezone

# L:ogging for the extractor
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class GitHubExtractor:
    """
    Handles extraction of data from the GitHub GraphQL API, including
    authentication, making requests, and managing API rate limits.
    """

    def __init__(self, api_url: str, api_token: str):
        """
        Initializes the GitHubExtractor with API URL and authentication token.

        Args:
            api_url (str): The URL for the GitHub GraphQL API.
            api_token (str): The GitHub API token (preferably an installation token).
        """
        self.api_url = api_url
        self.headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
        }
        logger.info("GitHubExtractor initialized.")

    def _make_graphql_request(self, query: str, variables: dict = None) -> dict:
        """
        Makes a POST request to the GitHub GraphQL API.

        Args:
            query (str): The GraphQL query string.
            variables (dict, optional): A dictionary of variables for the query. Defaults to None.

        Returns:
            dict: The JSON response from the API.

        Raises:
            requests.exceptions.RequestException: For network-related errors.
            ValueError: For API errors or unexpected response structures.
        """
        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        try:
            response = requests.post(self.api_url, headers=self.headers, json=payload)
            response.raise_for_status()  # Raise HTTPError for bad responses
            data = response.json()

            # Handle Specific errors
            if "errors" in data:
                error_messages = [
                    err.get("message", "Unknown error") for err in data["errors"]
                ]
                logger.error(
                    f"GraphQL API returned errors: {'; '.join(error_messages)}"
                )
                raise ValueError(f"GraphQL API errors: {'; '.join(error_messages)}")

            return data

        except requests.exceptions.HTTPError as e:
            logger.error(
                f"HTTP error during GraphQL request: {e.response.status_code} - {e.response.text}"
            )
            raise
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error during GraphQL request: {e}")
            raise
        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout error during GraphQL request: {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"An unexpected request error occurred: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(
                f"Failed to decode JSON response: {e}. Response text: {response.text}"
            )
            raise

    def _handle_rate_limit(self, response_headers: dict):
        """
        Checks GitHub API rate limit headers and pauses execution if necessary.

        Args:
            response_headers (dict): The headers from the API response.
        """
        # GitHub GraphQL API
        limit = int(response_headers.get("x-ratelimit-limit", 0))
        remaining = int(response_headers.get("x-ratelimit-remaining", 0))
        reset_time_utc = int(response_headers.get("x-ratelimit-reset", 0))

        logger.info(
            f"API Rate Limit: Remaining = {remaining}/{limit}, Reset at = {datetime.fromtimestamp(reset_time_utc, tz=timezone.utc)}"
        )

        if remaining < 50:  # Threshold to start pausing proactively
            sleep_duration = max(0, reset_time_utc - time.time()) + 5  # Small buffer
            logger.warning(
                f"Rate limit low. Sleeping for {sleep_duration:.2f} seconds until reset."
            )
            time.sleep(sleep_duration)
            logger.info("Resuming after rate limit sleep.")
        elif remaining == 0:
            sleep_duration = max(0, reset_time_utc - time.time()) + 5
            logger.warning(
                f"Rate limit exhausted. Sleeping for {sleep_duration:.2f} seconds until reset."
            )
            time.sleep(sleep_duration)
            logger.info("Resuming after rate limit sleep.")

    def fetch_repository_metadata(self, owner: str, name: str) -> dict:
        """
        Fetches basic metadata for a single GitHub repository.

        Args:
            owner (str): The owner (user or organization) of the repository.
            name (str): The name of the repository.

        Returns:
            dict: A dictionary containing the repository data, or None if not found/error.
        """
        query = """
            query GetRepositoryMetadata($owner: String!, $name: String!) {
                repository(owner: $owner, name: $name) {
                    id
                    name
                    owner {
                        login
                    }
                    description
                    stargazerCount
                    forkCount
                    primaryLanguage {
                        name
                    }
                    createdAt
                    pushedAt
                    licenseInfo {
                        name
                    }
                    isArchived
                    isDisabled
                    isFork
                    url
                }
                rateLimit {
                    limit
                    cost
                    remaining
                    resetAt
                }
            }
        """
        variables = {"owner": owner, "name": name}

        try:
            response_data = self._make_graphql_request(query, variables)
            self._handle_rate_limit(
                response_data["data"]["rateLimit"]
            )  # Pass rateLimit data for handling
            return response_data.get("data", {}).get("repository")
        except Exception as e:
            logger.error(f"Failed to fetch metadata for {owner}/{name}: {e}")
            return None
