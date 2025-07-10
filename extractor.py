import requests
import json
import time
import logging
from datetime import datetime, timezone
import itertools

# Logging for the extractor
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class GitHubExtractor:
    """
    Handles extraction of data from the GitHub GraphQL API, including
    authentication, making requests, and managing API rate limits.
    Supports cycling through multiple API tokens.
    """

    def __init__(self, api_url: str, api_tokens: list[str]):
        """
        Initializes the GitHubExtractor with API URL and a list of authentication tokens.

        Args:
            api_url (str): The URL for the GitHub GraphQL API.
            api_tokens (list[str]): A list of GitHub API tokens (preferably installation tokens).
        """
        self.api_url = api_url
        if not api_tokens:
            raise ValueError("At least one API token must be provided.")
        self.api_tokens = api_tokens
        # Use itertools.cycle to endlessly cycle through the tokens
        self._token_cycler = itertools.cycle(self.api_tokens)
        self._current_token = next(self._token_cycler)  # Get the first token
        self._set_headers()
        logger.info(f"GitHubExtractor initialized with {len(self.api_tokens)} tokens.")

    def _set_headers(self):
        """Sets the Authorization header with the current token."""
        self.headers = {
            "Authorization": f"Bearer {self._current_token}",
            "Content-Type": "application/json",
        }

    def _cycle_token(self):
        """Cycles to the next API token in the list."""
        self._current_token = next(self._token_cycler)
        self._set_headers()
        logger.info(f"Switched to a new API token.")

    def _make_graphql_request(
        self, query: str, variables: dict = None, retries: int = 3
    ) -> dict:
        """
        Makes a POST request to the GitHub GraphQL API with retry and token cycling logic.

        Args:
            query (str): The GraphQL query string.
            variables (dict, optional): A dictionary of variables for the query. Defaults to None.
            retries (int): Number of retries for transient errors.

        Returns:
            dict: The JSON response from the API.

        Raises:
            requests.exceptions.RequestException: For persistent network-related errors.
            ValueError: For persistent API errors or unexpected response structures.
        """
        for attempt in range(retries + 1):
            try:
                response = requests.post(
                    self.api_url, headers=self.headers, json=payload
                )
                response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
                data = response.json()

                # Handle GitHub GraphQL specific errors
                if "errors" in data:
                    error_messages = [
                        err.get("message", "Unknown error") for err in data["errors"]
                    ]
                    logger.error(
                        f"GraphQL API returned errors: {'; '.join(error_messages)}"
                    )
                    # Check for rate limit specific errors within GraphQL response
                    if any(
                        "rate limit exceeded" in msg.lower() for msg in error_messages
                    ):
                        logger.warning(
                            "GraphQL rate limit error detected. Cycling token and retrying."
                        )
                        self._cycle_token()
                        if attempt < retries:
                            time.sleep(2**attempt)  # Exponential backoff before retry
                            continue
                    raise ValueError(f"GraphQL API errors: {'; '.join(error_messages)}")

                # Check and handle rate limits from headers
                if "rateLimit" in data.get("data", {}):
                    self._handle_rate_limit(data["data"]["rateLimit"])

                return data

            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code
                logger.error(
                    f"HTTP error during GraphQL request (Status: {status_code}): {e.response.text}"
                )
                if status_code == 429:  # Too Many Requests
                    logger.warning(
                        "HTTP 429 (Too Many Requests) received. Cycling token and retrying."
                    )
                    self._cycle_token()
                if attempt < retries:
                    time.sleep(2**attempt)  # Exponential backoff
                    continue
                raise  # Re-raise if all retries fail
            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
            ) as e:
                logger.error(
                    f"Network error during GraphQL request (Attempt {attempt + 1}/{retries + 1}): {e}"
                )
                if attempt < retries:
                    time.sleep(2**attempt)  # Exponential backoff
                    continue
                raise  # Re-raise if all retries fail
            except json.JSONDecodeError as e:
                logger.error(
                    f"Failed to decode JSON response: {e}. Response text: {response.text}"
                )
                raise
            except Exception as e:
                logger.error(
                    f"An unexpected error occurred during GraphQL request: {e}"
                )
                raise

        raise Exception("Failed to make GraphQL request after multiple retries.")

    def _handle_rate_limit(self, rate_limit_data: dict):
        """
        Checks GitHub API rate limit data from the GraphQL response and pauses execution if necessary.
        Also triggers token cycling if remaining tokens are low.

        Args:
            rate_limit_data (dict): The 'rateLimit' object from the GraphQL response.
        """
        limit = rate_limit_data.get("limit", 0)
        cost = rate_limit_data.get("cost", 0)
        remaining = rate_limit_data.get("remaining", 0)
        reset_at_str = rate_limit_data.get("resetAt")

        reset_time_utc = (
            datetime.fromisoformat(reset_at_str.replace("Z", "+00:00")).timestamp()
            if reset_at_str
            else time.time()
        )

        logger.info(
            f"API Rate Limit Status (Current Token): Cost={cost}, Remaining={remaining}/{limit}, Reset at={datetime.fromtimestamp(reset_time_utc, tz=timezone.utc)}"
        )

        # If remaining is below a threshold, or exhausted, cycle token or sleep
        if remaining < 100:
            if len(self.api_tokens) > 1:
                logger.warning(
                    f"Current token's rate limit low ({remaining}). Cycling to next token."
                )
                self._cycle_token()
            else:
                sleep_duration = (
                    max(0, reset_time_utc - time.time()) + 10
                )  # Add a buffer
                logger.warning(
                    f"Rate limit low and no other tokens available. Sleeping for {sleep_duration:.2f} seconds until reset."
                )
                time.sleep(sleep_duration)
                logger.info("Resuming after rate limit sleep.")
        elif remaining == 0:
            if len(self.api_tokens) > 1:
                logger.warning(
                    "Current token's rate limit exhausted. Cycling to next token."
                )
                self._cycle_token()
            else:
                sleep_duration = max(0, reset_time_utc - time.time()) + 10
                logger.warning(
                    f"Rate limit exhausted and no other tokens available. Sleeping for {sleep_duration:.2f} seconds until reset."
                )
                time.sleep(sleep_duration)
                logger.info("Resuming after rate limit sleep.")

    def fetch_repository_metadata(self, owner: str, name: str) -> dict | None:
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
            # Handle rate limit
            return response_data.get("data", {}).get("repository")
        except Exception as e:
            logger.error(f"Failed to fetch metadata for {owner}/{name}: {e}")
            return None
