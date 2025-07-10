import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# GitHub API Configuration
GITHUB_GRAPHQL_API_URL = "https://api.github.com/graphql"

# For cycling through multiple tokens, define them as a comma-separated string
_github_tokens_str = os.getenv("GITHUB_APP_INSTALLATION_TOKENS")
GITHUB_API_TOKENS = (
    [token.strip() for token in _github_tokens_str.split(",") if token.strip()]
    if _github_tokens_str
    else []
)

# PostgreSQL Database Configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "github_data")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_PORT = os.getenv("DB_PORT", "5432")

#  Validation for tokens
if not GITHUB_API_TOKENS:
    raise ValueError(
        "GITHUB_APP_INSTALLATION_TOKENS environment variable not set or empty. Please provide at least one token."
    )
