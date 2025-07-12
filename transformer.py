import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

# Configure logging for the data transformer
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class GitHubDataTransformer:
    """
    Transforms raw GitHub GraphQL API response data into a structured format
    suitable for loading into a PostgreSQL database.
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
                                             not the full GraphQL response with 'data' key).

        Returns:
            Optional[Dict[str, Any]]: A dictionary with transformed project data,
                                      or None if essential data is missing.
        """
        if not raw_repo_data:
            logger.warning("No raw repository data provided for transformation.")
            return None

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

    def transform_topics_data(
        self, raw_repo_data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Extracts and transforms topics data for the 'project_topics' table.

        Args:
            raw_repo_data (Dict[str, Any]): The raw dictionary containing repository data.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each representing a topic.
        """
        topics_list = []
        repository = raw_repo_data
        if not repository:
            return []

        project_id = repository.get("id")
        if not project_id:
            logger.warning("Project ID missing for topics transformation.")
            return []

        topics_nodes = repository.get("repositoryTopics", {}).get("nodes", [])
        for topic_node in topics_nodes:
            topic_name = topic_node.get("topic", {}).get("name")
            if topic_name:
                topics_list.append({"project_id": project_id, "topic": topic_name})
        return topics_list

    def transform_build_configs_data(
        self, raw_repo_data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Extracts and transforms build configuration data for the 'project_build_configs' table.
        This is a placeholder; actual parsing of file content would be more complex.

        Args:
            raw_repo_data (Dict[str, Any]): The raw dictionary containing repository data.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each representing a build config.
        """
        build_configs_list = []
        repository = raw_repo_data
        if not repository:
            return []

        project_id = repository.get("id")
        if not project_id:
            logger.warning("Project ID missing for build configs transformation.")
            return []

        build_configs_nodes = repository.get("buildConfigs", {}).get(
            "nodes", []
        )  # Hypothetical field
        for config_node in build_configs_nodes:
            file_path = config_node.get("path")
            config_type = config_node.get("type")
            raw_content = config_node.get("rawContent")
            parsed_content = config_node.get(
                "parsedContent"
            )  # Assuming this is already JSONB-compatible

            if file_path and config_type:
                build_configs_list.append(
                    {
                        "project_id": project_id,
                        "file_path": file_path,
                        "config_type": config_type,
                        "parsed_content": parsed_content,
                        "raw_content": raw_content,
                    }
                )

        return build_configs_list

    def transform_dependencies_data(
        self, raw_repo_data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Extracts and transforms dependency data for the 'project_dependencies' table.
        This is a placeholder; actual dependency parsing would be more complex.

        Args:
            raw_repo_data (Dict[str, Any]): The raw dictionary containing repository data.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each representing a dependency.
        """
        dependencies_list = []
        repository = raw_repo_data
        if not repository:
            return []

        project_id = repository.get("id")
        if not project_id:
            logger.warning("Project ID missing for dependencies transformation.")
            return []

        # Similar to build configs
        dependencies_nodes = repository.get("dependencies", {}).get(
            "nodes", []
        )  # Hypothetical field
        for dep_node in dependencies_nodes:
            package_name = dep_node.get("packageName")
            version = dep_node.get("version")
            dependency_type = dep_node.get("type")

            if package_name:
                dependencies_list.append(
                    {
                        "project_id": project_id,
                        "package_name": package_name,
                        "version": version,
                        "dependency_type": dependency_type,
                    }
                )

        return dependencies_list

    def transform_all_data(self, raw_repo_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Orchestrates the transformation of all relevant data points from raw repository data.

        Args:
            raw_repo_data (Dict[str, Any]): The raw dictionary containing repository data.

        Returns:
            Dict[str, Any]: A dictionary containing lists of transformed data for each table.
                            Keys: 'projects', 'topics', 'build_configs', 'dependencies'.
        """
        transformed_data = {
            "projects": None,
            "topics": [],
            "build_configs": [],
            "dependencies": [],
        }

        # Transform core project data first
        project_data = self.transform_repository_metadata(raw_repo_data)
        if project_data:
            transformed_data["projects"] = project_data
            # Only proceed with sub-transformations if project_data is valid
            transformed_data["topics"] = self.transform_topics_data(raw_repo_data)
            transformed_data["build_configs"] = self.transform_build_configs_data(
                raw_repo_data
            )
            transformed_data["dependencies"] = self.transform_dependencies_data(
                raw_repo_data
            )
        else:
            logger.warning("Skipping sub-transformations due to invalid project data.")

        return transformed_data
