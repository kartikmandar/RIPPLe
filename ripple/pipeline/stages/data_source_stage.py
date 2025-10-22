import logging
import os
from ripple.utils.logger import Logger
from ripple.pipeline.pipeline_stage import PipelineStage
from ripple.data_access import ButlerClient, ButlerConfig, RSPTAPClient, create_rsp_client
from typing import Any, Dict, Optional

class DataSourceStage(PipelineStage):
    """
    Pipeline stage for data source operations.
    """
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the DataSourceStage.

        Args:
            config (Optional[Dict[str, Any]]): Full configuration dictionary for the pipeline.
                                               The stage will extract its specific configuration.
        """
        super().__init__(config)
        # Extract specific data_source configuration attributes from the full config
        data_source_specific_config = self.config.get('data_source', {})
        self.source_type = data_source_specific_config.get('type', 'default')
        self.source_params = data_source_specific_config.get('params', {})
        self.logger = logging.getLogger(__name__)

        # Store the full data_source config for Butler server access
        self.data_source_config = data_source_specific_config

        # Initialize clients for remote server access
        self.butler_client = None
        self.rsp_tap_client = None

        if self.source_type == 'butler_server':
            # Use PyVO/TAP for external RSP access (recommended approach)
            self._initialize_rsp_tap_client()
        elif self.source_type == 'butler_repo':
            # Use Butler for local repository access
            self._initialize_remote_butler()

    def _initialize_remote_butler(self):
        """Initialize remote Butler client for RSP access."""
        try:
            # Get authentication details from environment, checking for compatibility
            access_token = os.environ.get("RSP_ACCESS_TOKEN") or \
                           os.environ.get("LSST_ACCESS_TOKEN") or \
                           os.environ.get("ACCESS_TOKEN")

            # Validate that an access token is set before proceeding
            if not access_token:
                self.logger.error("RSP_ACCESS_TOKEN (or LSST_ACCESS_TOKEN/ACCESS_TOKEN) environment variable not set. "
                                  "Authentication cannot proceed.")
                self.butler_client = None
                return

            self.logger.info("RSP access token found.")

            # Get RSP configuration from data_source
            server_url = self.data_source_config.get('server_url')
            if not server_url:
                self.logger.error("server_url not found in data_source configuration.")
                self.butler_client = None
                return
            
            auth_method = self.data_source_config.get('auth_method', 'token')
            token_username = self.data_source_config.get('token_username', 'x-oauth-basic')
            collections = self.data_source_config.get('collections', [])

            # Create Butler configuration
            butler_config = ButlerConfig(
                server_url=server_url,
                access_token=access_token,
                token_username=token_username,
                auth_method=auth_method,
                collections=collections
            )

            # Initialize Butler client
            self.butler_client = ButlerClient(config=butler_config)
            self.logger.info(f"Remote Butler client initialized for {server_url}")

        except Exception as e:
            self.logger.error(f"Failed to initialize remote Butler client: {e}")
            self.butler_client = None

    def _initialize_rsp_tap_client(self):
        """Initialize RSP TAP client using PyVO for external access."""
        try:
            # Get authentication details from environment
            access_token = os.environ.get("RSP_ACCESS_TOKEN") or \
                           os.environ.get("LSST_ACCESS_TOKEN") or \
                           os.environ.get("ACCESS_TOKEN")

            # Validate that an access token is set before proceeding
            if not access_token:
                self.logger.error("RSP_ACCESS_TOKEN environment variable not set. "
                                  "Authentication cannot proceed.")
                self.rsp_tap_client = None
                return

            self.logger.info("RSP access token found.")

            # Get SIA URL from configuration if available
            sia_url = self.data_source_config.get('sia_url')

            # Initialize RSP TAP client using PyVO
            self.rsp_tap_client = create_rsp_client(access_token=access_token, sia_url=sia_url)

            # Test connection
            if self.rsp_tap_client.test_connection():
                self.logger.info("✓ RSP TAP client initialized successfully")
            else:
                self.logger.error("✗ RSP TAP connection test failed")
                self.rsp_tap_client = None

        except Exception as e:
            self.logger.error(f"Failed to initialize RSP TAP client: {e}")
            self.rsp_tap_client = None

    def execute(self, data: Any = None) -> Any:
        """
        Execute the data source stage. This method contains the core logic for the stage.

        Args:
            data (Any, optional): Input data for the stage. Defaults to None.

        Returns:
            Any: Output data from the stage.
        """
        self.logger.info(f"Executing Data Source Stage (Type: {self.source_type})")

        if self.source_type == 'data_folder':
            data_path = self.source_params.get('path')
            self.logger.info(f"Processing data_folder source with path: {data_path}")
            if data_path and os.path.isdir(data_path):
                self.logger.info(f"Data folder found at: {data_path}")
                # Placeholder: In a real scenario, you would load or process data from this folder
                # For now, we'll just confirm its existence.
                data = {"status": "success", "message": f"Data folder verified at {data_path}", "path": data_path}
            else:
                self.logger.error(f"Data folder not found or path not provided: {data_path}")
                # Optionally, raise an exception or return an error status
                data = {"status": "error", "message": f"Data folder not found at {data_path}"}

        elif self.source_type == 'butler_repo':
            repo_path = self.source_params.get('path')
            self.logger.info(f"Processing butler_repo source with path: {repo_path}")
            if repo_path and os.path.isdir(repo_path):
                # A more robust check for a Butler repository might involve looking for specific files
                # like 'butler.yaml' or a '.butler' directory.
                # For now, we'll check if the path is a directory.
                self.logger.info(f"Butler repository directory found at: {repo_path}")
                # Placeholder: In a real scenario, you would connect to the Butler repository
                # and possibly fetch data or metadata.
                data = {"status": "success", "message": f"Butler repository verified at {repo_path}", "path": repo_path}
            else:
                self.logger.error(f"Butler repository not found or path not provided: {repo_path}")
                # Optionally, raise an exception or return an error status
                data = {"status": "error", "message": f"Butler repository not found at {repo_path}"}

        elif self.source_type == 'butler_server':
            self.logger.info("Processing butler_server source (RSP)")
            if self.rsp_tap_client:
                # Use PyVO/TAP client for RSP access
                self.logger.info("Using RSP TAP client for external access")

                # Get configuration
                collections = self.data_source_config.get('collections', [])
                server_url = self.data_source_config.get('server_url', 'unknown')

                # Test TAP connection
                if self.rsp_tap_client.test_connection():
                    self.logger.info("✓ RSP TAP connection successful")

                    # Get available tables to verify access
                    try:
                        tables = self.rsp_tap_client.list_available_tables()[:10]  # First 10 tables
                        self.logger.info(f"✓ Found {len(tables)} tables available")

                        data = {
                            "status": "success",
                            "message": f"RSP TAP server connected at {server_url}",
                            "server_url": server_url,
                            "collections": collections,
                            "tables": tables,
                            "rsp_tap_client": self.rsp_tap_client,
                            "client_type": "pyvo_tap"
                        }
                    except Exception as e:
                        self.logger.warning(f"Could not list tables: {e}")
                        data = {
                            "status": "success",
                            "message": f"RSP TAP server connected at {server_url}",
                            "server_url": server_url,
                            "collections": collections,
                            "rsp_tap_client": self.rsp_tap_client,
                            "client_type": "pyvo_tap"
                        }
                else:
                    self.logger.error("✗ RSP TAP connection failed")
                    data = {"status": "error", "message": "RSP TAP connection failed"}
            else:
                self.logger.error("✗ RSP TAP client not initialized")
                data = {"status": "error", "message": "RSP TAP client not initialized"}

        else:
            self.logger.warning(f"Unsupported data source type: {self.source_type}. Performing no operation.")
            data = {"status": "skipped", "message": f"Unsupported data source type: {self.source_type}"}

        if data.get("status") == "success":
            Logger.success("✓ Data Source Stage completed successfully")
        else:
            Logger.error("✗ Data Source Stage completed with errors")

        return data