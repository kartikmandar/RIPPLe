import logging
from typing import Any, Optional

from lsst.daf.butler import Butler, DatasetNotFoundError

from ripple.data_access.exceptions import InvalidRepositoryError

class ButlerClient:
    """
    A client for interacting with the LSST Science Pipelines Butler.

    This class provides a simplified interface for querying and retrieving data
    from a Butler repository.
    """

    def __init__(self, repo_path: str, collection: str):
        """
        Initializes the ButlerClient.

        Args:
            repo_path (str): The path to the Butler repository.
            collection (str): The collection to use for data queries.
        """
        self.repo_path = repo_path
        self.collection = collection
        self.butler = self._initialize_butler()

    def _initialize_butler(self) -> Butler:
        """
        Initializes the Butler instance.

        Returns:
            Butler: An instance of the lsst.daf.butler.Butler.
        
        Raises:
            InvalidRepositoryError: If the repository path is invalid.
        """
        try:
            return Butler(self.repo_path, collections=[self.collection])
        except FileNotFoundError:
            logging.error(f"Invalid repository path: {self.repo_path}")
            raise InvalidRepositoryError(f"Invalid repository path: {self.repo_path}")

    def get_calexp(self, visit: int, detector: int) -> Optional[Any]:
        """
        Retrieves a calibrated exposure (calexp).

        Args:
            visit (int): The visit ID.
            detector (int): The detector ID.

        Returns:
            Optional[Any]: The calexp object, or None if not found.
        """
        try:
            return self.butler.get('calexp', visit=visit, detector=detector)
        except DatasetNotFoundError:
            return None

    def get_deepCoadd(self, tract: int, patch: int) -> Optional[Any]:
        """
        Retrieves a deep coadded image (deepCoadd).

        Args:
            tract (int): The tract ID.
            patch (int): The patch ID.

        Returns:
            Optional[Any]: The deepCoadd object, or None if not found.
        """
        try:
            return self.butler.get('deepCoadd', tract=tract, patch=patch)
        except DatasetNotFoundError:
            return None

    def get_source_catalog(self, visit: int, detector: int) -> Optional[Any]:
        """
        Retrieves a source catalog.

        Args:
            visit (int): The visit ID.
            detector (int): The detector ID.

        Returns:
            Optional[Any]: The source catalog object, or None if not found.
        """
        try:
            return self.butler.get('sourceTable', visit=visit, detector=detector)
        except DatasetNotFoundError:
            return None