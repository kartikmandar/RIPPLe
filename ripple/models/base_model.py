from abc import ABC, abstractmethod

class ModelInterface(ABC):
    """
    Abstract base class for all models in RIPPLe pipeline.
    
    This interface ensures consistent API across different model types
    (DeepLense, custom models, future integrations).
    """
    
    @abstractmethod
    def predict(self, data):
        """Abstract method for model prediction."""
        pass

class BaseModel(ModelInterface):
    """
    A placeholder for the Base Model.
    This class will be implemented in a later phase.
    """
    def __init__(self, config=None):
        self.config = config
    
    def predict(self, data):
        """Placeholder implementation of predict method."""
        raise NotImplementedError("Predict method not implemented yet")