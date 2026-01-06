"""
Configuration management using pydantic-settings.
Loads from environment variables and .env file.
"""

import os
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Load .env file explicitly
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(env_path)


class Settings(BaseSettings):
    """Main settings class with all configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # AACT Database
    aact_host: str = Field(default="aact-db.ctti-clinicaltrials.org", alias="AACT_HOST")
    aact_port: int = Field(default=5432, alias="AACT_PORT")
    aact_database: str = Field(default="aact", alias="AACT_DATABASE")
    aact_user: str = Field(default="", alias="AACT_USER")
    aact_password: str = Field(default="", alias="AACT_PASSWORD")

    # Neo4j Database
    neo4j_uri: str = Field(default="bolt://localhost:7687", alias="NEO4J_URI")
    neo4j_user: str = Field(default="neo4j", alias="NEO4J_USER")
    neo4j_password: str = Field(default="password", alias="NEO4J_PASSWORD")

    # Data paths
    data_raw_path: Path = Field(default=Path("./data/raw"), alias="DATA_RAW_PATH")
    data_staged_path: Path = Field(default=Path("./data/staged"), alias="DATA_STAGED_PATH")
    data_graph_path: Path = Field(default=Path("./data/graph"), alias="DATA_GRAPH_PATH")

    # Extraction settings
    extraction_limit: Optional[int] = Field(default=1000, alias="EXTRACTION_LIMIT")
    extraction_phases: List[str] = Field(
        default=["PHASE1", "PHASE2", "PHASE3", "PHASE4", "PHASE1/PHASE2", "PHASE2/PHASE3", "EARLY_PHASE1"],
    )
    extraction_intervention_types: List[str] = Field(
        default=["DRUG", "BIOLOGICAL"],
    )

    # Logging
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    @property
    def aact_connection_string(self) -> str:
        """Build PostgreSQL connection string for AACT."""
        return f"postgresql://{self.aact_user}:{self.aact_password}@{self.aact_host}:{self.aact_port}/{self.aact_database}"

    def ensure_data_paths_exist(self) -> None:
        """Create data directories if they don't exist."""
        for path in [self.data_raw_path, self.data_staged_path, self.data_graph_path]:
            path.mkdir(parents=True, exist_ok=True)


# Wrapper classes for backward compatibility with extractor
class AACTSettings:
    """AACT settings wrapper."""
    def __init__(self, settings: Settings):
        self._settings = settings
    
    @property
    def host(self) -> str:
        return self._settings.aact_host
    
    @property
    def port(self) -> int:
        return self._settings.aact_port
    
    @property
    def database(self) -> str:
        return self._settings.aact_database
    
    @property
    def user(self) -> str:
        return self._settings.aact_user
    
    @property
    def password(self) -> str:
        return self._settings.aact_password
    
    @property
    def connection_string(self) -> str:
        return self._settings.aact_connection_string


class DataSettings:
    """Data settings wrapper."""
    def __init__(self, settings: Settings):
        self._settings = settings
    
    @property
    def raw_path(self) -> Path:
        return self._settings.data_raw_path
    
    @raw_path.setter
    def raw_path(self, value: Path) -> None:
        self._settings.data_raw_path = value
    
    @property
    def staged_path(self) -> Path:
        return self._settings.data_staged_path
    
    @property
    def graph_path(self) -> Path:
        return self._settings.data_graph_path
    
    def ensure_paths_exist(self) -> None:
        self._settings.ensure_data_paths_exist()


class ExtractionSettings:
    """Extraction settings wrapper."""
    def __init__(self, settings: Settings):
        self._settings = settings
    
    @property
    def limit(self) -> Optional[int]:
        return self._settings.extraction_limit
    
    @limit.setter
    def limit(self, value: Optional[int]) -> None:
        self._settings.extraction_limit = value
    
    @property
    def phases(self) -> List[str]:
        return self._settings.extraction_phases
    
    @property
    def intervention_types(self) -> List[str]:
        return self._settings.extraction_intervention_types


class SettingsWrapper:
    """Wrapper providing the expected interface for the extractor."""
    def __init__(self):
        self._settings = Settings()
        self.aact = AACTSettings(self._settings)
        self.data = DataSettings(self._settings)
        self.extraction = ExtractionSettings(self._settings)
        self.log_level = self._settings.log_level


def get_settings() -> SettingsWrapper:
    """Get settings instance."""
    return SettingsWrapper()
