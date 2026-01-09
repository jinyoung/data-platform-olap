"""Configuration settings for AI Pivot Studio."""
import os
from functools import lru_cache
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings."""
    
    # API Settings
    app_name: str = "AI Pivot Studio"
    debug: bool = True
    
    # OpenAI Settings
    openai_api_key: str = os.getenv(
        "OPENAI_API_KEY", 
        "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"
    )
    openai_model: str = "gpt-4o-mini"
    
    # Database Settings
    database_url: str = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/pivot_studio"
    )
    
    # Query Settings
    query_timeout: int = 30
    max_rows: int = 1000
    
    # Neo4j Settings (for catalog exploration and lineage)
    neo4j_uri: str = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user: str = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password: str = os.getenv("NEO4J_PASSWORD", "12345analyzer")
    neo4j_database: str = os.getenv("NEO4J_DATABASE", "neo4j")
    
    # DW Schema Settings
    dw_schema: str = os.getenv("DW_SCHEMA", "dw")
    
    class Config:
        env_file = ".env"


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()

