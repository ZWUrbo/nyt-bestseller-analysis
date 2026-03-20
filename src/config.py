from __future__ import annotations

from pathlib import Path
from pydantic import BaseModel, Field
from dotenv import load_dotenv
import os

# Load .env if present
load_dotenv()

class Settings(BaseModel):
    # Paths
    project_root: Path = Field(default_factory=lambda: Path(__file__).resolve().parents[1])
    data_dir: Path = Field(default_factory=lambda: Path(__file__).resolve().parents[1] / "data")
    db_path: Path = Field(default_factory=lambda: Path(__file__).resolve().parents[1] / "data" / "interim" / "books.db")

    # API keys
    nyt_api_key: str = Field(default_factory=lambda: os.getenv("NYT_API_KEY", ""))
    hardcover_api_token: str = Field(default_factory=lambda: os.getenv("HARDCOVER_API_TOKEN", ""))
    hardcover_api_url: str = Field(default_factory=lambda: os.getenv("HARDCOVER_API_URL", "https://api.hardcover.app/v1/graphql"))

    # HTTP behavior
    http_cache_path: str = Field(default_factory=lambda: os.getenv("HTTP_CACHE_PATH", "data/interim/http_cache"))
    http_cache_expire_seconds: int = Field(default_factory=lambda: int(os.getenv("HTTP_CACHE_EXPIRE_SECONDS", "86400")))

    # Rate limits
    nyt_rps: float = Field(default_factory=lambda: float(os.getenv("NYT_RPS", "2.0")))
    openlibrary_rps: float = Field(default_factory=lambda: float(os.getenv("OPENLIBRARY_RPS", "5.0")))
    hardcover_rps: float = Field(default_factory=lambda: float(os.getenv("HARDCOVER_RPS", "0.8")))

    # Ingestion scope defaults
    start_year: int = Field(default_factory=lambda: int(os.getenv("START_YEAR", "2021")))
    end_year: int = Field(default_factory=lambda: int(os.getenv("END_YEAR", "2024")))

    # Email
    contact_email: str = Field(default_factory=lambda: os.getenv("CONTACT_EMAIL", ""))

    def ensure_dirs(self) -> None:
        (self.data_dir / "raw").mkdir(parents=True, exist_ok=True)
        (self.data_dir / "interim").mkdir(parents=True, exist_ok=True)
        (self.data_dir / "processed").mkdir(parents=True, exist_ok=True)

settings = Settings()
