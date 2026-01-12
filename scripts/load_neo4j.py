#!/usr/bin/env python3
"""
CLI script to load staged data into Neo4j.

Usage:
    python scripts/load_neo4j.py
"""

import sys
from pathlib import Path

import click

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import get_settings
from src.loading import Neo4jLoader
from src.utils import get_logger, setup_logging


@click.command()
@click.option(
    "--staged-dir",
    default=None,
    type=click.Path(path_type=Path),
    help="Staged data directory (overrides config)",
)
@click.option(
    "--neo4j-uri",
    default=None,
    help="Neo4j connection URI (overrides config)",
)
@click.option(
    "--neo4j-user",
    default=None,
    help="Neo4j username (overrides config)",
)
@click.option(
    "--neo4j-password",
    default=None,
    help="Neo4j password (overrides config)",
)
@click.option(
    "--log-level",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]),
    help="Logging level",
)
def main(
    staged_dir: Path,
    neo4j_uri: str,
    neo4j_user: str,
    neo4j_password: str,
    log_level: str,
) -> None:
    """Load staged clinical trials data into Neo4j."""
    setup_logging(log_level)
    logger = get_logger(__name__)
    
    settings = get_settings()
    
    # Use CLI args or config
    staged_path = staged_dir or settings.data.staged_path
    uri = neo4j_uri or settings.neo4j.uri
    user = neo4j_user or settings.neo4j.user
    password = neo4j_password or settings.neo4j.password
    
    logger.info(
        "Starting Neo4j loading",
        staged_path=str(staged_path),
        neo4j_uri=uri,
    )
    
    loader = None
    try:
        loader = Neo4jLoader(
            uri=uri,
            user=user,
            password=password,
            staged_path=staged_path,
        )
        
        stats = loader.load_all()
        logger.info("Loading completed successfully", **stats)
        
    except Exception as e:
        logger.error("Loading failed", error=str(e), exc_info=True)
        sys.exit(1)
    finally:
        if loader:
            loader.close()


if __name__ == "__main__":
    main()

