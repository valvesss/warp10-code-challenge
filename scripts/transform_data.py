#!/usr/bin/env python3
"""
CLI script to transform raw data to staged data.

Usage:
    python scripts/transform_data.py
"""

import sys
from pathlib import Path

import click

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import get_settings
from src.transformation import StagedTransformer
from src.utils import get_logger, setup_logging


@click.command()
@click.option(
    "--raw-dir",
    default=None,
    type=click.Path(path_type=Path),
    help="Raw data directory (overrides config)",
)
@click.option(
    "--staged-dir",
    default=None,
    type=click.Path(path_type=Path),
    help="Staged data directory (overrides config)",
)
@click.option(
    "--log-level",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]),
    help="Logging level",
)
def main(
    raw_dir: Path,
    staged_dir: Path,
    log_level: str,
) -> None:
    """Transform raw clinical trials data to staged format."""
    setup_logging(log_level)
    logger = get_logger(__name__)
    
    settings = get_settings()
    
    # Use CLI args or config
    raw_path = raw_dir or settings.data.raw_path
    staged_path = staged_dir or settings.data.staged_path
    
    logger.info(
        "Starting data transformation",
        raw_path=str(raw_path),
        staged_path=str(staged_path),
    )
    
    try:
        transformer = StagedTransformer(raw_path, staged_path)
        stats = transformer.transform_all()
        
        logger.info("Transformation completed successfully", **stats)
        
    except Exception as e:
        logger.error("Transformation failed", error=str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

