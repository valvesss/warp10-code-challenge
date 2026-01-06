#!/usr/bin/env python3
"""
CLI script to extract data from AACT database.

Usage:
    python scripts/extract_data.py --help
    python scripts/extract_data.py --limit 1000
    python scripts/extract_data.py --tables studies,sponsors,interventions
"""

import sys
from pathlib import Path
from typing import Optional

import click

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import get_settings
from src.ingestion import AACTExtractor
from src.utils import get_logger, setup_logging


@click.command()
@click.option(
    "--limit",
    default=None,
    type=int,
    help="Limit number of studies to extract (overrides config)",
)
@click.option(
    "--tables",
    default=None,
    type=str,
    help="Comma-separated list of tables to extract",
)
@click.option(
    "--output-dir",
    default=None,
    type=click.Path(path_type=Path),
    help="Output directory for extracted data",
)
@click.option(
    "--log-level",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]),
    help="Logging level",
)
def main(
    limit: Optional[int],
    tables: Optional[str],
    output_dir: Optional[Path],
    log_level: str,
) -> None:
    """Extract clinical trials data from AACT database."""
    # Setup logging
    setup_logging(log_level)
    logger = get_logger(__name__)

    # Load settings
    settings = get_settings()

    # Override settings if CLI args provided
    if limit is not None:
        settings.extraction.limit = limit

    if output_dir is not None:
        settings.data.raw_path = output_dir

    # Ensure output directory exists
    settings.data.ensure_paths_exist()

    logger.info(
        "Starting AACT data extraction",
        limit=settings.extraction.limit,
        output_dir=str(settings.data.raw_path),
    )

    # Parse tables if provided
    table_list = None
    if tables:
        table_list = [t.strip() for t in tables.split(",")]
        logger.info("Extracting specific tables", tables=table_list)

    try:
        # Initialize extractor
        extractor = AACTExtractor(settings)

        # Run extraction
        stats = extractor.extract_all(tables=table_list)

        logger.info(
            "Extraction completed successfully",
            **stats,
        )

    except Exception as e:
        logger.error("Extraction failed", error=str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

