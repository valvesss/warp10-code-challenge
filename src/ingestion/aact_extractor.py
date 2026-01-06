"""
AACT Database Extractor.

Extracts clinical trial data from the AACT PostgreSQL database
and saves it as Parquet files for further processing.
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.utils import get_logger

logger = get_logger(__name__)


class AACTExtractor:
    """
    Extracts clinical trial data from AACT database.

    The AACT database mirrors ClinicalTrials.gov and provides
    structured access to clinical trial information.
    """

    # Tables to extract with their key columns (based on actual AACT schema)
    TABLES_CONFIG = {
        "studies": {
            "description": "Core study information",
            "columns": [
                "nct_id",
                "official_title",
                "brief_title",
                "overall_status",
                "last_known_status",
                "phase",
                "study_type",
                "enrollment",
                "enrollment_type",
                "start_date",
                "start_date_type",
                "completion_date",
                "completion_date_type",
                "primary_completion_date",
                "study_first_submitted_date",
                "last_update_posted_date",
                "source",
                "source_class",
                "is_fda_regulated_drug",
                "is_fda_regulated_device",
                "number_of_arms",
                "acronym",
                "why_stopped",
                "has_dmc",
            ],
        },
        "sponsors": {
            "description": "Sponsor and collaborator organizations",
            "columns": [
                "id",
                "nct_id",
                "agency_class",
                "lead_or_collaborator",
                "name",
            ],
        },
        "interventions": {
            "description": "Study interventions (drugs, biologics, etc.)",
            "columns": [
                "id",
                "nct_id",
                "intervention_type",
                "name",
                "description",
            ],
        },
        "intervention_other_names": {
            "description": "Alternative names for interventions",
            "columns": [
                "id",
                "nct_id",
                "intervention_id",
                "name",
            ],
        },
        "design_outcomes": {
            "description": "Primary and secondary outcomes",
            "columns": [
                "id",
                "nct_id",
                "outcome_type",
                "measure",
                "time_frame",
                "population",
                "description",
            ],
        },
        "eligibilities": {
            "description": "Eligibility criteria",
            "columns": [
                "id",
                "nct_id",
                "gender",
                "minimum_age",
                "maximum_age",
                "healthy_volunteers",
                "adult",
                "child",
                "older_adult",
                "criteria",
            ],
        },
        "conditions": {
            "description": "Conditions/diseases being studied",
            "columns": [
                "id",
                "nct_id",
                "name",
                "downcase_name",
            ],
        },
        "browse_interventions": {
            "description": "MeSH terms for interventions",
            "columns": [
                "id",
                "nct_id",
                "mesh_term",
                "downcase_mesh_term",
                "mesh_type",
            ],
        },
        "facilities": {
            "description": "Study locations/sites",
            "columns": [
                "id",
                "nct_id",
                "name",
                "city",
                "state",
                "zip",
                "country",
                "status",
            ],
        },
        "responsible_parties": {
            "description": "Responsible party information",
            "columns": [
                "id",
                "nct_id",
                "responsible_party_type",
                "name",
                "title",
                "organization",
                "affiliation",
            ],
        },
    }

    def __init__(self, settings: Any) -> None:
        """
        Initialize the AACT extractor.

        Args:
            settings: Application settings containing database credentials
        """
        self.settings = settings
        self._engine: Optional[Engine] = None

    @property
    def engine(self) -> Engine:
        """Get or create SQLAlchemy engine."""
        if self._engine is None:
            connection_string = self.settings.aact.connection_string
            logger.debug("Creating database connection", host=self.settings.aact.host)
            self._engine = create_engine(
                connection_string,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=10,
            )
        return self._engine

    def test_connection(self) -> bool:
        """
        Test database connectivity.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                result.fetchone()
            logger.info("Database connection successful")
            return True
        except Exception as e:
            logger.error("Database connection failed", error=str(e))
            return False

    def get_study_nct_ids(self) -> List[str]:
        """
        Get NCT IDs for studies matching extraction criteria.

        Filters by:
        - Intervention type: Drug or Biological
        - Phase: Configurable phases
        - Limit: Configurable limit

        Returns:
            List of NCT IDs matching criteria
        """
        phases = self.settings.extraction.phases
        intervention_types = self.settings.extraction.intervention_types
        limit = self.settings.extraction.limit

        # Build phase filter
        phase_placeholders = ", ".join([f":phase_{i}" for i in range(len(phases))])

        query = f"""
        SELECT DISTINCT s.nct_id
        FROM ctgov.studies s
        INNER JOIN ctgov.interventions i ON s.nct_id = i.nct_id
        WHERE s.phase IN ({phase_placeholders})
        AND i.intervention_type IN (:int_type_1, :int_type_2)
        ORDER BY s.nct_id
        """

        if limit:
            query += f" LIMIT {limit}"

        # Build parameters
        params = {f"phase_{i}": phase for i, phase in enumerate(phases)}
        params["int_type_1"] = intervention_types[0] if len(intervention_types) > 0 else "Drug"
        params["int_type_2"] = intervention_types[1] if len(intervention_types) > 1 else "Biological"

        logger.info(
            "Fetching study NCT IDs",
            phases=phases,
            intervention_types=intervention_types,
            limit=limit,
        )

        with self.engine.connect() as conn:
            result = conn.execute(text(query), params)
            nct_ids = [row[0] for row in result.fetchall()]

        logger.info("Found studies matching criteria", count=len(nct_ids))
        return nct_ids

    def extract_table(
        self,
        table_name: str,
        nct_ids: List[str],
    ) -> pd.DataFrame:
        """
        Extract a single table filtered by NCT IDs.

        Args:
            table_name: Name of the table to extract
            nct_ids: List of NCT IDs to filter by

        Returns:
            DataFrame with extracted data
        """
        if table_name not in self.TABLES_CONFIG:
            raise ValueError(f"Unknown table: {table_name}")

        config = self.TABLES_CONFIG[table_name]
        columns = config["columns"]
        columns_str = ", ".join(columns)

        # Use ANY for efficient filtering with large lists
        query = f"""
        SELECT {columns_str}
        FROM ctgov.{table_name}
        WHERE nct_id = ANY(:nct_ids)
        """

        logger.debug(f"Extracting table: {table_name}", columns=columns)

        with self.engine.connect() as conn:
            df = pd.read_sql(
                text(query),
                conn,
                params={"nct_ids": nct_ids},
            )

        logger.info(
            f"Extracted {table_name}",
            rows=len(df),
            columns=list(df.columns),
        )

        return df

    def save_parquet(
        self,
        df: pd.DataFrame,
        table_name: str,
        output_dir: Path,
    ) -> Path:
        """
        Save DataFrame as Parquet file.

        Args:
            df: DataFrame to save
            table_name: Name for the output file
            output_dir: Directory to save to

        Returns:
            Path to saved file
        """
        output_dir.mkdir(parents=True, exist_ok=True)

        # Add extraction timestamp to filename for versioning
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{table_name}_{timestamp}.parquet"
        filepath = output_dir / filename

        df.to_parquet(filepath, index=False, compression="snappy")

        logger.debug(f"Saved {table_name} to {filepath}", rows=len(df))
        return filepath

    def extract_all(
        self,
        tables: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Extract all configured tables.

        Args:
            tables: Optional list of specific tables to extract.
                   If None, extracts all configured tables.

        Returns:
            Dictionary with extraction statistics
        """
        # Test connection first
        if not self.test_connection():
            raise ConnectionError("Cannot connect to AACT database")

        # Get NCT IDs for filtering
        nct_ids = self.get_study_nct_ids()

        if not nct_ids:
            logger.warning("No studies found matching criteria")
            return {"studies_found": 0, "tables_extracted": 0}

        # Determine which tables to extract
        tables_to_extract = tables or list(self.TABLES_CONFIG.keys())

        # Validate requested tables
        invalid_tables = set(tables_to_extract) - set(self.TABLES_CONFIG.keys())
        if invalid_tables:
            raise ValueError(f"Unknown tables: {invalid_tables}")

        output_dir = self.settings.data.raw_path
        stats = {
            "studies_found": len(nct_ids),
            "tables_extracted": 0,
            "total_rows": 0,
            "files": [],
        }

        for table_name in tables_to_extract:
            try:
                logger.info(f"Extracting {table_name}...")
                df = self.extract_table(table_name, nct_ids)

                if not df.empty:
                    filepath = self.save_parquet(df, table_name, output_dir)
                    stats["files"].append(str(filepath))
                    stats["total_rows"] += len(df)
                    stats["tables_extracted"] += 1
                else:
                    logger.warning(f"No data found for {table_name}")

            except Exception as e:
                logger.error(f"Failed to extract {table_name}", error=str(e))
                raise

        # Save extraction metadata
        metadata = {
            "extraction_time": datetime.now().isoformat(),
            "nct_ids_count": len(nct_ids),
            "nct_ids": nct_ids[:100],  # Save first 100 for reference
            "settings": {
                "phases": self.settings.extraction.phases,
                "intervention_types": self.settings.extraction.intervention_types,
                "limit": self.settings.extraction.limit,
            },
            "stats": stats,
        }

        metadata_df = pd.DataFrame([metadata])
        self.save_parquet(metadata_df, "_extraction_metadata", output_dir)

        return stats

    def get_table_schema(self, table_name: str) -> pd.DataFrame:
        """
        Get schema information for a table.

        Args:
            table_name: Name of the table

        Returns:
            DataFrame with column information
        """
        query = """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'ctgov'
        AND table_name = :table_name
        ORDER BY ordinal_position
        """

        with self.engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params={"table_name": table_name})

        return df

    def explore_available_tables(self) -> pd.DataFrame:
        """
        List all available tables in the AACT database.

        Returns:
            DataFrame with table names and row counts
        """
        query = """
        SELECT 
            table_name,
            (SELECT COUNT(*) FROM information_schema.columns 
             WHERE table_name = t.table_name AND table_schema = 'ctgov') as column_count
        FROM information_schema.tables t
        WHERE table_schema = 'ctgov'
        ORDER BY table_name
        """

        with self.engine.connect() as conn:
            df = pd.read_sql(text(query), conn)

        logger.info("Found tables in AACT", count=len(df))
        return df

    def close(self) -> None:
        """Close database connections."""
        if self._engine:
            self._engine.dispose()
            self._engine = None
            logger.debug("Database connections closed")

