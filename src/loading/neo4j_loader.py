"""
Neo4j graph database loader.

Loads staged data into Neo4j using MERGE for idempotency.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional
import glob

import pandas as pd
from neo4j import GraphDatabase, Driver

from src.utils import get_logger

logger = get_logger(__name__)


class Neo4jLoader:
    """
    Loads clinical trial data into Neo4j.
    
    Uses MERGE operations for idempotent loading.
    Supports batch processing for large datasets.
    """
    
    BATCH_SIZE = 500
    
    def __init__(
        self,
        uri: str,
        user: str,
        password: str,
        staged_path: Path,
    ):
        """
        Initialize loader.
        
        Args:
            uri: Neo4j connection URI
            user: Database username
            password: Database password
            staged_path: Path to staged data
        """
        self.uri = uri
        self.user = user
        self.password = password
        self.staged_path = Path(staged_path)
        self._driver: Optional[Driver] = None
    
    @property
    def driver(self) -> Driver:
        """Get or create Neo4j driver."""
        if self._driver is None:
            self._driver = GraphDatabase.driver(
                self.uri,
                auth=(self.user, self.password),
            )
        return self._driver
    
    def test_connection(self) -> bool:
        """Test database connectivity."""
        try:
            with self.driver.session() as session:
                result = session.run("RETURN 1 AS test")
                result.single()
            logger.info("Neo4j connection successful")
            return True
        except Exception as e:
            logger.error("Neo4j connection failed", error=str(e))
            return False
    
    def _get_latest_file(self, pattern: str) -> Optional[Path]:
        """Get the most recent file matching pattern."""
        files = sorted(glob.glob(str(self.staged_path / f"{pattern}_*.parquet")))
        return Path(files[-1]) if files else None
    
    def _load_staged(self, table_name: str) -> Optional[pd.DataFrame]:
        """Load staged data for a table."""
        filepath = self._get_latest_file(table_name)
        if filepath and filepath.exists():
            return pd.read_parquet(filepath)
        logger.warning(f"No staged data found for {table_name}")
        return None
    
    def create_constraints_and_indexes(self) -> None:
        """
        Create uniqueness constraints and indexes.
        
        Must be run before loading data.
        """
        constraints = [
            # Uniqueness constraints
            "CREATE CONSTRAINT trial_nct_id IF NOT EXISTS FOR (t:Trial) REQUIRE t.nct_id IS UNIQUE",
            "CREATE CONSTRAINT org_key IF NOT EXISTS FOR (o:Organization) REQUIRE o.org_key IS UNIQUE",
            "CREATE CONSTRAINT drug_key IF NOT EXISTS FOR (d:Drug) REQUIRE d.drug_key IS UNIQUE",
            "CREATE CONSTRAINT condition_key IF NOT EXISTS FOR (c:Condition) REQUIRE c.condition_key IS UNIQUE",
        ]
        
        indexes = [
            # Performance indexes
            "CREATE INDEX trial_phase IF NOT EXISTS FOR (t:Trial) ON (t.phase)",
            "CREATE INDEX trial_status IF NOT EXISTS FOR (t:Trial) ON (t.overall_status)",
            "CREATE INDEX org_name IF NOT EXISTS FOR (o:Organization) ON (o.name)",
            "CREATE INDEX drug_name IF NOT EXISTS FOR (d:Drug) ON (d.name)",
            "CREATE INDEX drug_route IF NOT EXISTS FOR (d:Drug) ON (d.route)",
        ]
        
        with self.driver.session() as session:
            for constraint in constraints:
                try:
                    session.run(constraint)
                    logger.debug(f"Created constraint: {constraint[:50]}...")
                except Exception as e:
                    logger.debug(f"Constraint may already exist: {e}")
            
            for index in indexes:
                try:
                    session.run(index)
                    logger.debug(f"Created index: {index[:50]}...")
                except Exception as e:
                    logger.debug(f"Index may already exist: {e}")
        
        logger.info("Created constraints and indexes")
    
    def load_trials(self) -> int:
        """
        Load Trial nodes.
        
        Returns:
            Number of nodes created/updated
        """
        df = self._load_staged("studies")
        if df is None or df.empty:
            return 0
        
        query = """
        UNWIND $batch AS row
        MERGE (t:Trial {nct_id: row.nct_id})
        SET t.brief_title = row.brief_title,
            t.official_title = row.official_title,
            t.phase = row.phase,
            t.phase_clean = row.phase_clean,
            t.overall_status = row.overall_status,
            t.status_category = row.status_category,
            t.study_type = row.study_type,
            t.enrollment = row.enrollment,
            t.start_date = row.start_date,
            t.completion_date = row.completion_date,
            t.is_fda_regulated_drug = row.is_fda_regulated_drug,
            t.number_of_arms = row.number_of_arms,
            t.updated_at = datetime()
        """
        
        count = self._batch_execute(query, df)
        logger.info("Loaded Trial nodes", count=count)
        return count
    
    def load_organizations(self) -> int:
        """
        Load Organization nodes.
        
        Returns:
            Number of nodes created/updated
        """
        df = self._load_staged("organizations")
        if df is None or df.empty:
            return 0
        
        query = """
        UNWIND $batch AS row
        MERGE (o:Organization {org_key: row.org_key})
        SET o.name = row.org_name,
            o.name_original = row.org_name_original,
            o.agency_class = row.agency_class,
            o.updated_at = datetime()
        """
        
        count = self._batch_execute(query, df)
        logger.info("Loaded Organization nodes", count=count)
        return count
    
    def load_drugs(self) -> int:
        """
        Load Drug nodes.
        
        Returns:
            Number of nodes created/updated
        """
        df = self._load_staged("drugs")
        if df is None or df.empty:
            return 0
        
        # Get unique drugs by drug_key
        unique_drugs = df.groupby('drug_key').agg({
            'drug_name': 'first',
            'drug_name_original': 'first',
            'intervention_type': 'first',
        }).reset_index()
        
        query = """
        UNWIND $batch AS row
        MERGE (d:Drug {drug_key: row.drug_key})
        SET d.name = row.drug_name,
            d.name_original = row.drug_name_original,
            d.intervention_type = row.intervention_type,
            d.updated_at = datetime()
        """
        
        count = self._batch_execute(query, unique_drugs)
        logger.info("Loaded Drug nodes", count=count)
        return count
    
    def load_conditions(self) -> int:
        """
        Load Condition nodes.
        
        Returns:
            Number of nodes created/updated
        """
        df = self._load_staged("conditions")
        if df is None or df.empty:
            return 0
        
        # Get unique conditions
        unique_conditions = df.groupby('condition_key').agg({
            'name': 'first',
        }).reset_index()
        
        query = """
        UNWIND $batch AS row
        MERGE (c:Condition {condition_key: row.condition_key})
        SET c.name = row.name,
            c.updated_at = datetime()
        """
        
        count = self._batch_execute(query, unique_conditions)
        logger.info("Loaded Condition nodes", count=count)
        return count
    
    def load_trial_organization_relationships(self) -> int:
        """
        Load Trial-Organization relationships.
        
        Returns:
            Number of relationships created
        """
        df = self._load_staged("trial_organizations")
        if df is None or df.empty:
            return 0
        
        # Split by relationship type
        sponsors = df[df['relationship_type'] == 'SPONSORED_BY']
        collaborators = df[df['relationship_type'] == 'COLLABORATES_WITH']
        
        count = 0
        
        # Load SPONSORED_BY relationships
        if not sponsors.empty:
            query = """
            UNWIND $batch AS row
            MATCH (t:Trial {nct_id: row.nct_id})
            MATCH (o:Organization {org_key: row.org_key})
            MERGE (t)-[r:SPONSORED_BY]->(o)
            SET r.updated_at = datetime()
            """
            count += self._batch_execute(query, sponsors)
        
        # Load COLLABORATES_WITH relationships
        if not collaborators.empty:
            query = """
            UNWIND $batch AS row
            MATCH (t:Trial {nct_id: row.nct_id})
            MATCH (o:Organization {org_key: row.org_key})
            MERGE (t)-[r:COLLABORATES_WITH]->(o)
            SET r.updated_at = datetime()
            """
            count += self._batch_execute(query, collaborators)
        
        logger.info("Loaded Trial-Organization relationships", count=count)
        return count
    
    def load_trial_drug_relationships(self) -> int:
        """
        Load Trial-Drug relationships with route/dosage properties.
        
        Returns:
            Number of relationships created
        """
        df = self._load_staged("drugs")
        if df is None or df.empty:
            return 0
        
        query = """
        UNWIND $batch AS row
        MATCH (t:Trial {nct_id: row.nct_id})
        MATCH (d:Drug {drug_key: row.drug_key})
        MERGE (t)-[r:INVESTIGATES]->(d)
        SET r.route = row.route,
            r.dosage_form = row.dosage_form,
            r.intervention_id = row.intervention_id,
            r.updated_at = datetime()
        """
        
        count = self._batch_execute(query, df)
        logger.info("Loaded Trial-Drug relationships", count=count)
        return count
    
    def load_trial_condition_relationships(self) -> int:
        """
        Load Trial-Condition relationships.
        
        Returns:
            Number of relationships created
        """
        df = self._load_staged("conditions")
        if df is None or df.empty:
            return 0
        
        query = """
        UNWIND $batch AS row
        MATCH (t:Trial {nct_id: row.nct_id})
        MATCH (c:Condition {condition_key: row.condition_key})
        MERGE (t)-[r:TARGETS]->(c)
        SET r.updated_at = datetime()
        """
        
        count = self._batch_execute(query, df)
        logger.info("Loaded Trial-Condition relationships", count=count)
        return count
    
    def _batch_execute(self, query: str, df: pd.DataFrame) -> int:
        """
        Execute query in batches.
        
        Args:
            query: Cypher query with $batch parameter
            df: DataFrame to process
            
        Returns:
            Number of records processed
        """
        total = 0
        records = df.to_dict('records')
        
        # Replace NaN/None with None for Neo4j
        for record in records:
            for key, value in record.items():
                if pd.isna(value):
                    record[key] = None
        
        with self.driver.session() as session:
            for i in range(0, len(records), self.BATCH_SIZE):
                batch = records[i:i + self.BATCH_SIZE]
                session.run(query, batch=batch)
                total += len(batch)
                
                if total % 1000 == 0:
                    logger.debug(f"Processed {total} records...")
        
        return total
    
    def load_all(self) -> Dict[str, Any]:
        """
        Load all staged data into Neo4j.
        
        Returns:
            Statistics about loading
        """
        if not self.test_connection():
            raise ConnectionError("Cannot connect to Neo4j")
        
        logger.info("Starting Neo4j loading...")
        
        stats = {}
        
        # Create constraints first
        self.create_constraints_and_indexes()
        
        # Load nodes
        stats['trials'] = self.load_trials()
        stats['organizations'] = self.load_organizations()
        stats['drugs'] = self.load_drugs()
        stats['conditions'] = self.load_conditions()
        
        # Load relationships
        stats['trial_org_rels'] = self.load_trial_organization_relationships()
        stats['trial_drug_rels'] = self.load_trial_drug_relationships()
        stats['trial_condition_rels'] = self.load_trial_condition_relationships()
        
        logger.info("Neo4j loading completed", **stats)
        return stats
    
    def close(self) -> None:
        """Close database connection."""
        if self._driver:
            self._driver.close()
            self._driver = None

