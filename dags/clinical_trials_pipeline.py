"""
Airflow DAG for Clinical Trials Knowledge Graph Pipeline.

Orchestrates the extraction, transformation, and loading of clinical
trial data from AACT into Neo4j.

Schedule: Daily at 2 AM UTC (adjust as needed)
"""

from datetime import datetime, timedelta
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# Add project to path (adjust for your deployment)
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


# Default DAG arguments
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


def _extract_data(**context):
    """Extract data from AACT database."""
    from config.settings import get_settings
    from src.ingestion.aact_extractor import AACTExtractor
    from src.utils import get_logger, setup_logging
    
    settings = get_settings()
    setup_logging(settings.log_level)
    logger = get_logger(__name__)
    
    logger.info("Starting AACT extraction task")
    
    # Ensure output directory exists
    settings.data.ensure_paths_exist()
    
    extractor = AACTExtractor(
        host=settings.aact.host,
        port=settings.aact.port,
        database=settings.aact.database,
        user=settings.aact.user,
        password=settings.aact.password,
        intervention_types=settings.extraction.intervention_types,
        phases=settings.extraction.phases,
        output_dir=settings.data.raw_path,
    )
    
    stats = extractor.extract_all(limit=settings.extraction.limit)
    
    logger.info("Extraction completed", **stats)
    
    # Push stats to XCom for downstream tasks
    return stats


def _transform_data(**context):
    """Transform raw data to staged format."""
    from config.settings import get_settings
    from src.transformation import StagedTransformer
    from src.utils import get_logger, setup_logging
    
    settings = get_settings()
    setup_logging(settings.log_level)
    logger = get_logger(__name__)
    
    logger.info("Starting transformation task")
    
    transformer = StagedTransformer(
        raw_path=settings.data.raw_path,
        staged_path=settings.data.staged_path,
    )
    
    stats = transformer.transform_all()
    
    logger.info("Transformation completed", **stats)
    return stats


def _load_to_neo4j(**context):
    """Load staged data into Neo4j."""
    from config.settings import get_settings
    from src.loading import Neo4jLoader
    from src.utils import get_logger, setup_logging
    
    settings = get_settings()
    setup_logging(settings.log_level)
    logger = get_logger(__name__)
    
    logger.info("Starting Neo4j loading task")
    
    loader = Neo4jLoader(
        uri=settings.neo4j.uri,
        user=settings.neo4j.user,
        password=settings.neo4j.password,
        staged_path=settings.data.staged_path,
    )
    
    try:
        stats = loader.load_all()
        logger.info("Neo4j loading completed", **stats)
        return stats
    finally:
        loader.close()


def _validate_graph(**context):
    """Validate loaded data in Neo4j."""
    from config.settings import get_settings
    from neo4j import GraphDatabase
    from src.utils import get_logger, setup_logging
    
    settings = get_settings()
    setup_logging(settings.log_level)
    logger = get_logger(__name__)
    
    logger.info("Starting graph validation task")
    
    driver = GraphDatabase.driver(
        settings.neo4j.uri,
        auth=(settings.neo4j.user, settings.neo4j.password),
    )
    
    validation_queries = {
        "trial_count": "MATCH (t:Trial) RETURN count(t) AS count",
        "org_count": "MATCH (o:Organization) RETURN count(o) AS count",
        "drug_count": "MATCH (d:Drug) RETURN count(d) AS count",
        "condition_count": "MATCH (c:Condition) RETURN count(c) AS count",
        "sponsored_by_count": "MATCH ()-[r:SPONSORED_BY]->() RETURN count(r) AS count",
        "investigates_count": "MATCH ()-[r:INVESTIGATES]->() RETURN count(r) AS count",
        "targets_count": "MATCH ()-[r:TARGETS]->() RETURN count(r) AS count",
    }
    
    results = {}
    try:
        with driver.session() as session:
            for name, query in validation_queries.items():
                result = session.run(query)
                record = result.single()
                results[name] = record["count"] if record else 0
        
        logger.info("Validation completed", **results)
        
        # Basic validation checks
        if results["trial_count"] == 0:
            raise ValueError("No trials loaded - validation failed")
        
        return results
    finally:
        driver.close()


# Create DAG
with DAG(
    dag_id="clinical_trials_knowledge_graph",
    default_args=default_args,
    description="ETL pipeline for Clinical Trials Knowledge Graph",
    schedule_interval="0 2 * * *",  # Daily at 2 AM UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["clinical-trials", "knowledge-graph", "neo4j"],
) as dag:
    
    # Start marker
    start = EmptyOperator(task_id="start")
    
    # Extract data from AACT
    with TaskGroup(group_id="extraction") as extraction_group:
        extract_task = PythonOperator(
            task_id="extract_from_aact",
            python_callable=_extract_data,
            provide_context=True,
        )
    
    # Transform data
    with TaskGroup(group_id="transformation") as transformation_group:
        transform_task = PythonOperator(
            task_id="transform_to_staged",
            python_callable=_transform_data,
            provide_context=True,
        )
    
    # Load to Neo4j
    with TaskGroup(group_id="loading") as loading_group:
        load_task = PythonOperator(
            task_id="load_to_neo4j",
            python_callable=_load_to_neo4j,
            provide_context=True,
        )
    
    # Validate
    with TaskGroup(group_id="validation") as validation_group:
        validate_task = PythonOperator(
            task_id="validate_graph",
            python_callable=_validate_graph,
            provide_context=True,
        )
    
    # End marker
    end = EmptyOperator(task_id="end")
    
    # Define task dependencies
    start >> extraction_group >> transformation_group >> loading_group >> validation_group >> end


# For testing outside Airflow
if __name__ == "__main__":
    print("Testing DAG tasks...")
    print("1. Extract...")
    _extract_data()
    print("2. Transform...")
    _transform_data()
    print("3. Load...")
    _load_to_neo4j()
    print("4. Validate...")
    _validate_graph()
    print("Done!")

