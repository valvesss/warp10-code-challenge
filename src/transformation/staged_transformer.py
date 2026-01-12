"""
Staged data transformer.

Transforms raw extracted data into cleaned, normalized staged data
ready for graph model creation.
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import glob

import pandas as pd

from src.utils import get_logger
from .normalizers import OrganizationNormalizer, DrugNormalizer
from .extractors import RouteExtractor, DosageFormExtractor

logger = get_logger(__name__)


class StagedTransformer:
    """
    Transforms raw data to staged data.
    
    Applies:
    - Organization name normalization
    - Drug name normalization
    - Route of administration extraction
    - Dosage form extraction
    - Deduplication
    """
    
    def __init__(self, raw_path: Path, staged_path: Path):
        """
        Initialize transformer.
        
        Args:
            raw_path: Path to raw data directory
            staged_path: Path to staged data directory
        """
        self.raw_path = Path(raw_path)
        self.staged_path = Path(staged_path)
        
        # Initialize normalizers and extractors
        self.org_normalizer = OrganizationNormalizer()
        self.drug_normalizer = DrugNormalizer()
        self.route_extractor = RouteExtractor()
        self.dosage_extractor = DosageFormExtractor()
    
    def _get_latest_file(self, pattern: str) -> Optional[Path]:
        """Get the most recent file matching pattern."""
        files = sorted(glob.glob(str(self.raw_path / f"{pattern}_*.parquet")))
        return Path(files[-1]) if files else None
    
    def _load_raw(self, table_name: str) -> Optional[pd.DataFrame]:
        """Load raw data for a table."""
        filepath = self._get_latest_file(table_name)
        if filepath and filepath.exists():
            logger.debug(f"Loading {table_name} from {filepath}")
            return pd.read_parquet(filepath)
        logger.warning(f"No raw data found for {table_name}")
        return None
    
    def _save_staged(self, df: pd.DataFrame, name: str) -> Path:
        """Save staged data."""
        self.staged_path.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = self.staged_path / f"{name}_{timestamp}.parquet"
        df.to_parquet(filepath, index=False)
        logger.info(f"Saved staged {name}", rows=len(df), path=str(filepath))
        return filepath
    
    def transform_studies(self) -> pd.DataFrame:
        """
        Transform studies data.
        
        Returns:
            Staged studies DataFrame
        """
        df = self._load_raw("studies")
        if df is None or df.empty:
            return pd.DataFrame()
        
        # Select and rename columns for clarity
        staged = df[[
            'nct_id',
            'brief_title',
            'official_title',
            'phase',
            'overall_status',
            'study_type',
            'enrollment',
            'start_date',
            'completion_date',
            'primary_completion_date',
            'is_fda_regulated_drug',
            'number_of_arms',
        ]].copy()
        
        # Clean phase values
        staged['phase_clean'] = staged['phase'].str.replace('PHASE', 'Phase ').str.replace('/', '/Phase ')
        
        # Add status category
        staged['status_category'] = staged['overall_status'].apply(self._categorize_status)
        
        logger.info("Transformed studies", count=len(staged))
        return staged
    
    def _categorize_status(self, status: str) -> str:
        """Categorize study status."""
        if not status:
            return 'UNKNOWN'
        status = status.upper()
        if status in ['COMPLETED']:
            return 'COMPLETED'
        elif status in ['RECRUITING', 'ENROLLING_BY_INVITATION', 'ACTIVE_NOT_RECRUITING']:
            return 'ACTIVE'
        elif status in ['TERMINATED', 'WITHDRAWN', 'SUSPENDED']:
            return 'STOPPED'
        elif status in ['NOT_YET_RECRUITING', 'APPROVED_FOR_MARKETING']:
            return 'PLANNED'
        return 'OTHER'
    
    def transform_organizations(self) -> pd.DataFrame:
        """
        Transform and deduplicate organizations.
        
        Returns:
            Staged organizations DataFrame
        """
        sponsors = self._load_raw("sponsors")
        responsible = self._load_raw("responsible_parties")
        
        if sponsors is None or sponsors.empty:
            return pd.DataFrame()
        
        # Process sponsors
        orgs = []
        
        for _, row in sponsors.iterrows():
            name = row.get('name', '')
            if not name or pd.isna(name):
                continue
                
            normalized = self.org_normalizer.normalize(name)
            key = self.org_normalizer.normalize_for_key(name)
            
            orgs.append({
                'org_key': key,
                'org_name': normalized,
                'org_name_original': name,
                'agency_class': row.get('agency_class', 'UNKNOWN'),
            })
        
        # Process responsible parties (organizations)
        if responsible is not None and not responsible.empty:
            for _, row in responsible.iterrows():
                org_name = row.get('organization', '')
                if not org_name or pd.isna(org_name):
                    continue
                    
                normalized = self.org_normalizer.normalize(org_name)
                key = self.org_normalizer.normalize_for_key(org_name)
                
                orgs.append({
                    'org_key': key,
                    'org_name': normalized,
                    'org_name_original': org_name,
                    'agency_class': 'UNKNOWN',
                })
        
        # Create DataFrame and deduplicate
        df = pd.DataFrame(orgs)
        
        if df.empty:
            return df
        
        # Group by normalized key and take first occurrence
        staged = df.groupby('org_key').agg({
            'org_name': 'first',
            'org_name_original': 'first',
            'agency_class': 'first',
        }).reset_index()
        
        logger.info(
            "Transformed organizations",
            raw_count=len(orgs),
            deduplicated_count=len(staged),
        )
        
        return staged
    
    def transform_drugs(self) -> pd.DataFrame:
        """
        Transform and enrich drug/intervention data.
        
        Extracts route and dosage form from descriptions.
        
        Returns:
            Staged drugs DataFrame
        """
        interventions = self._load_raw("interventions")
        design_groups = self._load_raw("design_groups")
        
        if interventions is None or interventions.empty:
            return pd.DataFrame()
        
        # Filter to drugs and biologicals
        drugs = interventions[
            interventions['intervention_type'].isin(['DRUG', 'BIOLOGICAL'])
        ].copy()
        
        # Create design group lookup by nct_id
        design_group_desc = {}
        if design_groups is not None and not design_groups.empty:
            for _, row in design_groups.iterrows():
                nct_id = row.get('nct_id')
                desc = row.get('description', '')
                if nct_id and desc:
                    if nct_id not in design_group_desc:
                        design_group_desc[nct_id] = []
                    design_group_desc[nct_id].append(str(desc))
        
        # Transform each drug
        staged_drugs = []
        
        for _, row in drugs.iterrows():
            name = row.get('name', '')
            description = row.get('description', '')
            nct_id = row.get('nct_id', '')
            
            # Get design group descriptions for this study
            dg_descs = design_group_desc.get(nct_id, [])
            dg_desc_combined = ' '.join(dg_descs) if dg_descs else None
            
            # Normalize drug name
            normalized_name = self.drug_normalizer.normalize(name)
            drug_key = self.drug_normalizer.normalize_for_key(name)
            
            # Extract route
            route = self.route_extractor.extract_primary(
                name=name,
                description=description,
                design_group_desc=dg_desc_combined,
            )
            
            # Extract dosage form
            dosage_form = self.dosage_extractor.extract_primary(
                name=name,
                description=description,
            )
            
            staged_drugs.append({
                'intervention_id': row.get('id'),
                'nct_id': nct_id,
                'drug_key': drug_key,
                'drug_name': normalized_name,
                'drug_name_original': name,
                'intervention_type': row.get('intervention_type'),
                'description': description,
                'route': route,
                'dosage_form': dosage_form,
            })
        
        staged = pd.DataFrame(staged_drugs)
        
        # Calculate extraction stats
        route_coverage = (staged['route'].notna().sum() / len(staged) * 100) if len(staged) > 0 else 0
        form_coverage = (staged['dosage_form'].notna().sum() / len(staged) * 100) if len(staged) > 0 else 0
        
        logger.info(
            "Transformed drugs",
            count=len(staged),
            route_coverage=f"{route_coverage:.1f}%",
            dosage_form_coverage=f"{form_coverage:.1f}%",
        )
        
        return staged
    
    def transform_conditions(self) -> pd.DataFrame:
        """
        Transform conditions data.
        
        Returns:
            Staged conditions DataFrame
        """
        df = self._load_raw("conditions")
        if df is None or df.empty:
            return pd.DataFrame()
        
        staged = df[[
            'id',
            'nct_id',
            'name',
            'downcase_name',
        ]].copy()
        
        # Create condition key for deduplication
        staged['condition_key'] = staged['downcase_name'].fillna(
            staged['name'].str.lower()
        )
        
        logger.info("Transformed conditions", count=len(staged))
        return staged
    
    def transform_trial_organizations(self) -> pd.DataFrame:
        """
        Create trial-organization relationships.
        
        Returns:
            DataFrame with trial-org relationships
        """
        sponsors = self._load_raw("sponsors")
        if sponsors is None or sponsors.empty:
            return pd.DataFrame()
        
        relationships = []
        
        for _, row in sponsors.iterrows():
            nct_id = row.get('nct_id')
            name = row.get('name', '')
            role = row.get('lead_or_collaborator', 'unknown')
            
            if not name or pd.isna(name):
                continue
            
            org_key = self.org_normalizer.normalize_for_key(name)
            
            relationships.append({
                'nct_id': nct_id,
                'org_key': org_key,
                'relationship_type': 'SPONSORED_BY' if role == 'lead' else 'COLLABORATES_WITH',
            })
        
        staged = pd.DataFrame(relationships)
        logger.info("Created trial-org relationships", count=len(staged))
        return staged
    
    def transform_all(self) -> Dict[str, Any]:
        """
        Transform all data from raw to staged.
        
        Returns:
            Statistics about transformation
        """
        logger.info("Starting staged transformation...")
        
        stats = {
            'tables_transformed': 0,
            'files': [],
        }
        
        # Transform each entity type
        transformations = [
            ('studies', self.transform_studies),
            ('organizations', self.transform_organizations),
            ('drugs', self.transform_drugs),
            ('conditions', self.transform_conditions),
            ('trial_organizations', self.transform_trial_organizations),
        ]
        
        for name, transform_fn in transformations:
            try:
                df = transform_fn()
                if df is not None and not df.empty:
                    filepath = self._save_staged(df, name)
                    stats['files'].append(str(filepath))
                    stats['tables_transformed'] += 1
                    stats[f'{name}_count'] = len(df)
            except Exception as e:
                logger.error(f"Failed to transform {name}", error=str(e))
                raise
        
        logger.info("Staged transformation completed", **stats)
        return stats

