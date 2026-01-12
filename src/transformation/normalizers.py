"""
Normalizers for organization and drug names.

Handles common variations, suffixes, and inconsistencies in entity names.
"""

import re
from typing import Optional

from src.utils import get_logger

logger = get_logger(__name__)


class OrganizationNormalizer:
    """
    Normalizes organization names for consistent matching.
    
    Handles:
    - Case normalization
    - Common suffix removal (Inc., Ltd., Corp., etc.)
    - Whitespace normalization
    - Common abbreviation expansion
    """
    
    # Suffixes to remove for normalized comparison
    SUFFIXES_TO_REMOVE = [
        r',?\s*Inc\.?$',
        r',?\s*Incorporated$',
        r',?\s*Ltd\.?$',
        r',?\s*Limited$',
        r',?\s*Corp\.?$',
        r',?\s*Corporation$',
        r',?\s*LLC$',
        r',?\s*L\.L\.C\.?$',
        r',?\s*Co\.?$',
        r',?\s*Company$',
        r',?\s*PLC$',
        r',?\s*P\.L\.C\.?$',
        r',?\s*GmbH$',
        r',?\s*AG$',
        r',?\s*S\.A\.?$',
        r',?\s*S\.p\.A\.?$',
        r',?\s*N\.V\.?$',
        r',?\s*B\.V\.?$',
    ]
    
    # Common abbreviations to expand
    ABBREVIATIONS = {
        'NIH': 'National Institutes of Health',
        'NCI': 'National Cancer Institute',
        'NIAID': 'National Institute of Allergy and Infectious Diseases',
        'NHLBI': 'National Heart, Lung, and Blood Institute',
        'NIDA': 'National Institute on Drug Abuse',
        'NEI': 'National Eye Institute',
        'NINDS': 'National Institute of Neurological Disorders and Stroke',
        'NIMH': 'National Institute of Mental Health',
        'NICHD': 'Eunice Kennedy Shriver National Institute of Child Health and Human Development',
        'NIDDK': 'National Institute of Diabetes and Digestive and Kidney Diseases',
        'FDA': 'Food and Drug Administration',
        'CDC': 'Centers for Disease Control and Prevention',
        'WHO': 'World Health Organization',
    }
    
    def __init__(self):
        """Initialize the normalizer."""
        # Compile suffix patterns for efficiency
        self._suffix_pattern = re.compile(
            '|'.join(self.SUFFIXES_TO_REMOVE),
            re.IGNORECASE
        )
    
    def normalize(self, name: Optional[str]) -> str:
        """
        Normalize an organization name.
        
        Args:
            name: Raw organization name
            
        Returns:
            Normalized name for matching/deduplication
        """
        if not name or not isinstance(name, str):
            return ""
        
        # Strip whitespace
        normalized = name.strip()
        
        # Remove common suffixes
        normalized = self._suffix_pattern.sub('', normalized)
        
        # Normalize whitespace (multiple spaces to single)
        normalized = re.sub(r'\s+', ' ', normalized)
        
        # Strip again after suffix removal
        normalized = normalized.strip()
        
        # Remove trailing punctuation
        normalized = re.sub(r'[,\.\-]+$', '', normalized).strip()
        
        return normalized
    
    def normalize_for_key(self, name: Optional[str]) -> str:
        """
        Create a normalized key for deduplication.
        
        More aggressive normalization for matching purposes.
        
        Args:
            name: Raw organization name
            
        Returns:
            Lowercase normalized key
        """
        normalized = self.normalize(name)
        
        # Lowercase for key
        key = normalized.lower()
        
        # Remove all non-alphanumeric except spaces
        key = re.sub(r'[^a-z0-9\s]', '', key)
        
        # Normalize whitespace again
        key = re.sub(r'\s+', ' ', key).strip()
        
        return key
    
    def get_display_name(self, name: Optional[str]) -> str:
        """
        Get a clean display name (preserves case but cleans up).
        
        Args:
            name: Raw organization name
            
        Returns:
            Clean display name
        """
        return self.normalize(name)


class DrugNormalizer:
    """
    Normalizes drug/intervention names.
    
    Handles:
    - Case normalization
    - Dosage/strength removal from name
    - Common formatting variations
    """
    
    # Patterns for dosage information to remove
    DOSAGE_PATTERNS = [
        r'\s*\d+\s*(?:mg|g|mcg|µg|ml|l|iu|u|%)\b',  # 100mg, 5ml, etc.
        r'\s*\d+\s*/\s*\d+\s*(?:mg|g|mcg|µg|ml)\b',  # 100/5mg
        r'\s*\(\d+\s*(?:mg|g|mcg|µg|ml|%)[^)]*\)',   # (100mg)
        r'\s*\d+(?:\.\d+)?\s*%',  # 0.5%
    ]
    
    def __init__(self):
        """Initialize the normalizer."""
        self._dosage_pattern = re.compile(
            '|'.join(self.DOSAGE_PATTERNS),
            re.IGNORECASE
        )
    
    def normalize(self, name: Optional[str]) -> str:
        """
        Normalize a drug name.
        
        Args:
            name: Raw drug name
            
        Returns:
            Normalized name
        """
        if not name or not isinstance(name, str):
            return ""
        
        # Strip whitespace
        normalized = name.strip()
        
        # Normalize whitespace
        normalized = re.sub(r'\s+', ' ', normalized)
        
        return normalized
    
    def normalize_for_key(self, name: Optional[str]) -> str:
        """
        Create a normalized key for deduplication.
        
        Args:
            name: Raw drug name
            
        Returns:
            Lowercase normalized key without dosage info
        """
        normalized = self.normalize(name)
        
        # Remove dosage information
        key = self._dosage_pattern.sub('', normalized)
        
        # Lowercase
        key = key.lower()
        
        # Remove non-alphanumeric except spaces and hyphens
        key = re.sub(r'[^a-z0-9\s\-]', '', key)
        
        # Normalize whitespace
        key = re.sub(r'\s+', ' ', key).strip()
        
        return key
    
    def get_display_name(self, name: Optional[str]) -> str:
        """
        Get a clean display name.
        
        Args:
            name: Raw drug name
            
        Returns:
            Clean display name
        """
        return self.normalize(name)

