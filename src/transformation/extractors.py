"""
Extractors for route of administration and dosage form.

Uses regex patterns to extract information from intervention descriptions.
"""

import re
from dataclasses import dataclass
from typing import List, Optional, Set

from src.utils import get_logger

logger = get_logger(__name__)


@dataclass
class ExtractionResult:
    """Result of extraction with confidence."""
    value: str
    source: str  # Where it was found: 'name', 'description', 'design_group'
    confidence: float  # 0.0 to 1.0


class RouteExtractor:
    """
    Extracts route of administration from text.
    
    Routes are standardized to common terms:
    - ORAL
    - INTRAVENOUS
    - INTRAMUSCULAR
    - SUBCUTANEOUS
    - TOPICAL
    - INHALATION
    - OPHTHALMIC
    - NASAL
    - RECTAL
    - TRANSDERMAL
    - INTRATHECAL
    - OTHER
    """
    
    # Route patterns with standardized names
    ROUTE_PATTERNS = {
        'ORAL': [
            r'\b(oral|orally|per\s*os|p\.?\s*o\.?|by\s*mouth)\b',
            r'\b(tablet|capsule|pill|syrup|liquid|suspension|solution)\s*(form|formulation)?\b',
        ],
        'INTRAVENOUS': [
            r'\b(intravenous|intravenously|i\.?\s*v\.?|iv\b)\b',
            r'\b(infusion|infused)\b',
        ],
        'INTRAMUSCULAR': [
            r'\b(intramuscular|intramuscularly|i\.?\s*m\.?|im\b)\b',
        ],
        'SUBCUTANEOUS': [
            r'\b(subcutaneous|subcutaneously|s\.?\s*c\.?|sc\b|subq|sub-q)\b',
        ],
        'TOPICAL': [
            r'\b(topical|topically)\b',
            r'\b(cream|ointment|gel|lotion)\s*(applied|application)?\b',
        ],
        'INHALATION': [
            r'\b(inhal|nebuliz|aerosol)\w*\b',
            r'\b(inhaler|nebulizer|puff|metered.dose)\b',
        ],
        'OPHTHALMIC': [
            r'\b(ophthalmic|ocular|eye\s*drop|intravitreal|intraocular)\b',
        ],
        'NASAL': [
            r'\b(nasal|intranasal|nose\s*spray)\b',
        ],
        'RECTAL': [
            r'\b(rectal|rectally|suppository|enema|per\s*rectum|p\.?\s*r\.?)\b',
        ],
        'TRANSDERMAL': [
            r'\b(transdermal|patch|patches)\b',
        ],
        'INTRATHECAL': [
            r'\b(intrathecal|spinal)\b',
        ],
    }
    
    def __init__(self):
        """Initialize extractor with compiled patterns."""
        self._compiled_patterns = {}
        for route, patterns in self.ROUTE_PATTERNS.items():
            combined = '|'.join(f'({p})' for p in patterns)
            self._compiled_patterns[route] = re.compile(combined, re.IGNORECASE)
    
    def extract(
        self,
        name: Optional[str] = None,
        description: Optional[str] = None,
        design_group_desc: Optional[str] = None,
    ) -> List[ExtractionResult]:
        """
        Extract routes from available text fields.
        
        Args:
            name: Intervention name
            description: Intervention description
            design_group_desc: Design group description
            
        Returns:
            List of extracted routes with source and confidence
        """
        results = []
        found_routes: Set[str] = set()
        
        # Check each text source (in order of reliability)
        sources = [
            ('description', description, 0.9),
            ('design_group', design_group_desc, 0.8),
            ('name', name, 0.7),
        ]
        
        for source_name, text, base_confidence in sources:
            if not text:
                continue
                
            text = str(text)
            
            for route, pattern in self._compiled_patterns.items():
                if route in found_routes:
                    continue
                    
                if pattern.search(text):
                    results.append(ExtractionResult(
                        value=route,
                        source=source_name,
                        confidence=base_confidence,
                    ))
                    found_routes.add(route)
        
        return results
    
    def extract_primary(
        self,
        name: Optional[str] = None,
        description: Optional[str] = None,
        design_group_desc: Optional[str] = None,
    ) -> Optional[str]:
        """
        Extract the primary (most confident) route.
        
        Args:
            name: Intervention name
            description: Intervention description
            design_group_desc: Design group description
            
        Returns:
            Primary route or None if not found
        """
        results = self.extract(name, description, design_group_desc)
        if results:
            # Return highest confidence result
            best = max(results, key=lambda r: r.confidence)
            return best.value
        return None


class DosageFormExtractor:
    """
    Extracts dosage form from text.
    
    Forms are standardized to common terms:
    - TABLET
    - CAPSULE
    - INJECTION
    - SOLUTION
    - SUSPENSION
    - CREAM
    - OINTMENT
    - GEL
    - DROPS
    - SPRAY
    - PATCH
    - IMPLANT
    - POWDER
    - SUPPOSITORY
    - OTHER
    """
    
    FORM_PATTERNS = {
        'TABLET': [r'\b(tablet|tablets|tab|tabs)\b'],
        'CAPSULE': [r'\b(capsule|capsules|cap|caps)\b'],
        'INJECTION': [r'\b(injection|injectable|inject)\b'],
        'SOLUTION': [r'\b(solution|liquid)\b'],
        'SUSPENSION': [r'\b(suspension)\b'],
        'CREAM': [r'\b(cream|creams)\b'],
        'OINTMENT': [r'\b(ointment|ointments)\b'],
        'GEL': [r'\b(gel|gels)\b'],
        'DROPS': [r'\b(drop|drops|eye\s*drop)\b'],
        'SPRAY': [r'\b(spray|sprays|nasal\s*spray)\b'],
        'PATCH': [r'\b(patch|patches|transdermal\s*patch)\b'],
        'IMPLANT': [r'\b(implant|implants)\b'],
        'POWDER': [r'\b(powder|powders)\b'],
        'SUPPOSITORY': [r'\b(suppository|suppositories)\b'],
        'INHALER': [r'\b(inhaler|inhalers|puffer)\b'],
        'NEBULIZER': [r'\b(nebulizer|nebuliser|nebulized)\b'],
        'INFUSION': [r'\b(infusion|infusions|iv\s*bag)\b'],
    }
    
    def __init__(self):
        """Initialize extractor with compiled patterns."""
        self._compiled_patterns = {}
        for form, patterns in self.FORM_PATTERNS.items():
            combined = '|'.join(f'({p})' for p in patterns)
            self._compiled_patterns[form] = re.compile(combined, re.IGNORECASE)
    
    def extract(
        self,
        name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> List[ExtractionResult]:
        """
        Extract dosage forms from available text fields.
        
        Args:
            name: Intervention name
            description: Intervention description
            
        Returns:
            List of extracted forms with source and confidence
        """
        results = []
        found_forms: Set[str] = set()
        
        sources = [
            ('description', description, 0.9),
            ('name', name, 0.7),
        ]
        
        for source_name, text, base_confidence in sources:
            if not text:
                continue
                
            text = str(text)
            
            for form, pattern in self._compiled_patterns.items():
                if form in found_forms:
                    continue
                    
                if pattern.search(text):
                    results.append(ExtractionResult(
                        value=form,
                        source=source_name,
                        confidence=base_confidence,
                    ))
                    found_forms.add(form)
        
        return results
    
    def extract_primary(
        self,
        name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Optional[str]:
        """
        Extract the primary (most confident) dosage form.
        
        Args:
            name: Intervention name
            description: Intervention description
            
        Returns:
            Primary form or None if not found
        """
        results = self.extract(name, description)
        if results:
            best = max(results, key=lambda r: r.confidence)
            return best.value
        return None

