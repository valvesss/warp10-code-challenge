"""Tests for route and dosage form extractors."""

import pytest

from src.transformation.extractors import RouteExtractor, DosageFormExtractor


class TestRouteExtractor:
    """Tests for RouteExtractor."""
    
    @pytest.fixture
    def extractor(self):
        return RouteExtractor()
    
    def test_extract_oral_from_description(self, extractor):
        """Should extract oral route from description."""
        result = extractor.extract_primary(
            description="Administered orally once daily"
        )
        assert result == "ORAL"
    
    def test_extract_oral_abbreviation(self, extractor):
        """Should extract oral from p.o. abbreviation."""
        result = extractor.extract_primary(
            description="Given p.o. twice daily"
        )
        assert result == "ORAL"
    
    def test_extract_intravenous(self, extractor):
        """Should extract IV route."""
        result = extractor.extract_primary(
            description="Intravenous infusion over 2 hours"
        )
        assert result == "INTRAVENOUS"
    
    def test_extract_iv_abbreviation(self, extractor):
        """Should extract IV from abbreviation."""
        result = extractor.extract_primary(
            description="Given IV q8h"
        )
        assert result == "INTRAVENOUS"
    
    def test_extract_subcutaneous(self, extractor):
        """Should extract subcutaneous route."""
        result = extractor.extract_primary(
            description="Subcutaneous injection"
        )
        assert result == "SUBCUTANEOUS"
    
    def test_extract_intramuscular(self, extractor):
        """Should extract intramuscular route."""
        result = extractor.extract_primary(
            description="Intramuscular injection in deltoid"
        )
        assert result == "INTRAMUSCULAR"
    
    def test_extract_topical(self, extractor):
        """Should extract topical route."""
        result = extractor.extract_primary(
            description="Apply topically to affected area"
        )
        assert result == "TOPICAL"
    
    def test_extract_inhalation(self, extractor):
        """Should extract inhalation route."""
        result = extractor.extract_primary(
            description="Inhaled via nebulizer"
        )
        assert result == "INHALATION"
    
    def test_extract_from_name(self, extractor):
        """Should extract route from drug name."""
        result = extractor.extract_primary(
            name="Drug Oral Solution"
        )
        # Oral is inferred from "solution" pattern
        assert result is not None
    
    def test_extract_from_design_group(self, extractor):
        """Should extract route from design group description."""
        result = extractor.extract_primary(
            design_group_desc="IV infusion group receives drug over 1 hour"
        )
        assert result == "INTRAVENOUS"
    
    def test_no_match_returns_none(self, extractor):
        """Should return None when no route found."""
        result = extractor.extract_primary(
            name="Drug XYZ",
            description="Some description without route info"
        )
        assert result is None
    
    def test_extract_multiple_routes(self, extractor):
        """Should extract multiple routes."""
        results = extractor.extract(
            description="Can be given orally or intravenously"
        )
        routes = [r.value for r in results]
        assert "ORAL" in routes
        assert "INTRAVENOUS" in routes
    
    def test_description_higher_confidence(self, extractor):
        """Description should have higher confidence than name."""
        results = extractor.extract(
            name="Oral Drug",
            description="Given intravenously"
        )
        # IV from description should have higher confidence
        best = max(results, key=lambda r: r.confidence)
        assert best.value == "INTRAVENOUS"
        assert best.source == "description"
    
    def test_handles_none_inputs(self, extractor):
        """Should handle None inputs gracefully."""
        result = extractor.extract_primary()
        assert result is None
    
    def test_rectal_route(self, extractor):
        """Should extract rectal route."""
        result = extractor.extract_primary(
            description="Rectal suppository"
        )
        assert result == "RECTAL"
    
    def test_transdermal_route(self, extractor):
        """Should extract transdermal route."""
        result = extractor.extract_primary(
            description="Applied via transdermal patch"
        )
        assert result == "TRANSDERMAL"


class TestDosageFormExtractor:
    """Tests for DosageFormExtractor."""
    
    @pytest.fixture
    def extractor(self):
        return DosageFormExtractor()
    
    def test_extract_tablet(self, extractor):
        """Should extract tablet form."""
        result = extractor.extract_primary(
            name="Drug Tablet"
        )
        assert result == "TABLET"
    
    def test_extract_tablet_plural(self, extractor):
        """Should extract tablets (plural)."""
        result = extractor.extract_primary(
            description="Two tablets daily"
        )
        assert result == "TABLET"
    
    def test_extract_capsule(self, extractor):
        """Should extract capsule form."""
        result = extractor.extract_primary(
            name="Drug 100mg Capsule"
        )
        assert result == "CAPSULE"
    
    def test_extract_injection(self, extractor):
        """Should extract injection form."""
        result = extractor.extract_primary(
            description="Given as injection"
        )
        assert result == "INJECTION"
    
    def test_extract_solution(self, extractor):
        """Should extract solution form."""
        result = extractor.extract_primary(
            name="Drug Oral Solution"
        )
        assert result == "SOLUTION"
    
    def test_extract_cream(self, extractor):
        """Should extract cream form."""
        result = extractor.extract_primary(
            description="Apply cream twice daily"
        )
        assert result == "CREAM"
    
    def test_extract_patch(self, extractor):
        """Should extract patch form."""
        result = extractor.extract_primary(
            name="Nicotine Patch"
        )
        assert result == "PATCH"
    
    def test_extract_inhaler(self, extractor):
        """Should extract inhaler form."""
        result = extractor.extract_primary(
            description="Administered via inhaler"
        )
        assert result == "INHALER"
    
    def test_extract_drops(self, extractor):
        """Should extract drops form."""
        result = extractor.extract_primary(
            name="Eye Drops"
        )
        assert result == "DROPS"
    
    def test_no_match_returns_none(self, extractor):
        """Should return None when no form found."""
        result = extractor.extract_primary(
            name="Drug",
            description="General description"
        )
        assert result is None
    
    def test_extract_multiple_forms(self, extractor):
        """Should extract multiple forms."""
        results = extractor.extract(
            description="Available as tablet or capsule"
        )
        forms = [r.value for r in results]
        assert "TABLET" in forms
        assert "CAPSULE" in forms
    
    def test_handles_none_inputs(self, extractor):
        """Should handle None inputs gracefully."""
        result = extractor.extract_primary()
        assert result is None
    
    def test_suppository(self, extractor):
        """Should extract suppository form."""
        result = extractor.extract_primary(
            description="Rectal suppository"
        )
        assert result == "SUPPOSITORY"
    
    def test_infusion(self, extractor):
        """Should extract infusion form."""
        result = extractor.extract_primary(
            description="IV infusion bag"
        )
        assert result == "INFUSION"

