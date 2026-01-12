"""Tests for normalizers."""

import pytest

from src.transformation.normalizers import OrganizationNormalizer, DrugNormalizer


class TestOrganizationNormalizer:
    """Tests for OrganizationNormalizer."""
    
    @pytest.fixture
    def normalizer(self):
        return OrganizationNormalizer()
    
    def test_removes_inc_suffix(self, normalizer):
        """Should remove Inc. suffix."""
        assert normalizer.normalize("Pfizer Inc.") == "Pfizer"
        assert normalizer.normalize("Pfizer, Inc") == "Pfizer"
        assert normalizer.normalize("Pfizer Inc") == "Pfizer"
    
    def test_removes_ltd_suffix(self, normalizer):
        """Should remove Ltd suffix."""
        assert normalizer.normalize("GlaxoSmithKline Ltd.") == "GlaxoSmithKline"
        assert normalizer.normalize("AstraZeneca Limited") == "AstraZeneca"
    
    def test_removes_corp_suffix(self, normalizer):
        """Should remove Corp suffix."""
        assert normalizer.normalize("Johnson Corporation") == "Johnson"
        assert normalizer.normalize("Merck Corp.") == "Merck"
    
    def test_removes_llc_suffix(self, normalizer):
        """Should remove LLC suffix."""
        assert normalizer.normalize("BioNTech LLC") == "BioNTech"
        assert normalizer.normalize("Moderna L.L.C.") == "Moderna"
    
    def test_removes_gmbh_suffix(self, normalizer):
        """Should remove GmbH suffix."""
        assert normalizer.normalize("Bayer GmbH") == "Bayer"
    
    def test_normalizes_whitespace(self, normalizer):
        """Should normalize multiple spaces."""
        assert normalizer.normalize("Pfizer   Inc.") == "Pfizer"
        assert normalizer.normalize("  Pfizer  ") == "Pfizer"
    
    def test_handles_none(self, normalizer):
        """Should handle None values."""
        assert normalizer.normalize(None) == ""
    
    def test_handles_empty_string(self, normalizer):
        """Should handle empty string."""
        assert normalizer.normalize("") == ""
    
    def test_normalize_for_key_lowercase(self, normalizer):
        """Should create lowercase key."""
        key = normalizer.normalize_for_key("Pfizer Inc.")
        assert key == "pfizer"
    
    def test_normalize_for_key_removes_special_chars(self, normalizer):
        """Should remove special characters from key."""
        key = normalizer.normalize_for_key("Johnson & Johnson")
        assert key == "johnson johnson"  # ampersand removed, spaces normalized
    
    def test_get_display_name_preserves_case(self, normalizer):
        """Should preserve case for display."""
        display = normalizer.get_display_name("Pfizer Inc.")
        assert display == "Pfizer"  # Suffix removed but case preserved


class TestDrugNormalizer:
    """Tests for DrugNormalizer."""
    
    @pytest.fixture
    def normalizer(self):
        return DrugNormalizer()
    
    def test_normalize_basic(self, normalizer):
        """Should normalize basic drug name."""
        assert normalizer.normalize("Aspirin") == "Aspirin"
        assert normalizer.normalize("  Aspirin  ") == "Aspirin"
    
    def test_normalize_for_key_removes_dosage_mg(self, normalizer):
        """Should remove mg dosage from key."""
        key = normalizer.normalize_for_key("Aspirin 100mg")
        assert "100" not in key
        assert "mg" not in key
    
    def test_normalize_for_key_removes_dosage_ml(self, normalizer):
        """Should remove ml dosage from key."""
        key = normalizer.normalize_for_key("Ibuprofen 5ml")
        assert "5" not in key or "ml" not in key.lower()
    
    def test_normalize_for_key_removes_percentage(self, normalizer):
        """Should remove percentage from key."""
        key = normalizer.normalize_for_key("Lidocaine 2%")
        assert "2" not in key
        assert "%" not in key
    
    def test_normalize_for_key_removes_ratio_dosage(self, normalizer):
        """Should remove ratio dosage from key."""
        key = normalizer.normalize_for_key("Drug 100/5mg")
        assert key == "drug"
    
    def test_normalize_for_key_lowercase(self, normalizer):
        """Should create lowercase key."""
        key = normalizer.normalize_for_key("ASPIRIN")
        assert key == "aspirin"
    
    def test_handles_none(self, normalizer):
        """Should handle None values."""
        assert normalizer.normalize(None) == ""
        assert normalizer.normalize_for_key(None) == ""
    
    def test_handles_empty_string(self, normalizer):
        """Should handle empty string."""
        assert normalizer.normalize("") == ""
        assert normalizer.normalize_for_key("") == ""
    
    def test_preserves_hyphens_in_key(self, normalizer):
        """Should preserve hyphens in drug name key."""
        key = normalizer.normalize_for_key("beta-blocker")
        assert "-" in key
    
    def test_get_display_name(self, normalizer):
        """Should return clean display name."""
        display = normalizer.get_display_name("  Aspirin  ")
        assert display == "Aspirin"

