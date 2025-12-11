"""
Unit tests for fuzzy matching functionality.
"""
import pytest
from apps.voting.matching import (
    similarity_ratio,
    levenshtein_distance,
    is_similar,
    match_verified_artist,
    clear_artist_cache,
)


class TestSimilarityRatio:
    """Tests for the similarity_ratio function."""
    
    def test_identical_strings(self):
        """Test identical strings return 1.0."""
        assert similarity_ratio("Winky D", "Winky D") == 1.0
    
    def test_completely_different(self):
        """Test completely different strings return low score."""
        score = similarity_ratio("abc", "xyz")
        assert score < 0.3
    
    def test_case_insensitive(self):
        """Test matching is case insensitive."""
        assert similarity_ratio("WINKY D", "winky d") == 1.0
    
    def test_minor_typo(self):
        """Test minor typo still gives high score."""
        score = similarity_ratio("Jah Prayzah", "Jah Prayzha")
        assert score > 0.85
    
    def test_partial_match(self):
        """Test partial matches give medium scores."""
        score = similarity_ratio("Killer T", "Killer")
        assert 0.5 < score < 0.9


class TestLevenshteinDistance:
    """Tests for the levenshtein_distance function."""
    
    def test_identical_strings(self):
        """Test identical strings have distance 0."""
        assert levenshtein_distance("hello", "hello") == 0
    
    def test_one_char_difference(self):
        """Test one character difference has distance 1."""
        assert levenshtein_distance("hello", "hallo") == 1
    
    def test_insertion(self):
        """Test insertion gives correct distance."""
        assert levenshtein_distance("helo", "hello") == 1
    
    def test_deletion(self):
        """Test deletion gives correct distance."""
        assert levenshtein_distance("hello", "helo") == 1
    
    def test_empty_strings(self):
        """Test empty string cases."""
        assert levenshtein_distance("", "hello") == 5
        assert levenshtein_distance("hello", "") == 5
        assert levenshtein_distance("", "") == 0


class TestIsSimilar:
    """Tests for the is_similar function."""
    
    def test_exact_match(self):
        """Test exact matches return True."""
        assert is_similar("Winky D", "Winky D") is True
    
    def test_normalized_match(self):
        """Test normalized text matches return True."""
        assert is_similar("  WINKY  D  ", "winky d") is True
    
    def test_similar_with_typo(self):
        """Test similar strings with typo return True at default threshold."""
        assert is_similar("Jah Prayzah", "Jah Prayzha", threshold=0.85) is True
    
    def test_not_similar(self):
        """Test different strings return False."""
        assert is_similar("Winky D", "Freeman", threshold=0.85) is False
    
    def test_custom_threshold(self):
        """Test custom threshold is respected."""
        # These are about 70% similar
        assert is_similar("Killer", "Killa", threshold=0.60) is True
        assert is_similar("Killer", "Killa", threshold=0.90) is False


class TestMatchVerifiedArtist:
    """Tests for verified artist matching."""
    
    def setup_method(self):
        """Clear cache before each test."""
        clear_artist_cache()
    
    @pytest.mark.django_db
    def test_no_match_returns_none(self):
        """Test non-existent artist returns None."""
        # With the seeded database, an unknown artist should return None
        result = match_verified_artist("Unknown Artist XYZ That Does Not Exist")
        assert result is None


class TestFuzzyMatchingEdgeCases:
    """Edge case tests for fuzzy matching."""
    
    def test_empty_strings(self):
        """Test empty strings don't crash."""
        assert similarity_ratio("", "") == 1.0
        assert similarity_ratio("test", "") == 0.0
    
    def test_unicode_characters(self):
        """Test Unicode characters are handled."""
        score = similarity_ratio("Mädchen", "Madchen")
        assert score > 0.8
    
    def test_numbers_in_names(self):
        """Test numbers in names work correctly."""
        score = similarity_ratio("Killer T", "Killer T2")
        assert score > 0.8
