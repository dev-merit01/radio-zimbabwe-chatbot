"""
Unit tests for vote parsing and normalization.
"""
import pytest
from apps.voting.services import parse_vote_input
from apps.voting.models import normalize_text, create_match_key, make_display_name


class TestParseVoteInput:
    """Tests for the parse_vote_input function."""
    
    def test_standard_format(self):
        """Test standard 'Artist - Song' format."""
        result = parse_vote_input("Winky D - Ijipita")
        assert result == ("Winky D", "Ijipita")
    
    def test_no_spaces_around_dash(self):
        """Test 'Artist-Song' with no spaces."""
        result = parse_vote_input("Killer T-Hwahwa")
        assert result == ("Killer T", "Hwahwa")
    
    def test_space_before_dash_only(self):
        """Test 'Artist -Song' format."""
        result = parse_vote_input("Jah Prayzah -Mudhara Vachauya")
        assert result == ("Jah Prayzah", "Mudhara Vachauya")
    
    def test_space_after_dash_only(self):
        """Test 'Artist- Song' format."""
        result = parse_vote_input("Freeman- Joina City")
        assert result == ("Freeman", "Joina City")
    
    def test_extra_whitespace(self):
        """Test input with extra whitespace."""
        result = parse_vote_input("  Tocky Vibes   -   Mhai   ")
        assert result == ("Tocky Vibes", "Mhai")
    
    def test_multiple_dashes_in_song(self):
        """Test song name containing a dash (only splits on first)."""
        result = parse_vote_input("Sungura Boys - Rudo-Rwemoyo")
        assert result == ("Sungura Boys", "Rudo-Rwemoyo")
    
    def test_no_dash_returns_song_only(self):
        """Test input without a dash returns song-only tuple (artist=None)."""
        result = parse_vote_input("Just some text without dash")
        # Song-only votes return (None, song_text) for helpful prompting
        assert result == (None, "Just some text without dash")
    
    def test_empty_artist_returns_none(self):
        """Test input with empty artist part returns None."""
        result = parse_vote_input("- Song Only")
        assert result is None
    
    def test_empty_song_returns_none(self):
        """Test input with empty song part returns None."""
        result = parse_vote_input("Artist Only -")
        assert result is None
    
    def test_too_short_parts_returns_none(self):
        """Test input with parts too short (< 2 chars) returns None."""
        result = parse_vote_input("A - B")
        assert result is None
    
    def test_empty_string_returns_none(self):
        """Test empty string returns None."""
        result = parse_vote_input("")
        assert result is None


class TestNormalizeText:
    """Tests for the normalize_text function."""
    
    def test_lowercase(self):
        """Test that text is lowercased."""
        assert normalize_text("WINKY D") == "winky d"
    
    def test_strip_whitespace(self):
        """Test that leading/trailing whitespace is stripped."""
        assert normalize_text("  Killer T  ") == "killer t"
    
    def test_collapse_multiple_spaces(self):
        """Test that multiple spaces are collapsed to one."""
        assert normalize_text("Jah   Prayzah") == "jah prayzah"
    
    def test_combined_normalization(self):
        """Test all normalization together."""
        assert normalize_text("  FREEMAN   THE   HKD  ") == "freeman the hkd"


class TestCreateMatchKey:
    """Tests for the create_match_key function."""
    
    def test_basic_match_key(self):
        """Test basic match key creation."""
        assert create_match_key("Winky D", "Ijipita") == "winky d::ijipita"
    
    def test_match_key_with_extra_spaces(self):
        """Test match key normalizes spaces."""
        key1 = create_match_key("Winky D", "Ijipita")
        key2 = create_match_key("  Winky   D  ", "  Ijipita  ")
        assert key1 == key2
    
    def test_match_key_case_insensitive(self):
        """Test match key is case insensitive."""
        key1 = create_match_key("WINKY D", "IJIPITA")
        key2 = create_match_key("winky d", "ijipita")
        assert key1 == key2


class TestMakeDisplayName:
    """Tests for the make_display_name function."""
    
    def test_basic_display_name(self):
        """Test basic display name creation."""
        assert make_display_name("Winky D", "Ijipita") == "Winky D - Ijipita"
    
    def test_display_name_trims_spaces(self):
        """Test display name trims extra spaces."""
        result = make_display_name("  Winky D  ", "  Ijipita  ")
        assert result == "Winky D - Ijipita"
    
    def test_display_name_collapses_internal_spaces(self):
        """Test display name collapses multiple internal spaces."""
        result = make_display_name("Jah   Prayzah", "Mwana   WaMambo")
        assert result == "Jah Prayzah - Mwana WaMambo"
