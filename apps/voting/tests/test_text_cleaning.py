"""
Tests for text cleaning utilities.
"""
import pytest
from apps.voting.text_cleaning import (
    correct_artist_typo,
    normalize_common_words,
    clean_song_title,
    extract_featured_artists,
    parse_artist_with_features,
    clean_vote_text,
    ARTIST_TYPO_CORRECTIONS,
)


class TestTypoCorrection:
    """Tests for common typo correction."""
    
    def test_winky_d_typos(self):
        assert correct_artist_typo('winkyd') == 'Winky D'
        assert correct_artist_typo('winky') == 'Winky D'
        assert correct_artist_typo('Winky D') == 'Winky D'  # Already correct
    
    def test_jah_prayzah_typos(self):
        assert correct_artist_typo('jah prayza') == 'Jah Prayzah'
        assert correct_artist_typo('jahprayzah') == 'Jah Prayzah'
        assert correct_artist_typo('jah praiza') == 'Jah Prayzah'
    
    def test_holy_ten_typos(self):
        assert correct_artist_typo('holyten') == 'Holy Ten'
        assert correct_artist_typo('holy 10') == 'Holy Ten'
        assert correct_artist_typo('hollyten') == 'Holy Ten'
    
    def test_killer_t_typos(self):
        assert correct_artist_typo('killert') == 'Killer T'
        assert correct_artist_typo('killa t') == 'Killer T'
    
    def test_unknown_artist_unchanged(self):
        assert correct_artist_typo('Some New Artist') == 'Some New Artist'


class TestNormalizeCommonWords:
    """Tests for normalizing common abbreviations."""
    
    def test_ft_variations(self):
        assert 'feat.' in normalize_common_words('Winky D ft Holy Ten')
        assert 'feat.' in normalize_common_words('Winky D ft. Holy Ten')
        assert 'feat.' in normalize_common_words('Winky D featuring Holy Ten')
    
    def test_ampersand_to_and(self):
        result = normalize_common_words('Winky D & Holy Ten')
        assert 'and' in result
        assert '&' not in result
    
    def test_x_collaboration(self):
        result = normalize_common_words('Winky D x Holy Ten')
        assert 'feat.' in result
    
    def test_removes_prod_by(self):
        result = normalize_common_words('Song prod. by Producer')
        # Removes "prod. by" but may leave "Producer"
        assert 'prod.' not in result.lower()
        assert 'prod by' not in result.lower()


class TestCleanSongTitle:
    """Tests for cleaning song titles."""
    
    def test_removes_official_video(self):
        assert clean_song_title('Ijipita (Official Video)') == 'Ijipita'
        assert clean_song_title('Ijipita [Official Video]') == 'Ijipita'
    
    def test_removes_official_audio(self):
        assert clean_song_title('Ijipita (Official Audio)') == 'Ijipita'
    
    def test_removes_lyrics_video(self):
        assert clean_song_title('Ijipita (Lyrics Video)') == 'Ijipita'
        assert clean_song_title('Ijipita (Lyric Video)') == 'Ijipita'
    
    def test_removes_hd(self):
        assert clean_song_title('Ijipita (HD)') == 'Ijipita'
        assert clean_song_title('Ijipita [4K]') == 'Ijipita'
    
    def test_preserves_normal_title(self):
        assert clean_song_title('Ijipita') == 'Ijipita'
        assert clean_song_title('Mwana WaMambo') == 'Mwana WaMambo'


class TestExtractFeaturedArtists:
    """Tests for extracting featured artists."""
    
    def test_feat_extraction(self):
        main, featured = extract_featured_artists('Winky D feat. Holy Ten')
        assert main == 'Winky D'
        assert featured == ['Holy Ten']
    
    def test_multiple_featured(self):
        main, featured = extract_featured_artists('Winky D feat. Holy Ten, Freeman')
        assert main == 'Winky D'
        assert 'Holy Ten' in featured
        assert 'Freeman' in featured
    
    def test_featuring_keyword(self):
        main, featured = extract_featured_artists('Winky D featuring Holy Ten')
        # After normalize_common_words converts it
        normalized = normalize_common_words('Winky D featuring Holy Ten')
        main, featured = extract_featured_artists(normalized)
        assert main == 'Winky D'
        assert 'Holy Ten' in featured
    
    def test_no_featured(self):
        main, featured = extract_featured_artists('Winky D')
        assert main == 'Winky D'
        assert featured == []


class TestCleanVoteText:
    """Tests for the main cleaning function."""
    
    def test_full_cleaning_pipeline(self):
        artist, song = clean_vote_text(
            'winkyd ft holyten',
            'Ijipita (Official Video)'
        )
        assert artist == 'Winky D feat. Holy Ten'
        assert song == 'Ijipita'
    
    def test_typo_correction_in_pipeline(self):
        artist, song = clean_vote_text('jah prayza', 'Kutonga Kwaro')
        assert artist == 'Jah Prayzah'
    
    def test_preserves_normal_input(self):
        artist, song = clean_vote_text('Winky D', 'Ijipita')
        assert artist == 'Winky D'
        assert song == 'Ijipita'
    
    def test_handles_ampersand(self):
        artist, song = clean_vote_text('Winky D & Holy Ten', 'Song')
        assert 'and' in artist or 'feat.' in artist
