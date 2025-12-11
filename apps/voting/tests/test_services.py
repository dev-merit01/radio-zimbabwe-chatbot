"""
Unit tests for the VotingService.
"""
import pytest
from datetime import date
from unittest.mock import patch, MagicMock
from django.test import TestCase
from django.utils import timezone

from apps.voting.models import User, RawVote, RawSongTally
from apps.voting.services import VotingService, MAX_VOTES_PER_DAY


class TestVotingServiceWelcome(TestCase):
    """Tests for welcome and help messages."""
    
    def setUp(self):
        self.service = VotingService(channel='telegram', user_ref='12345')
    
    def test_start_command_returns_welcome(self):
        """Test /start returns welcome message."""
        response = self.service.handle_incoming_text('/start')
        assert '🎶 Welcome to Radio Zimbabwe Top 100!' in response
    
    def test_start_lowercase_returns_welcome(self):
        """Test 'start' (lowercase) returns welcome message."""
        response = self.service.handle_incoming_text('start')
        assert '🎶 Welcome to Radio Zimbabwe Top 100!' in response
    
    def test_help_command_returns_help(self):
        """Test /help returns help message."""
        response = self.service.handle_incoming_text('/help')
        assert '📋 How to vote:' in response
    
    def test_empty_text_returns_welcome(self):
        """Test empty text returns welcome message."""
        response = self.service.handle_incoming_text('')
        assert '🎶 Welcome to Radio Zimbabwe Top 100!' in response


class TestVotingServiceVoting(TestCase):
    """Tests for vote recording functionality."""
    
    def setUp(self):
        self.service = VotingService(channel='telegram', user_ref='test_user_123')
    
    @patch('apps.voting.services.timezone')
    def test_valid_vote_is_recorded(self, mock_tz):
        """Test a valid vote is recorded successfully."""
        mock_tz.localdate.return_value = date(2025, 12, 10)
        
        response = self.service.handle_incoming_text('Winky D - Ijipita')
        
        assert '✅ Vote recorded!' in response
        assert 'Winky D - Ijipita' in response
        
        # Verify vote was saved
        vote = RawVote.objects.filter(
            user__user_ref='test_user_123',
            display_name__icontains='Winky D'
        ).first()
        assert vote is not None
    
    @patch('apps.voting.services.timezone')
    def test_song_only_vote_accepted(self, mock_tz):
        """Test song-only input (no artist) is accepted."""
        mock_tz.localdate.return_value = date(2025, 12, 10)
        
        response = self.service.handle_incoming_text('Ijipita')
        
        # Song-only votes are now accepted with "Unknown Artist"
        assert '✅ Vote recorded!' in response
        assert 'Unknown Artist - Ijipita' in response
    
    @patch('apps.voting.services.timezone')
    def test_song_only_matches_existing_artist(self, mock_tz):
        """Test song-only input matches existing song and uses its artist."""
        from apps.voting.models import CleanedSong
        
        mock_tz.localdate.return_value = date(2025, 12, 10)
        
        # Create an existing CleanedSong directly
        CleanedSong.objects.create(
            artist='Killer T',
            title='Takabva Kure',
            canonical_name='Killer T - Takabva Kure',
            status='verified'
        )
        
        # Now vote with song title only - should match existing song
        response = self.service.handle_incoming_text('Takabva Kure')
        
        # Should match the existing song and use Killer T as artist
        assert '✅ Vote recorded!' in response
        assert 'Killer T' in response
        assert 'Unknown Artist' not in response

    @patch('apps.voting.services.timezone')
    def test_duplicate_vote_same_day_rejected(self, mock_tz):
        """Test voting for the same song twice in one day is rejected."""
        mock_tz.localdate.return_value = date(2025, 12, 10)
        
        # First vote
        response1 = self.service.handle_incoming_text('Jah Prayzah - Mudhara')
        assert '✅ Vote recorded!' in response1
        
        # Second vote for same song
        response2 = self.service.handle_incoming_text('Jah Prayzah - Mudhara')
        assert '⚠️ You already voted for' in response2
    
    @patch('apps.voting.services.timezone')
    def test_vote_limit_enforced(self, mock_tz):
        """Test that users cannot exceed MAX_VOTES_PER_DAY."""
        # Use a unique date to avoid collisions with other tests
        mock_tz.localdate.return_value = date(2099, 1, 1)
        
        # Create a fresh service with unique user ref for this test
        service = VotingService(channel='telegram', user_ref='limit_test_user_unique_99')
        
        # Cast MAX_VOTES_PER_DAY votes with VERY distinct song names
        songs = [
            'Alpha Romeo - Zephyr Moon Dancing',
            'Bravo Charlie - Quantum Stars Rising',
            'Delta Echo - Midnight Sun Falling',
            'Foxtrot Golf - Crystal Lake Shining',
            'Hotel India - Thunder Valley Calling',
        ]
        
        for i, song in enumerate(songs[:MAX_VOTES_PER_DAY]):
            response = service.handle_incoming_text(song)
            assert '✅ Vote recorded!' in response, f"Vote {i+1} failed: {response}"
        
        # Try to cast one more
        response = service.handle_incoming_text('Juliet Kilo - Ocean Waves Breaking')
        assert f'🚫 You have used all {MAX_VOTES_PER_DAY} votes for today' in response
    
    @patch('apps.voting.services.timezone')
    def test_remaining_votes_displayed(self, mock_tz):
        """Test remaining votes count is displayed after voting."""
        mock_tz.localdate.return_value = date(2025, 12, 10)
        
        response = self.service.handle_incoming_text('Freeman - Joina City')
        
        remaining = MAX_VOTES_PER_DAY - 1
        assert f'{remaining} vote' in response
    
    @patch('apps.voting.services.timezone')
    def test_tally_is_updated(self, mock_tz):
        """Test that RawSongTally is updated when a vote is cast."""
        mock_tz.localdate.return_value = date(2025, 12, 10)
        
        # Vote for a song
        self.service.handle_incoming_text('Tocky Vibes - Mhai')
        
        # Create a second user and vote for the same song
        service2 = VotingService(channel='telegram', user_ref='another_user')
        service2.handle_incoming_text('Tocky Vibes - Mhai')
        
        # Check tally
        tally = RawSongTally.objects.filter(
            display_name__icontains='Tocky Vibes'
        ).first()
        
        assert tally is not None
        assert tally.count == 2


class TestVotingServiceChannels(TestCase):
    """Tests for multi-channel support."""
    
    @patch('apps.voting.services.timezone')
    def test_telegram_and_whatsapp_users_separate(self, mock_tz):
        """Test that Telegram and WhatsApp users are tracked separately."""
        mock_tz.localdate.return_value = date(2025, 12, 10)
        
        same_ref = '123456789'
        
        telegram_service = VotingService(channel='telegram', user_ref=same_ref)
        whatsapp_service = VotingService(channel='whatsapp', user_ref=same_ref)
        
        # Both can vote for the same song
        response1 = telegram_service.handle_incoming_text('Selmor Mtukudzi - Amai')
        response2 = whatsapp_service.handle_incoming_text('Selmor Mtukudzi - Amai')
        
        # Both should succeed
        assert '✅ Vote recorded!' in response1
        assert '✅ Vote recorded!' in response2
        
        # Should create two separate users
        users = User.objects.filter(user_ref=same_ref)
        assert users.count() == 2
