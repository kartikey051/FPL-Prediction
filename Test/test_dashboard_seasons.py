"""
Integration tests for FPL Dashboard season switching.

Tests verify that all dashboard endpoints work correctly across different seasons,
especially for seasons with and without teams table support.
"""

import pytest
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

# Test seasons covering different capability combinations
TEST_SEASONS = [
    ("2024-25", True, True),   # Current: has teams, has understat
    ("2023-24", True, True),   # Historical with teams
    ("2019-20", True, True),   # First season with teams
    ("2018-19", False, True),  # No teams table
    ("2016-17", False, True),  # Earliest season
]


class TestSeasonSchema:
    """Test season schema configuration."""
    
    def test_schema_returns_typed_object(self):
        from app.api.dashboard.season_config import get_season_schema, SeasonSchema
        
        schema = get_season_schema("2024-25")
        assert isinstance(schema, SeasonSchema)
        assert schema.name == "2024-25"
        assert schema.is_historical is False
    
    def test_historical_season_schema(self):
        from app.api.dashboard.season_config import get_season_schema
        
        schema = get_season_schema("2019-20")
        assert schema.is_historical is True
        assert schema.supports_teams is True
        assert schema.table_fact == "fpl_player_gameweeks"
    
    def test_pre_teams_season_schema(self):
        from app.api.dashboard.season_config import get_season_schema
        
        schema = get_season_schema("2018-19")
        assert schema.is_historical is True
        assert schema.supports_teams is False
        assert schema.supports_standings is False


class TestSummaryEndpoint:
    """Test /dashboard/summary endpoint across seasons."""
    
    @pytest.mark.parametrize("season,has_teams,expected_ok", TEST_SEASONS)
    def test_summary_returns_200(self, season, has_teams, expected_ok):
        # Note: This test requires valid auth token
        # For now, we test the service function directly
        from app.api.dashboard.service import get_summary_stats
        
        result = get_summary_stats(season=season)
        assert result is not None
        assert hasattr(result, 'total_players')
        assert hasattr(result, 'total_goals')


class TestTrendsEndpoint:
    """Test /dashboard/trends endpoint - must return gap-free data."""
    
    @pytest.mark.parametrize("season,has_teams,expected_ok", TEST_SEASONS)
    def test_trends_no_gaps(self, season, has_teams, expected_ok):
        from app.api.dashboard.service import get_gameweek_trends
        
        result = get_gameweek_trends(season=season)
        assert result is not None
        assert hasattr(result, 'data')
        
        if result.data:
            # Verify no gaps in gameweek sequence
            gameweeks = [p.gameweek for p in result.data]
            expected = list(range(1, len(gameweeks) + 1))
            assert gameweeks == expected, f"Gap detected in trends for {season}"


class TestTopPlayersEndpoint:
    """Test /dashboard/top-players endpoint."""
    
    @pytest.mark.parametrize("season,has_teams,expected_ok", TEST_SEASONS)
    def test_top_players_no_keyerror(self, season, has_teams, expected_ok):
        from app.api.dashboard.service import get_top_players
        
        # This should NEVER raise KeyError
        result = get_top_players(limit=5, season=season)
        assert isinstance(result, list)


class TestStandingsEndpoint:
    """Test /dashboard/standings endpoint with graceful degradation."""
    
    def test_standings_with_teams(self):
        from app.api.dashboard.service import get_league_standings
        
        result = get_league_standings(season="2023-24")
        assert result is not None
        # Should have standings for seasons with teams
    
    def test_standings_without_teams_graceful(self):
        from app.api.dashboard.service import get_league_standings
        
        # Pre-2019 seasons have no teams - should return empty, not crash
        result = get_league_standings(season="2018-19")
        assert result is not None
        assert result.standings == []  # Graceful empty response


class TestPlayerSearch:
    """Test /dashboard/search/players endpoint."""
    
    def test_search_all_seasons(self):
        from app.api.dashboard.service import get_global_players
        from app.api.dashboard.schemas import GlobalSearchFilters
        
        for season, _, _ in TEST_SEASONS:
            filters = GlobalSearchFilters(season=season, name="Kane")
            result = get_global_players(filters)
            assert isinstance(result, list)  # Should not crash


class TestPlayerTrends:
    """Test /dashboard/players/{id}/trends endpoint."""
    
    def test_player_trends_no_keyerror(self):
        from app.api.dashboard.service import get_player_trends
        
        # Test with a sample player ID
        result = get_player_trends(player_id=1, season="2023-24")
        assert result is not None
        assert hasattr(result, 'trend')


class TestNoKeyErrors:
    """Verify no KeyError is raised for any season."""
    
    def test_all_services_no_keyerror(self):
        """Comprehensive test that no service function raises KeyError."""
        from app.api.dashboard.service import (
            get_summary_stats, get_gameweek_trends, get_top_players,
            get_league_standings, get_player_trends, get_distributions,
            get_global_players, get_available_filters
        )
        from app.api.dashboard.schemas import GlobalSearchFilters
        
        all_seasons = [s for s, _, _ in TEST_SEASONS]
        
        for season in all_seasons:
            try:
                get_summary_stats(season=season)
                get_gameweek_trends(season=season)
                get_top_players(season=season)
                get_league_standings(season=season)
                get_player_trends(player_id=1, season=season)
                get_distributions(season=season)
                get_global_players(GlobalSearchFilters(season=season))
            except KeyError as e:
                pytest.fail(f"KeyError raised for season {season}: {e}")
        
        # get_available_filters doesn't take season
        get_available_filters()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
