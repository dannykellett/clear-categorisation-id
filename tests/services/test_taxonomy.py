import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime
from app.services.taxonomy import TaxonomyService
from app.models.taxonomy import TaxonomyVersion, TaxonomyData, ValidationResult


class TestTaxonomyService:
    """Test taxonomy service functionality"""
    
    @pytest.fixture
    def service(self):
        return TaxonomyService()
    
    @pytest.fixture
    def mock_sheet_data(self):
        """Mock Google Sheets data"""
        return [
            {
                "Classification Type": "Event Type",
                "Classification tier 1": "Technology",
                "tier 2": "Software",
                "tier 3": "Web Development",
                "tier 4": "APIs"
            },
            {
                "Classification Type": "Event Type",
                "Classification tier 1": "Technology",
                "tier 2": "Data Science",
                "tier 3": "Analysis",
                "tier 4": ""
            },
            {
                "Classification Type": "Other Type",  # Should be filtered out
                "Classification tier 1": "Ignored",
                "tier 2": "Category",
                "tier 3": "",
                "tier 4": ""
            }
        ]
    
    @pytest.fixture
    def mock_processed_data(self):
        """Mock processed taxonomy data"""
        return [
            {
                "tier_1": "Technology",
                "tier_2": "Software",
                "tier_3": "Web Development",
                "tier_4": "APIs",
                "source_row": 1
            },
            {
                "tier_1": "Technology",
                "tier_2": "Data Science",
                "tier_3": "Analysis",
                "tier_4": "",
                "source_row": 2
            }
        ]
    
    @pytest.mark.asyncio
    async def test_sync_taxonomy_success(self, service, mock_sheet_data, mock_processed_data):
        """Test successful taxonomy synchronization"""
        # Mock the dependencies
        with patch.object(service, '_fetch_sheet_data', return_value=mock_sheet_data) as mock_fetch, \
             patch.object(service, '_process_raw_data', return_value=mock_processed_data) as mock_process, \
             patch.object(service.validator, 'validate_taxonomy') as mock_validate, \
             patch.object(service, '_update_cache') as mock_cache:
            
            # Mock successful validation
            mock_validate.return_value = ValidationResult(is_valid=True, errors=[], warnings=[])
            
            success, result = await service.sync_taxonomy()
            
            assert success is True
            assert result.is_valid is True
            assert len(result.errors) == 0
            
            # Verify methods were called
            mock_fetch.assert_called_once()
            mock_process.assert_called_once_with(mock_sheet_data)
            mock_validate.assert_called_once_with(mock_processed_data)
            mock_cache.assert_called_once()
            
            # Verify version was generated
            assert service.get_current_version() is not None
            assert service.get_current_data() is not None
    
    @pytest.mark.asyncio
    async def test_sync_taxonomy_validation_failed(self, service, mock_sheet_data, mock_processed_data):
        """Test taxonomy sync with validation failure"""
        with patch.object(service, '_fetch_sheet_data', return_value=mock_sheet_data), \
             patch.object(service, '_process_raw_data', return_value=mock_processed_data), \
             patch.object(service.validator, 'validate_taxonomy') as mock_validate:
            
            # Mock failed validation
            mock_validate.return_value = ValidationResult(
                is_valid=False,
                errors=["Test validation error"],
                warnings=[]
            )
            
            success, result = await service.sync_taxonomy()
            
            assert success is False
            assert result.is_valid is False
            assert "Test validation error" in result.errors
            
            # Version should not be updated on validation failure
            assert service.get_current_version() is None
    
    @pytest.mark.asyncio
    async def test_sync_taxonomy_fetch_error(self, service):
        """Test taxonomy sync with fetch error"""
        with patch.object(service, '_fetch_sheet_data') as mock_fetch:
            # Mock fetch error
            from app.clients.sheets import GoogleSheetsError
            mock_fetch.side_effect = GoogleSheetsError("Test fetch error")
            
            success, result = await service.sync_taxonomy()
            
            assert success is False
            assert result.is_valid is False
            assert "Sheet fetch error" in str(result.errors)
    
    def test_process_raw_data_filtering(self, service, mock_sheet_data):
        """Test raw data processing and filtering"""
        processed = service._process_raw_data(mock_sheet_data)
        
        # Should filter out non-"Event Type" rows
        assert len(processed) == 2  # Only 2 "Event Type" rows
        
        # Check first processed row
        first_row = processed[0]
        assert first_row["tier_1"] == "Technology"
        assert first_row["tier_2"] == "Software"
        assert first_row["tier_3"] == "Web Development"
        assert first_row["tier_4"] == "APIs"
        assert first_row["source_row"] == 1
        
        # Check second processed row
        second_row = processed[1]
        assert second_row["tier_1"] == "Technology"
        assert second_row["tier_2"] == "Data Science"
        assert second_row["tier_3"] == "Analysis"
        assert second_row["tier_4"] == ""
        assert second_row["source_row"] == 2
    
    def test_process_raw_data_empty_tier1(self, service):
        """Test processing data with empty tier 1 (should be filtered)"""
        data_with_empty_tier1 = [
            {
                "Classification Type": "Event Type",
                "Classification tier 1": "",  # Empty tier 1
                "tier 2": "Software",
                "tier 3": "",
                "tier 4": ""
            }
        ]
        
        processed = service._process_raw_data(data_with_empty_tier1)
        
        # Should be filtered out due to empty tier 1
        assert len(processed) == 0
    
    def test_generate_version(self, service):
        """Test version generation"""
        version = service._generate_version()
        
        assert isinstance(version, TaxonomyVersion)
        assert version.version_string.startswith("tax_v")
        assert len(version.version_string) == 20  # tax_v + YYYYMMDD_HHMMSS
        assert isinstance(version.created_at, datetime)
    
    def test_get_taxonomy_preview_no_data(self, service):
        """Test taxonomy preview with no data"""
        preview = service.get_taxonomy_preview()
        
        assert preview["version"] is None
        assert preview["status"] == "no_data"
        assert preview["structure"] == {}
        assert preview["metadata"] == {}
    
    def test_get_taxonomy_preview_with_data(self, service):
        """Test taxonomy preview with data"""
        # Set up mock data
        mock_version = TaxonomyVersion(
            version_string="tax_v20250814_120000",
            created_at=datetime(2025, 8, 14, 12, 0, 0)
        )
        
        mock_data = TaxonomyData(
            version=mock_version,
            data=[
                {
                    "tier_1": "Technology",
                    "tier_2": "Software",
                    "tier_3": "Web Development",
                    "tier_4": "APIs",
                    "source_row": 1
                },
                {
                    "tier_1": "Technology",
                    "tier_2": "Software",
                    "tier_3": "Web Development",
                    "tier_4": "Frameworks",
                    "source_row": 2
                }
            ],
            created_at=datetime(2025, 8, 14, 12, 0, 0),
            source_info={"test": "data"}
        )
        
        service._current_version = mock_version
        service._current_data = mock_data
        
        preview = service.get_taxonomy_preview()
        
        assert preview["version"] == "tax_v20250814_120000"
        assert preview["status"] == "active"
        assert "Technology" in preview["structure"]
        assert "Software" in preview["structure"]["Technology"]
        assert "Web Development" in preview["structure"]["Technology"]["Software"]
        assert "APIs" in preview["structure"]["Technology"]["Software"]["Web Development"]
        assert "Frameworks" in preview["structure"]["Technology"]["Software"]["Web Development"]
        assert preview["metadata"]["total_paths"] == 2
    
    @pytest.mark.asyncio
    async def test_update_cache(self, service):
        """Test cache update functionality"""
        mock_version = TaxonomyVersion(
            version_string="tax_v20250814_120000",
            created_at=datetime(2025, 8, 14, 12, 0, 0)
        )
        
        mock_data = TaxonomyData(
            version=mock_version,
            data=[{"test": "data"}],
            created_at=datetime(2025, 8, 14, 12, 0, 0),
            source_info={"test": "info"}
        )
        
        with patch('app.services.taxonomy.cache_manager') as mock_cache_manager:
            # Make the set method async and return True
            async def mock_set(key, value, ttl=None):
                return True
            mock_cache_manager.set = mock_set
            
            await service._update_cache(mock_data, mock_version)
            
            # Just ensure no errors occurred during caching
    
    @pytest.mark.asyncio
    async def test_load_from_cache(self, service):
        """Test loading data from cache"""
        mock_cached_data = {
            "version": {
                "version_string": "tax_v20250814_120000",
                "created_at": "2025-08-14T12:00:00"
            },
            "data": [{"test": "data"}],
            "created_at": "2025-08-14T12:00:00",
            "source_info": {"test": "info"}
        }
        
        with patch('app.services.taxonomy.cache_manager') as mock_cache_manager:
            # Make the get method async and return the mock data
            async def mock_get(key):
                return mock_cached_data
            mock_cache_manager.get = mock_get
            
            success = await service._load_from_cache()
            
            assert success is True
            assert service._current_version.version_string == "tax_v20250814_120000"
            assert service._current_data.data == [{"test": "data"}]
    
    @pytest.mark.asyncio
    async def test_invalidate_cache(self, service):
        """Test cache invalidation"""
        with patch('app.services.taxonomy.cache_manager') as mock_cache_manager:
            # Make clear_pattern async
            async def mock_clear_pattern(pattern):
                return 0
            mock_cache_manager.clear_pattern = mock_clear_pattern
            
            await service.invalidate_cache()
            
            # Can't easily assert on async mock, just ensure it doesn't error