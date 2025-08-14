import pytest
import asyncio
from unittest.mock import patch, AsyncMock
from app.tasks.scheduler import TaxonomyScheduler
from app.models.taxonomy import ValidationResult


class TestTaxonomyScheduler:
    """Test taxonomy scheduler functionality"""
    
    @pytest.fixture
    def scheduler(self):
        return TaxonomyScheduler()
    
    @pytest.mark.asyncio
    async def test_scheduler_start_stop(self, scheduler):
        """Test scheduler start and stop operations"""
        assert not scheduler.is_running()
        
        await scheduler.start()
        assert scheduler.is_running()
        
        await scheduler.stop()
        assert not scheduler.is_running()
    
    @pytest.mark.asyncio
    async def test_scheduler_start_already_running(self, scheduler):
        """Test starting scheduler when already running"""
        await scheduler.start()
        
        # Starting again should log warning but not fail
        await scheduler.start()
        assert scheduler.is_running()
        
        await scheduler.stop()
    
    @pytest.mark.asyncio
    async def test_scheduler_stop_not_running(self, scheduler):
        """Test stopping scheduler when not running"""
        # Should not raise error
        await scheduler.stop()
        assert not scheduler.is_running()
    
    @pytest.mark.asyncio
    async def test_perform_sync_success(self, scheduler):
        """Test successful sync operation"""
        mock_validation_result = ValidationResult(
            is_valid=True,
            errors=[],
            warnings=[]
        )
        
        with patch('app.tasks.scheduler.taxonomy_service') as mock_service:
            mock_service.sync_taxonomy = AsyncMock(return_value=(True, mock_validation_result))
            mock_service.get_current_version.return_value = type('Version', (), {
                'version_string': 'tax_v20250814_120000'
            })()
            
            await scheduler._perform_sync("test")
            
            mock_service.sync_taxonomy.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_perform_sync_validation_failure(self, scheduler):
        """Test sync with validation failure"""
        mock_validation_result = ValidationResult(
            is_valid=False,
            errors=["Test validation error"],
            warnings=[]
        )
        
        with patch('app.tasks.scheduler.taxonomy_service') as mock_service:
            mock_service.sync_taxonomy = AsyncMock(return_value=(False, mock_validation_result))
            
            # Mock the alert method
            with patch.object(scheduler, '_emit_sync_alert') as mock_alert:
                await scheduler._perform_sync("test")
                
                mock_alert.assert_called_once_with("validation_failed", ["Test validation error"])
    
    @pytest.mark.asyncio
    async def test_perform_sync_exception(self, scheduler):
        """Test sync with exception"""
        with patch('app.tasks.scheduler.taxonomy_service') as mock_service:
            mock_service.sync_taxonomy = AsyncMock(side_effect=Exception("Test error"))
            
            # Mock the alert method
            with patch.object(scheduler, '_emit_sync_alert') as mock_alert:
                await scheduler._perform_sync("test")
                
                mock_alert.assert_called_once_with("sync_failed", ["Test error"])
    
    @pytest.mark.asyncio
    async def test_emit_sync_alert(self, scheduler):
        """Test alert emission functionality"""
        # This is mostly a logging function, just ensure it doesn't crash
        await scheduler._emit_sync_alert("test_alert", ["error1", "error2"])