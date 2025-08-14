import asyncio
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timezone
import json
from app.clients.sheets import sheets_client, GoogleSheetsError
from app.validators.taxonomy import TaxonomyValidator, ValidationError
from app.models.taxonomy import TaxonomyData, TaxonomyVersion, ValidationResult
from app.core.logging import get_logger, log_with_context
from app.core.config import config
from app.core.cache import cache_manager

logger = get_logger(__name__)


class TaxonomyService:
    """Service for managing taxonomy data synchronization and validation"""
    
    def __init__(self):
        self.sheet_id = "1LQEbAKH7KjAFfuMR1pWK9rSqaBfpkRi8DDw_7gU_qTg"
        self.tab_name = "event_type"
        self.validator = TaxonomyValidator()
        self._current_version: Optional[TaxonomyVersion] = None
        self._current_data: Optional[TaxonomyData] = None
        self.cache_key_current = "taxonomy:current"
        self.cache_key_version = "taxonomy:version"
        self.cache_ttl = 3600  # 1 hour TTL
    
    async def sync_taxonomy(self, force: bool = False) -> Tuple[bool, ValidationResult]:
        """
        Sync taxonomy data from Google Sheets with validation
        
        Args:
            force: If True, sync even if data hasn't changed
            
        Returns:
            Tuple of (success, validation_result)
        """
        log_with_context(
            logger, "info", "Starting taxonomy sync",
            sheet_id=self.sheet_id,
            tab_name=self.tab_name,
            force=force
        )
        
        try:
            # Fetch raw data from Google Sheets
            raw_data = await self._fetch_sheet_data()
            
            # Filter and process data
            processed_data = self._process_raw_data(raw_data)
            
            # Validate the processed data
            validation_result = await self.validator.validate_taxonomy(processed_data)
            
            if validation_result.is_valid:
                # Create new version
                version = self._generate_version()
                taxonomy_data = TaxonomyData(
                    version=version,
                    data=processed_data,
                    created_at=datetime.now(timezone.utc),
                    source_info={
                        "sheet_id": self.sheet_id,
                        "tab_name": self.tab_name,
                        "row_count": len(processed_data)
                    }
                )
                
                # Update current data and cache
                self._current_version = version
                self._current_data = taxonomy_data
                await self._update_cache(taxonomy_data, version)
                
                log_with_context(
                    logger, "info", "Taxonomy sync completed successfully",
                    version=version.version_string,
                    row_count=len(processed_data),
                    validation_errors=0
                )
                
                return True, validation_result
            else:
                log_with_context(
                    logger, "warning", "Taxonomy validation failed, keeping previous version",
                    validation_errors=len(validation_result.errors),
                    current_version=self._current_version.version_string if self._current_version else None
                )
                
                return False, validation_result
                
        except GoogleSheetsError as e:
            log_with_context(
                logger, "error", "Failed to fetch data from Google Sheets",
                error=str(e),
                sheet_id=self.sheet_id
            )
            return False, ValidationResult(
                is_valid=False,
                errors=[f"Sheet fetch error: {e}"],
                warnings=[]
            )
            
        except Exception as e:
            log_with_context(
                logger, "error", "Unexpected error during taxonomy sync",
                error=str(e),
                error_type=type(e).__name__
            )
            return False, ValidationResult(
                is_valid=False,
                errors=[f"Sync error: {e}"],
                warnings=[]
            )
    
    async def _fetch_sheet_data(self) -> List[Dict[str, Any]]:
        """Fetch raw data from Google Sheets"""
        return await sheets_client.fetch_sheet_data(self.sheet_id, self.tab_name)
    
    def _process_raw_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process raw sheet data by filtering and cleaning
        
        Args:
            raw_data: Raw data from Google Sheets
            
        Returns:
            Processed and filtered data
        """
        processed_data = []
        
        for row_idx, row in enumerate(raw_data):
            try:
                # Filter: Only rows where Classification Type == Event Type
                classification_type = row.get("Classification Type", "").strip()
                if classification_type != "Event Type":
                    continue
                
                # Extract hierarchy tiers
                tier_1 = row.get("Classification tier 1", "").strip()
                tier_2 = row.get("tier 2", "").strip()
                tier_3 = row.get("tier 3", "").strip()
                tier_4 = row.get("tier 4", "").strip()
                
                # Skip rows without at least tier 1
                if not tier_1:
                    continue
                
                processed_row = {
                    "tier_1": tier_1,
                    "tier_2": tier_2,
                    "tier_3": tier_3,
                    "tier_4": tier_4,
                    "source_row": row_idx + 1  # 1-based for human reference
                }
                
                processed_data.append(processed_row)
                
            except Exception as e:
                log_with_context(
                    logger, "warning", "Failed to process row",
                    row_index=row_idx,
                    error=str(e)
                )
                continue
        
        log_with_context(
            logger, "info", "Processed sheet data",
            total_rows=len(raw_data),
            filtered_rows=len(processed_data)
        )
        
        return processed_data
    
    def _generate_version(self) -> TaxonomyVersion:
        """Generate a new taxonomy version with timestamp format"""
        now = datetime.now(timezone.utc)
        version_string = f"tax_v{now.strftime('%Y%m%d_%H%M%S')}"
        
        return TaxonomyVersion(
            version_string=version_string,
            created_at=now
        )
    
    def get_current_version(self) -> Optional[TaxonomyVersion]:
        """Get the current taxonomy version"""
        return self._current_version
    
    def get_current_data(self) -> Optional[TaxonomyData]:
        """Get the current taxonomy data"""
        return self._current_data
    
    def get_taxonomy_preview(self) -> Dict[str, Any]:
        """
        Get a preview of the current taxonomy structure for debugging
        
        Returns:
            Dictionary with taxonomy structure and metadata
        """
        if not self._current_data:
            return {
                "version": None,
                "status": "no_data",
                "structure": {},
                "metadata": {}
            }
        
        # Build hierarchical structure for preview
        structure = {}
        for row in self._current_data.data:
            tier_1 = row["tier_1"]
            tier_2 = row["tier_2"]
            tier_3 = row["tier_3"]
            tier_4 = row["tier_4"]
            
            if tier_1 not in structure:
                structure[tier_1] = {}
            
            if tier_2:
                if tier_2 not in structure[tier_1]:
                    structure[tier_1][tier_2] = {}
                
                if tier_3:
                    if tier_3 not in structure[tier_1][tier_2]:
                        structure[tier_1][tier_2][tier_3] = []
                    
                    if tier_4:
                        structure[tier_1][tier_2][tier_3].append(tier_4)
        
        return {
            "version": self._current_version.version_string,
            "status": "active",
            "structure": structure,
            "metadata": {
                "created_at": self._current_data.created_at.isoformat(),
                "source_info": self._current_data.source_info,
                "total_paths": len(self._current_data.data)
            }
        }
    
    async def _update_cache(self, taxonomy_data: TaxonomyData, version: TaxonomyVersion):
        """Update cache with new taxonomy data"""
        try:
            # Cache the taxonomy data
            data_dict = {
                "version": {
                    "version_string": version.version_string,
                    "created_at": version.created_at.isoformat()
                },
                "data": taxonomy_data.data,
                "created_at": taxonomy_data.created_at.isoformat(),
                "source_info": taxonomy_data.source_info
            }
            
            await cache_manager.set(self.cache_key_current, data_dict, self.cache_ttl)
            
            # Cache just the version info
            version_dict = {
                "version_string": version.version_string,
                "created_at": version.created_at.isoformat()
            }
            await cache_manager.set(self.cache_key_version, version_dict, self.cache_ttl)
            
            log_with_context(
                logger, "info", "Taxonomy data cached successfully",
                version=version.version_string
            )
            
        except Exception as e:
            log_with_context(
                logger, "warning", "Failed to cache taxonomy data",
                error=str(e),
                version=version.version_string
            )
    
    async def _load_from_cache(self) -> bool:
        """Load taxonomy data from cache if available"""
        try:
            cached_data = await cache_manager.get(self.cache_key_current)
            if cached_data:
                # Reconstruct objects from cached data
                version_data = cached_data["version"]
                self._current_version = TaxonomyVersion(
                    version_string=version_data["version_string"],
                    created_at=datetime.fromisoformat(version_data["created_at"])
                )
                
                self._current_data = TaxonomyData(
                    version=self._current_version,
                    data=cached_data["data"],
                    created_at=datetime.fromisoformat(cached_data["created_at"]),
                    source_info=cached_data["source_info"]
                )
                
                log_with_context(
                    logger, "info", "Taxonomy data loaded from cache",
                    version=self._current_version.version_string
                )
                return True
                
        except Exception as e:
            log_with_context(
                logger, "warning", "Failed to load taxonomy data from cache",
                error=str(e)
            )
        
        return False
    
    async def invalidate_cache(self):
        """Invalidate all taxonomy cache entries"""
        await cache_manager.clear_pattern("taxonomy:*")
        log_with_context(logger, "info", "Taxonomy cache invalidated")


# Global instance
taxonomy_service = TaxonomyService()