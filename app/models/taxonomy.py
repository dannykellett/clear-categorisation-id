from typing import List, Dict, Any, Optional
from datetime import datetime
from pydantic import BaseModel, Field


class TaxonomyVersion(BaseModel):
    """Represents a taxonomy version with timestamp"""
    version_string: str = Field(..., description="Version string in format tax_vYYYYMMDD_HHMMSS")
    created_at: datetime = Field(..., description="When this version was created")


class TaxonomyData(BaseModel):
    """Complete taxonomy data with version and metadata"""
    version: TaxonomyVersion = Field(..., description="Version information")
    data: List[Dict[str, Any]] = Field(..., description="Processed taxonomy rows")
    created_at: datetime = Field(..., description="When this data was created")
    source_info: Dict[str, Any] = Field(..., description="Information about data source")


class ValidationResult(BaseModel):
    """Result of taxonomy validation"""
    is_valid: bool = Field(..., description="Whether validation passed")
    errors: List[str] = Field(default_factory=list, description="List of validation errors")
    warnings: List[str] = Field(default_factory=list, description="List of validation warnings")


class TaxonomyVersionResponse(BaseModel):
    """API response for taxonomy version endpoint"""
    version: Optional[str] = Field(None, description="Current taxonomy version")
    created_at: Optional[datetime] = Field(None, description="When current version was created")
    status: str = Field(..., description="Status: active, no_data, error")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")


class TaxonomyPreviewResponse(BaseModel):
    """API response for taxonomy preview endpoint"""
    version: Optional[str] = Field(None, description="Current taxonomy version")
    status: str = Field(..., description="Status: active, no_data, error")
    structure: Dict[str, Any] = Field(default_factory=dict, description="Hierarchical taxonomy structure")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")


class SyncTriggerResponse(BaseModel):
    """API response for manual sync trigger"""
    success: bool = Field(..., description="Whether sync was successful")
    version: Optional[str] = Field(None, description="New version if sync succeeded")
    validation_result: ValidationResult = Field(..., description="Validation results")
    message: str = Field(..., description="Human-readable result message")