import pytest
from app.validators.taxonomy import TaxonomyValidator


class TestTaxonomyValidator:
    """Test taxonomy validation logic"""
    
    @pytest.fixture
    def validator(self):
        return TaxonomyValidator()
    
    @pytest.fixture
    def valid_data(self):
        """Valid taxonomy data for testing"""
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
                "tier_2": "Software",
                "tier_3": "Web Development",
                "tier_4": "Frameworks",
                "source_row": 2
            },
            {
                "tier_1": "Technology",
                "tier_2": "Data Science",
                "tier_3": "Analysis",
                "tier_4": "",
                "source_row": 3
            },
            {
                "tier_1": "Business",
                "tier_2": "Marketing",
                "tier_3": "",
                "tier_4": "",
                "source_row": 4
            }
        ]
    
    @pytest.mark.asyncio
    async def test_validate_taxonomy_valid_data(self, validator, valid_data):
        """Test validation with valid taxonomy data"""
        result = await validator.validate_taxonomy(valid_data)
        
        assert result.is_valid is True
        assert len(result.errors) == 0
        assert len(result.warnings) >= 0  # May have warnings but should be valid
    
    @pytest.mark.asyncio
    async def test_validate_taxonomy_empty_data(self, validator):
        """Test validation with empty data"""
        result = await validator.validate_taxonomy([])
        
        assert result.is_valid is False
        assert "No taxonomy data provided" in result.errors
    
    @pytest.mark.asyncio
    async def test_validate_schema_missing_fields(self, validator):
        """Test schema validation with missing required fields"""
        invalid_data = [
            {
                "tier_1": "Technology",
                "tier_2": "Software",
                # Missing tier_3 and tier_4
                "source_row": 1
            }
        ]
        
        result = await validator.validate_taxonomy(invalid_data)
        
        assert result.is_valid is False
        assert any("Missing required fields" in error for error in result.errors)
        assert "tier_3" in str(result.errors)
        assert "tier_4" in str(result.errors)
    
    @pytest.mark.asyncio
    async def test_validate_hierarchy_orphan_tier2(self, validator):
        """Test hierarchy validation with orphan tier 2"""
        invalid_data = [
            {
                "tier_1": "Technology",
                "tier_2": "Software",
                "tier_3": "",
                "tier_4": "",
                "source_row": 1
            },
            {
                "tier_1": "Business",  # Different tier 1
                "tier_2": "Software",  # Same tier 2 name but different parent
                "tier_3": "",
                "tier_4": "",
                "source_row": 2
            }
        ]
        
        result = await validator.validate_taxonomy(invalid_data)
        
        assert result.is_valid is False
        assert any("has multiple parents" in error for error in result.errors)
    
    @pytest.mark.asyncio
    async def test_validate_hierarchy_orphan_tier3(self, validator):
        """Test hierarchy validation with orphan tier 3"""
        invalid_data = [
            {
                "tier_1": "Technology",
                "tier_2": "",
                "tier_3": "Web Development",  # Tier 3 without tier 2
                "tier_4": "",
                "source_row": 1
            }
        ]
        
        result = await validator.validate_taxonomy(invalid_data)
        
        assert result.is_valid is False
        # Should have non-contiguous error since tier 3 exists but tier 2 is empty
        assert any("non-contiguous" in error for error in result.errors)
    
    @pytest.mark.asyncio
    async def test_validate_hierarchy_non_contiguous_tiers(self, validator):
        """Test validation with non-contiguous tiers"""
        invalid_data = [
            {
                "tier_1": "Technology",
                "tier_2": "",  # Missing tier 2
                "tier_3": "Web Development",  # But tier 3 exists
                "tier_4": "",
                "source_row": 1
            },
            {
                "tier_1": "Business",
                "tier_2": "",  # Missing tier 2
                "tier_3": "",  # Missing tier 3
                "tier_4": "APIs",  # But tier 4 exists
                "source_row": 2
            }
        ]
        
        result = await validator.validate_taxonomy(invalid_data)
        
        assert result.is_valid is False
        assert any("non-contiguous" in error for error in result.errors)
        
        # Should have multiple contiguity errors
        contiguity_errors = [error for error in result.errors if "non-contiguous" in error]
        assert len(contiguity_errors) >= 2
    
    @pytest.mark.asyncio
    async def test_validate_structure_empty_rows(self, validator):
        """Test structure validation with completely empty rows"""
        invalid_data = [
            {
                "tier_1": "Technology",
                "tier_2": "Software",
                "tier_3": "Web Development",
                "tier_4": "",
                "source_row": 1
            },
            {
                "tier_1": "",  # Empty row
                "tier_2": "",
                "tier_3": "",
                "tier_4": "",
                "source_row": 2
            }
        ]
        
        result = await validator.validate_taxonomy(invalid_data)
        
        assert result.is_valid is False
        assert any("completely empty rows" in error for error in result.errors)
    
    @pytest.mark.asyncio
    async def test_validate_structure_duplicate_paths(self, validator):
        """Test structure validation with duplicate complete paths"""
        invalid_data = [
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
                "tier_4": "APIs",  # Exact duplicate
                "source_row": 2
            }
        ]
        
        result = await validator.validate_taxonomy(invalid_data)
        
        assert result.is_valid is False
        assert any("Duplicate path found" in error for error in result.errors)
    
    @pytest.mark.asyncio
    async def test_validate_taxonomy_complex_invalid(self, validator):
        """Test validation with multiple types of errors"""
        complex_invalid_data = [
            {
                "tier_1": "Technology",
                "tier_2": "Software",
                "tier_3": "Web Development",
                "tier_4": "APIs",
                "source_row": 1
            },
            {
                # Missing fields
                "tier_1": "Business",
                "source_row": 2
            },
            {
                # Non-contiguous
                "tier_1": "Marketing",
                "tier_2": "",
                "tier_3": "Digital",
                "tier_4": "",
                "source_row": 3
            },
            {
                # Duplicate of first
                "tier_1": "Technology",
                "tier_2": "Software",
                "tier_3": "Web Development",
                "tier_4": "APIs",
                "source_row": 4
            }
        ]
        
        result = await validator.validate_taxonomy(complex_invalid_data)
        
        assert result.is_valid is False
        assert len(result.errors) >= 2  # Should have multiple error types (missing fields + duplicates)
        
        # Check for different error types
        error_text = " ".join(result.errors)
        assert "Missing required fields" in error_text
        assert "Duplicate path found" in error_text
        # Note: non-contiguous errors are only detected in hierarchy validation 
        # which is skipped when schema validation fails
    
    def test_validate_schema_private_method(self, validator):
        """Test private schema validation method"""
        invalid_data = [
            {
                "tier_1": "Technology",
                # Missing tier_2, tier_3, tier_4
                "source_row": 1
            }
        ]
        
        errors = validator._validate_schema(invalid_data)
        
        assert len(errors) > 0
        assert any("Missing required fields" in error for error in errors)
    
    def test_validate_hierarchy_private_method(self, validator, valid_data):
        """Test private hierarchy validation method"""
        errors, warnings = validator._validate_hierarchy(valid_data)
        
        # Valid data should have no hierarchy errors
        assert len(errors) == 0
    
    def test_validate_structure_private_method(self, validator, valid_data):
        """Test private structure validation method"""
        errors = validator._validate_structure(valid_data)
        
        # Valid data should have no structure errors
        assert len(errors) == 0