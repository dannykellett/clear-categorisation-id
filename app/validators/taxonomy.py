from typing import List, Dict, Any, Set, Tuple
from app.models.taxonomy import ValidationResult
from app.core.logging import get_logger, log_with_context

logger = get_logger(__name__)


class ValidationError(Exception):
    """Exception raised for taxonomy validation errors"""
    pass


class TaxonomyValidator:
    """Validates taxonomy data structure and hierarchy rules"""
    
    def __init__(self):
        self.required_fields = ["tier_1", "tier_2", "tier_3", "tier_4"]
    
    async def validate_taxonomy(self, data: List[Dict[str, Any]]) -> ValidationResult:
        """
        Validate complete taxonomy data structure
        
        Args:
            data: List of taxonomy rows to validate
            
        Returns:
            ValidationResult with validation status and details
        """
        errors = []
        warnings = []
        
        log_with_context(
            logger, "info", "Starting taxonomy validation",
            row_count=len(data)
        )
        
        try:
            # Schema validation
            schema_errors = self._validate_schema(data)
            errors.extend(schema_errors)
            
            # Hierarchy validation (only if schema is valid)
            if not schema_errors:
                hierarchy_errors, hierarchy_warnings = self._validate_hierarchy(data)
                errors.extend(hierarchy_errors)
                warnings.extend(hierarchy_warnings)
            
            # Structure validation
            structure_errors = self._validate_structure(data)
            errors.extend(structure_errors)
            
            is_valid = len(errors) == 0
            
            log_with_context(
                logger, "info", "Taxonomy validation completed",
                is_valid=is_valid,
                error_count=len(errors),
                warning_count=len(warnings)
            )
            
            return ValidationResult(
                is_valid=is_valid,
                errors=errors,
                warnings=warnings
            )
            
        except Exception as e:
            log_with_context(
                logger, "error", "Unexpected error during validation",
                error=str(e),
                error_type=type(e).__name__
            )
            return ValidationResult(
                is_valid=False,
                errors=[f"Validation system error: {e}"],
                warnings=warnings
            )
    
    def _validate_schema(self, data: List[Dict[str, Any]]) -> List[str]:
        """
        Validate that required fields are present
        
        Args:
            data: List of taxonomy rows
            
        Returns:
            List of schema validation errors
        """
        errors = []
        
        if not data:
            errors.append("No taxonomy data provided")
            return errors
        
        # Check that all rows have required fields
        for idx, row in enumerate(data):
            missing_fields = []
            for field in self.required_fields:
                if field not in row:
                    missing_fields.append(field)
            
            if missing_fields:
                errors.append(
                    f"Row {row.get('source_row', idx + 1)}: Missing required fields: {', '.join(missing_fields)}"
                )
        
        return errors
    
    def _validate_hierarchy(self, data: List[Dict[str, Any]]) -> Tuple[List[str], List[str]]:
        """
        Validate hierarchy rules: no cycles, no orphans, tiers contiguous
        
        Args:
            data: List of taxonomy rows
            
        Returns:
            Tuple of (errors, warnings)
        """
        errors = []
        warnings = []
        
        # Build hierarchy mappings for validation
        tier_1_set = set()
        tier_2_to_1 = {}
        tier_3_to_2 = {}
        tier_4_to_3 = {}
        
        # Collect all tier relationships
        for row in data:
            tier_1 = row["tier_1"]
            tier_2 = row["tier_2"]
            tier_3 = row["tier_3"]
            tier_4 = row["tier_4"]
            
            if tier_1:
                tier_1_set.add(tier_1)
            
            if tier_2:
                if tier_2 in tier_2_to_1 and tier_2_to_1[tier_2] != tier_1:
                    errors.append(f"Tier 2 '{tier_2}' has multiple parents: '{tier_1}' and '{tier_2_to_1[tier_2]}'")
                tier_2_to_1[tier_2] = tier_1
            
            if tier_3:
                if tier_3 in tier_3_to_2 and tier_3_to_2[tier_3] != tier_2:
                    errors.append(f"Tier 3 '{tier_3}' has multiple parents: '{tier_2}' and '{tier_3_to_2[tier_3]}'")
                tier_3_to_2[tier_3] = tier_2
            
            if tier_4:
                if tier_4 in tier_4_to_3 and tier_4_to_3[tier_4] != tier_3:
                    errors.append(f"Tier 4 '{tier_4}' has multiple parents: '{tier_3}' and '{tier_4_to_3[tier_4]}'")
                tier_4_to_3[tier_4] = tier_3
        
        # Check for orphans (children without valid parents)
        for tier_2, parent_1 in tier_2_to_1.items():
            if parent_1 and parent_1 not in tier_1_set:
                errors.append(f"Tier 2 '{tier_2}' has orphan parent '{parent_1}' (not found in tier 1)")
        
        for tier_3, parent_2 in tier_3_to_2.items():
            if parent_2 and parent_2 not in tier_2_to_1:
                errors.append(f"Tier 3 '{tier_3}' has orphan parent '{parent_2}' (not found in tier 2)")
        
        for tier_4, parent_3 in tier_4_to_3.items():
            if parent_3 and parent_3 not in tier_3_to_2:
                errors.append(f"Tier 4 '{tier_4}' has orphan parent '{parent_3}' (not found in tier 3)")
        
        # Check for contiguous tiers (no gaps in hierarchy)
        for row in data:
            tier_1 = row["tier_1"]
            tier_2 = row["tier_2"]
            tier_3 = row["tier_3"]
            tier_4 = row["tier_4"]
            source_row = row.get("source_row", "unknown")
            
            # If tier 3 exists, tier 2 must exist
            if tier_3 and not tier_2:
                errors.append(f"Row {source_row}: Tier 3 '{tier_3}' exists but tier 2 is empty (non-contiguous)")
            
            # If tier 4 exists, tier 3 must exist
            if tier_4 and not tier_3:
                errors.append(f"Row {source_row}: Tier 4 '{tier_4}' exists but tier 3 is empty (non-contiguous)")
            
            # If tier 4 exists, tier 2 must exist
            if tier_4 and not tier_2:
                errors.append(f"Row {source_row}: Tier 4 '{tier_4}' exists but tier 2 is empty (non-contiguous)")
        
        # Check for potential cycles (simplified check)
        all_values = set()
        for row in data:
            for tier in ["tier_1", "tier_2", "tier_3", "tier_4"]:
                value = row[tier]
                if value and value in all_values:
                    # This is a simplified check - same value appearing in multiple tiers could indicate issues
                    warnings.append(f"Value '{value}' appears multiple times across different tiers")
                if value:
                    all_values.add(value)
        
        return errors, warnings
    
    def _validate_structure(self, data: List[Dict[str, Any]]) -> List[str]:
        """
        Validate data structure and consistency
        
        Args:
            data: List of taxonomy rows
            
        Returns:
            List of structure validation errors
        """
        errors = []
        
        # Check for completely empty rows
        empty_rows = []
        for idx, row in enumerate(data):
            if not any(row.get(tier, "").strip() for tier in self.required_fields):
                empty_rows.append(row.get("source_row", idx + 1))
        
        if empty_rows:
            errors.append(f"Found {len(empty_rows)} completely empty rows: {empty_rows}")
        
        # Check for duplicate complete paths
        path_counts = {}
        for row in data:
            path = tuple(row.get(tier, "") for tier in self.required_fields)
            if path in path_counts:
                path_counts[path] += 1
            else:
                path_counts[path] = 1
        
        duplicates = [(path, count) for path, count in path_counts.items() if count > 1]
        if duplicates:
            for path, count in duplicates:
                path_str = " -> ".join(p for p in path if p)
                errors.append(f"Duplicate path found {count} times: {path_str}")
        
        return errors