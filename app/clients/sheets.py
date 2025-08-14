import asyncio
from typing import List, Dict, Any, Optional
import httpx
from urllib.parse import quote
from app.core.logging import get_logger, log_with_context

logger = get_logger(__name__)


class GoogleSheetsClient:
    """Client for accessing Google Sheets with public read-only access"""
    
    def __init__(self):
        self.base_url = "https://docs.google.com/spreadsheets/d"
        self.timeout = 30.0
    
    async def fetch_sheet_data(
        self, 
        sheet_id: str, 
        tab_name: str = "event_type"
    ) -> List[Dict[str, Any]]:
        """
        Fetch data from a public Google Sheet as CSV and convert to list of dicts
        
        Args:
            sheet_id: The Google Sheet ID from the URL
            tab_name: The tab/sheet name to fetch (default: 'event_type')
            
        Returns:
            List of dictionaries representing rows with column headers as keys
        """
        url = f"{self.base_url}/{sheet_id}/gviz/tq?tqx=out:csv&sheet={quote(tab_name)}"
        
        log_with_context(
            logger, "info", "Fetching Google Sheets data",
            sheet_id=sheet_id,
            tab_name=tab_name,
            url=url
        )
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(url)
                response.raise_for_status()
                
                # Parse CSV content
                csv_content = response.text
                rows = self._parse_csv_content(csv_content)
                
                log_with_context(
                    logger, "info", "Successfully fetched sheet data",
                    sheet_id=sheet_id,
                    tab_name=tab_name,
                    rows_count=len(rows),
                    response_size=len(csv_content)
                )
                
                return rows
                
        except httpx.TimeoutException as e:
            log_with_context(
                logger, "error", "Timeout fetching Google Sheets data",
                sheet_id=sheet_id,
                tab_name=tab_name,
                timeout=self.timeout,
                error=str(e)
            )
            raise GoogleSheetsError(f"Timeout fetching sheet data: {e}")
            
        except httpx.HTTPStatusError as e:
            log_with_context(
                logger, "error", "HTTP error fetching Google Sheets data",
                sheet_id=sheet_id,
                tab_name=tab_name,
                status_code=e.response.status_code,
                error=str(e)
            )
            raise GoogleSheetsError(f"HTTP error {e.response.status_code}: {e}")
            
        except Exception as e:
            log_with_context(
                logger, "error", "Unexpected error fetching Google Sheets data",
                sheet_id=sheet_id,
                tab_name=tab_name,
                error=str(e),
                error_type=type(e).__name__
            )
            raise GoogleSheetsError(f"Failed to fetch sheet data: {e}")
    
    def _parse_csv_content(self, csv_content: str) -> List[Dict[str, Any]]:
        """
        Parse CSV content into list of dictionaries
        
        Args:
            csv_content: Raw CSV content as string
            
        Returns:
            List of dictionaries with first row as headers
        """
        lines = csv_content.strip().split('\n')
        if not lines:
            return []
        
        # Parse header row
        headers = self._parse_csv_row(lines[0])
        
        # Parse data rows
        rows = []
        for line_num, line in enumerate(lines[1:], start=2):
            if line.strip():  # Skip empty lines
                try:
                    values = self._parse_csv_row(line)
                    # Pad values to match headers length
                    while len(values) < len(headers):
                        values.append("")
                    
                    # Create dictionary from headers and values
                    row_dict = dict(zip(headers, values))
                    rows.append(row_dict)
                    
                except Exception as e:
                    log_with_context(
                        logger, "warning", "Failed to parse CSV row",
                        line_number=line_num,
                        line_content=line[:100],  # Truncate for logging
                        error=str(e)
                    )
                    continue
        
        return rows
    
    def _parse_csv_row(self, row: str) -> List[str]:
        """
        Simple CSV row parser that handles quoted fields
        
        Args:
            row: Single CSV row as string
            
        Returns:
            List of field values
        """
        fields = []
        current_field = ""
        in_quotes = False
        i = 0
        
        while i < len(row):
            char = row[i]
            
            if char == '"':
                if in_quotes and i + 1 < len(row) and row[i + 1] == '"':
                    # Escaped quote
                    current_field += '"'
                    i += 1  # Skip next quote
                else:
                    # Toggle quote state
                    in_quotes = not in_quotes
            elif char == ',' and not in_quotes:
                # End of field
                fields.append(current_field.strip())
                current_field = ""
            else:
                current_field += char
            
            i += 1
        
        # Add the last field
        fields.append(current_field.strip())
        
        return fields


class GoogleSheetsError(Exception):
    """Exception raised for Google Sheets API errors"""
    pass


# Global instance
sheets_client = GoogleSheetsClient()