import pytest
import httpx
from unittest.mock import AsyncMock, patch
from app.clients.sheets import GoogleSheetsClient, GoogleSheetsError


class TestGoogleSheetsClient:
    """Test Google Sheets client functionality"""
    
    @pytest.fixture
    def client(self):
        return GoogleSheetsClient()
    
    @pytest.fixture
    def mock_csv_response(self):
        """Sample CSV response from Google Sheets"""
        return '''Classification Type,Classification tier 1,tier 2,tier 3,tier 4
Event Type,Technology,Software,Web Development,APIs
Event Type,Technology,Software,Web Development,Frameworks
Event Type,Technology,Data Science,Analysis,Statistics
Event Type,Business,Marketing,Digital,Social Media
Other Type,Ignored,Category,Should,Be Filtered'''
    
    @pytest.mark.asyncio
    async def test_fetch_sheet_data_success(self, client, mock_csv_response):
        """Test successful sheet data fetching"""
        sheet_id = "test_sheet_id"
        tab_name = "test_tab"
        
        with patch('httpx.AsyncClient') as mock_client:
            # Mock successful HTTP response
            mock_response = AsyncMock()
            mock_response.text = mock_csv_response
            mock_response.raise_for_status.return_value = None
            
            mock_client.return_value.__aenter__.return_value.get.return_value = mock_response
            
            result = await client.fetch_sheet_data(sheet_id, tab_name)
            
            # Verify results
            assert len(result) == 5  # 5 data rows (excluding header)
            
            # Check first row
            first_row = result[0]
            assert first_row["Classification Type"] == "Event Type"
            assert first_row["Classification tier 1"] == "Technology"
            assert first_row["tier 2"] == "Software"
            assert first_row["tier 3"] == "Web Development"
            assert first_row["tier 4"] == "APIs"
            
            # Check last row
            last_row = result[4]
            assert last_row["Classification Type"] == "Other Type"
            assert last_row["Classification tier 1"] == "Ignored"
    
    @pytest.mark.asyncio
    async def test_fetch_sheet_data_http_error(self, client):
        """Test HTTP error handling"""
        sheet_id = "test_sheet_id"
        
        with patch('httpx.AsyncClient') as mock_client:
            # Mock HTTP error
            mock_response = AsyncMock()
            mock_response.status_code = 404
            http_error = httpx.HTTPStatusError("Not found", request=None, response=mock_response)
            mock_client.return_value.__aenter__.return_value.get.side_effect = http_error
            
            with pytest.raises(GoogleSheetsError) as exc_info:
                await client.fetch_sheet_data(sheet_id)
            
            assert "HTTP error 404" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_fetch_sheet_data_timeout(self, client):
        """Test timeout handling"""
        sheet_id = "test_sheet_id"
        
        with patch('httpx.AsyncClient') as mock_client:
            # Mock timeout
            timeout_error = httpx.TimeoutException("Request timeout")
            mock_client.return_value.__aenter__.return_value.get.side_effect = timeout_error
            
            with pytest.raises(GoogleSheetsError) as exc_info:
                await client.fetch_sheet_data(sheet_id)
            
            assert "Timeout fetching sheet data" in str(exc_info.value)
    
    def test_parse_csv_row_simple(self, client):
        """Test simple CSV row parsing"""
        row = "value1,value2,value3"
        result = client._parse_csv_row(row)
        assert result == ["value1", "value2", "value3"]
    
    def test_parse_csv_row_quoted(self, client):
        """Test CSV row parsing with quoted fields"""
        row = '"quoted value","another, with comma","normal"'
        result = client._parse_csv_row(row)
        assert result == ["quoted value", "another, with comma", "normal"]
    
    def test_parse_csv_row_escaped_quotes(self, client):
        """Test CSV row parsing with escaped quotes"""
        row = '"value with ""escaped"" quotes","normal value"'
        result = client._parse_csv_row(row)
        assert result == ['value with "escaped" quotes', "normal value"]
    
    def test_parse_csv_content_empty(self, client):
        """Test parsing empty CSV content"""
        result = client._parse_csv_content("")
        assert result == []
    
    def test_parse_csv_content_header_only(self, client):
        """Test parsing CSV with only header"""
        csv_content = "header1,header2,header3"
        result = client._parse_csv_content(csv_content)
        assert result == []
    
    def test_parse_csv_content_with_data(self, client):
        """Test parsing CSV with header and data"""
        csv_content = """header1,header2,header3
value1,value2,value3
"quoted","value with, comma",normal"""
        
        result = client._parse_csv_content(csv_content)
        
        assert len(result) == 2
        assert result[0] == {"header1": "value1", "header2": "value2", "header3": "value3"}
        assert result[1] == {"header1": "quoted", "header2": "value with, comma", "header3": "normal"}
    
    def test_parse_csv_content_mismatched_columns(self, client):
        """Test parsing CSV with mismatched column counts"""
        csv_content = """col1,col2,col3
value1,value2
value1,value2,value3,value4"""
        
        result = client._parse_csv_content(csv_content)
        
        assert len(result) == 2
        # First row should be padded
        assert result[0] == {"col1": "value1", "col2": "value2", "col3": ""}
        # Second row should be truncated to match headers
        assert result[1] == {"col1": "value1", "col2": "value2", "col3": "value3"}