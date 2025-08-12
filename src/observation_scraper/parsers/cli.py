from typing import Dict, Optional, Any, Tuple
import re
from datetime import datetime

from bs4 import BeautifulSoup

from observation_scraper.models.climate_data import CLIData

class CLIParser:
    """
    Parser for Climatological (CLI) reports from National Weather Service.
    Handles extracting and parsing the structured data from CLI reports.
    """

    def extract_report_content(self, html_content: str) -> str:
        """
        Extract the CLI report content from HTML.
        
        Args:
            html_content: The HTML content containing the CLI report.
            
        Returns:
            The extracted CLI report text.
            
        Raises:
            ValueError: If the CLI report content could not be found.
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        report_element = soup.find('pre', class_='glossaryProduct')
        
        if not report_element:
            raise ValueError("Could not find CLI report content in the HTML")
            
        return report_element.text

    def validate_report_type(self, report_content: str) -> bool:
        """
        Validate that the report contains yesterday's weather data, not today's.
        
        Args:
            report_content: The text content of the CLI report.
            
        Returns:
            True if the report contains yesterday's data, False otherwise.
        """
        # First, check if the report has "YESTERDAY" in the temperature section
        if "TEMPERATURE (F)\n YESTERDAY" in report_content:
            return True
            
        # Also check that it doesn't have "TODAY" in the temperature section
        if "TEMPERATURE (F)\n TODAY" in report_content:
            return False
            
        # Check for "VALID TODAY AS OF" which is in today's reports
        if "VALID TODAY AS OF" in report_content:
            return False
            
        # If neither clear indicator is found, do more detailed checks
        yesterday_pattern = r"TEMPERATURE \(F\).*?YESTERDAY"
        today_pattern = r"TEMPERATURE \(F\).*?TODAY"
        
        has_yesterday = bool(re.search(yesterday_pattern, report_content, re.DOTALL))
        has_today = bool(re.search(today_pattern, report_content, re.DOTALL))
        
        # If we found yesterday but not today, it's the report we want
        if has_yesterday and not has_today:
            return True
        # If we found today but not yesterday, it's not what we want
        elif has_today and not has_yesterday:
            return False
            
        # If we're still not sure, default to checking for "YESTERDAY" anywhere
        return "YESTERDAY" in report_content and not "VALID TODAY" in report_content

    def extract_report_date(self, report_content: str) -> Tuple[str, datetime]:
        """
        Extract the date for which the report is providing data.
        
        Args:
            report_content: The text content of the CLI report.
            
        Returns:
            A tuple containing the date string and a datetime object.
            
        Raises:
            ValueError: If the date cannot be extracted from the report.
        """
        date_pattern = r"\.\.\.THE .* CLIMATE SUMMARY FOR ([A-Z]+ \d+ \d+)\.\.\."
        date_match = re.search(date_pattern, report_content)
        
        if not date_match:
            raise ValueError("Could not extract date from the CLI report")
            
        date_str = date_match.group(1)
        date_obj = datetime.strptime(date_str, "%B %d %Y")
        
        return date_str, date_obj

    def parse_report(self, report_content: str, station_id: str) -> CLIData:
        """
        Parse the CLI report content into structured data.
        
        Args:
            report_content: The text content of the CLI report.
            station_id: The ID of the weather station (e.g., "KNYC").
            
        Returns:
            A CLIData object containing structured climate data.
            
        Raises:
            ValueError: If the report is not the expected type (doesn't contain yesterday's data).
            ValueError: If required temperature data cannot be extracted.
        """
        # Validate that this is a report with yesterday's data
        if not self.validate_report_type(report_content):
            raise ValueError("The CLI report does not contain yesterday's weather data.")
            
        # Extract report date
        report_date_str = None
        report_date_obj = None
        try:
            report_date_str, report_date_obj = self.extract_report_date(report_content)
        except ValueError:
            pass
            
        # Parse temperature data - this is required
        temp_data = self._parse_temperature(report_content, report_date_obj)
        if not temp_data or not all(key in temp_data for key in ['max', 'min', 'avg', 'max_time', 'min_time', 'max_datetime', 'min_datetime']):
            raise ValueError("Could not extract required temperature data from the report")
            
        # Create the CLIData object with required temperature fields
        cli_data = CLIData(
            report_date=report_date_str,
            report_datetime=report_date_obj,
            station_id=station_id,
            temperature_max=temp_data["max"],
            temperature_min=temp_data["min"],
            temperature_avg=temp_data["avg"],
            temperature_max_time=temp_data["max_time"],
            temperature_min_time=temp_data["min_time"],
            temperature_max_datetime=temp_data["max_datetime"],
            temperature_min_datetime=temp_data["min_datetime"]
        )
            
        # Parse precipitation data - optional
        precip_data = self._parse_precipitation(report_content)
        if precip_data and "yesterday" in precip_data:
            cli_data.precipitation_yesterday = precip_data["yesterday"]
            
        # Parse humidity data - optional
        humidity_data = self._parse_humidity(report_content)
        if humidity_data:
            if "highest" in humidity_data:
                cli_data.humidity_highest = humidity_data["highest"]
            if "lowest" in humidity_data:
                cli_data.humidity_lowest = humidity_data["lowest"]
            if "average" in humidity_data:
                cli_data.humidity_avg = humidity_data["average"]
            
        # Parse wind data - optional
        wind_data = self._parse_wind(report_content)
        if wind_data:
            if "highest_speed" in wind_data:
                cli_data.wind_highest_speed = wind_data["highest_speed"]
            if "highest_direction" in wind_data:
                cli_data.wind_highest_direction = wind_data["highest_direction"]
            if "average_speed" in wind_data:
                cli_data.wind_avg_speed = wind_data["average_speed"]
            
        return cli_data

    def _create_data_structure(self, report_content: str) -> Dict[str, Any]:
        """Create the base data structure and extract common data."""
        # Extract the report date
        try:
            date_str, date_obj = self.extract_report_date(report_content)
        except ValueError:
            date_str, date_obj = None, None
            
        return {
            "report_date": date_str,
            "report_datetime": date_obj,
            "temperature": {},
            "precipitation": {},
            "snowfall": {},
            "degree_days": {},
            "wind": {},
            "sky_cover": {},
            "humidity": {},
            "report_type": "yesterday"  # Explicitly mark this as a 'yesterday' report
        }

    def _parse_temperature(self, report_content: str, data: Dict[str, Any]) -> None:
        """Parse temperature data from the report and add it to the data dict."""
        # Extract basic temperature data with times
        temp_pattern = r"TEMPERATURE \(F\).*?YESTERDAY.*?MAXIMUM\s+(\d+)\s+(\d+)(?:\s+)([AP]M).*?MINIMUM\s+(\d+)\s+(\d+)(?:\s+)([AP]M).*?AVERAGE\s+(\d+)"
        temp_match = re.search(temp_pattern, report_content, re.DOTALL)
        
        if temp_match:
            # Extract the values and times
            max_temp = int(temp_match.group(1))
            max_time_hour = int(temp_match.group(2))
            max_time_ampm = temp_match.group(3)
            
            min_temp = int(temp_match.group(4))
            min_time_hour = int(temp_match.group(5))
            min_time_ampm = temp_match.group(6)
            
            avg_temp = int(temp_match.group(7))
            
            # Store the basic values
            data["temperature"]["max"] = max_temp
            data["temperature"]["min"] = min_temp
            data["temperature"]["avg"] = avg_temp
            
            # Store the times as formatted strings (e.g., "3:41 PM")
            max_hour = max_time_hour // 100
            max_minute = max_time_hour % 100
            data["temperature"]["max_time"] = f"{max_hour}:{max_minute:02d} {max_time_ampm}"
            
            min_hour = min_time_hour // 100
            min_minute = min_time_hour % 100
            data["temperature"]["min_time"] = f"{min_hour}:{min_minute:02d} {min_time_ampm}"
            
            # If report_datetime is available, create full timestamps
            if data.get("report_datetime"):
                report_date = data["report_datetime"]
                
                # For max temperature time
                max_hour_24 = max_hour
                if max_time_ampm == "PM" and max_hour < 12:
                    max_hour_24 += 12
                elif max_time_ampm == "AM" and max_hour == 12:
                    max_hour_24 = 0
                    
                # For min temperature time
                min_hour_24 = min_hour
                if min_time_ampm == "PM" and min_hour < 12:
                    min_hour_24 += 12
                elif min_time_ampm == "AM" and min_hour == 12:
                    min_hour_24 = 0
                
                # Create datetime objects for the exact times
                # Note: These will be on the same day as the report date, which is yesterday
                data["temperature"]["max_datetime"] = datetime(
                    report_date.year, report_date.month, report_date.day,
                    max_hour_24, max_minute
                )
                
                data["temperature"]["min_datetime"] = datetime(
                    report_date.year, report_date.month, report_date.day,
                    min_hour_24, min_minute
                )
        
    def _parse_temperature(self, report_content: str, report_date: Optional[datetime] = None) -> Dict:
        """
        Parse temperature data from the report.
        
        Args:
            report_content: The text content of the CLI report.
            report_date: Optional report date to use for creating datetime objects.
            
        Returns:
            Dictionary with temperature data or None if data couldn't be parsed.
        """
        # Extract temperature data with times
        temp_pattern = r"TEMPERATURE \(F\).*?YESTERDAY.*?MAXIMUM\s+(\d+)\s+(\d+)(?:\s+)([AP]M).*?MINIMUM\s+(\d+)\s+(\d+)(?:\s+)([AP]M).*?AVERAGE\s+(\d+)"
        temp_match = re.search(temp_pattern, report_content, re.DOTALL)
        
        if not temp_match:
            return None
            
        # Extract the values and times
        max_temp = int(temp_match.group(1))
        max_time_hour = int(temp_match.group(2))
        max_time_ampm = temp_match.group(3)
        
        min_temp = int(temp_match.group(4))
        min_time_hour = int(temp_match.group(5))
        min_time_ampm = temp_match.group(6)
        
        avg_temp = int(temp_match.group(7))
        
        # Format times as strings (e.g., "3:41 PM")
        max_hour = max_time_hour // 100
        max_minute = max_time_hour % 100
        max_time = f"{max_hour}:{max_minute:02d} {max_time_ampm}"
        
        min_hour = min_time_hour // 100
        min_minute = min_time_hour % 100
        min_time = f"{min_hour}:{min_minute:02d} {min_time_ampm}"
        
        # Create datetime objects
        max_datetime = None
        min_datetime = None
        if report_date:
            # For max temperature time
            max_hour_24 = max_hour
            if max_time_ampm == "PM" and max_hour < 12:
                max_hour_24 += 12
            elif max_time_ampm == "AM" and max_hour == 12:
                max_hour_24 = 0
                
            # For min temperature time
            min_hour_24 = min_hour
            if min_time_ampm == "PM" and min_hour < 12:
                min_hour_24 += 12
            elif min_time_ampm == "AM" and min_hour == 12:
                min_hour_24 = 0
            
            # Create datetime objects for the exact times
            max_datetime = datetime(
                report_date.year, report_date.month, report_date.day,
                max_hour_24, max_minute
            )
            
            min_datetime = datetime(
                report_date.year, report_date.month, report_date.day,
                min_hour_24, min_minute
            )
        else:
            # If we don't have a report date, use the current date
            # This is not ideal but ensures we have datetime objects
            now = datetime.now()
            
            # For max temperature time
            max_hour_24 = max_hour
            if max_time_ampm == "PM" and max_hour < 12:
                max_hour_24 += 12
            elif max_time_ampm == "AM" and max_hour == 12:
                max_hour_24 = 0
                
            # For min temperature time
            min_hour_24 = min_hour
            if min_time_ampm == "PM" and min_hour < 12:
                min_hour_24 += 12
            elif min_time_ampm == "AM" and min_hour == 12:
                min_hour_24 = 0
            
            # Create datetime objects for the exact times
            max_datetime = datetime(
                now.year, now.month, now.day,
                max_hour_24, max_minute
            )
            
            min_datetime = datetime(
                now.year, now.month, now.day,
                min_hour_24, min_minute
            )
            
        return {
            "max": max_temp,
            "min": min_temp,
            "avg": avg_temp,
            "max_time": max_time,
            "min_time": min_time,
            "max_datetime": max_datetime,
            "min_datetime": min_datetime
        }
        
    def _parse_precipitation(self, report_content: str) -> Optional[Dict]:
        """Parse precipitation data from the report."""
        precip_pattern = r"PRECIPITATION \(IN\).*?YESTERDAY\s+(\d+\.\d+)"
        precip_match = re.search(precip_pattern, report_content, re.DOTALL)
        
        if not precip_match:
            return None
            
        return {
            "yesterday": float(precip_match.group(1))
        }
        
    def _parse_humidity(self, report_content: str) -> Optional[Dict]:
        """Parse humidity data from the report."""
        humidity_pattern = r"RELATIVE HUMIDITY \(PERCENT\).*?HIGHEST\s+(\d+).*?LOWEST\s+(\d+).*?AVERAGE\s+(\d+)"
        humidity_match = re.search(humidity_pattern, report_content, re.DOTALL)
        
        if not humidity_match:
            return None
            
        return {
            "highest": int(humidity_match.group(1)),
            "lowest": int(humidity_match.group(2)),
            "average": int(humidity_match.group(3))
        }
        
    def _parse_wind(self, report_content: str) -> Optional[Dict]:
        """Parse wind data from the report."""
        # Extract highest wind speed and direction
        wind_pattern = r"WIND \(MPH\).*?HIGHEST WIND SPEED\s+(\d+).*?HIGHEST WIND DIRECTION\s+([A-Z]+).*?AVERAGE WIND SPEED\s+([\d\.]+)"
        wind_match = re.search(wind_pattern, report_content, re.DOTALL)
        
        if not wind_match:
            return None
            
        return {
            "highest_speed": int(wind_match.group(1)),
            "highest_direction": wind_match.group(2),
            "average_speed": float(wind_match.group(3))
        }
