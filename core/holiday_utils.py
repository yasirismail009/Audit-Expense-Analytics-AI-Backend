"""
Holiday Utilities Module

This module provides utilities for fetching holiday data from Google Calendar API
for different countries based on country codes and time ranges.
"""

import requests
import json
from datetime import datetime, date
from typing import List, Dict, Optional, Union
from urllib.parse import quote
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)

# Google Calendar API configuration
GOOGLE_CALENDAR_API_BASE_URL = "https://www.googleapis.com/calendar/v3/calendars"
API_KEY = "AIzaSyA_4lo9hZiZep-aI46pQEJ9VsmFonBg_f8"

# Country code to Google Calendar ID mapping
COUNTRY_CALENDAR_MAPPING = {
    'af': 'en.afghan#holiday@group.v.calendar.google.com',
    'al': 'en.albanian#holiday@group.v.calendar.google.com',
    'dz': 'en.algerian#holiday@group.v.calendar.google.com',
    'as': 'en.american_samoa#holiday@group.v.calendar.google.com',
    'ad': 'en.andorran#holiday@group.v.calendar.google.com',
    'ao': 'en.angolan#holiday@group.v.calendar.google.com',
    'ai': 'en.anguilla#holiday@group.v.calendar.google.com',
    'ag': 'en.antigua_barbuda#holiday@group.v.calendar.google.com',
    'ar': 'en.argentine#holiday@group.v.calendar.google.com',
    'am': 'en.armenian#holiday@group.v.calendar.google.com',
    'aw': 'en.aruban#holiday@group.v.calendar.google.com',
    'australian': 'en.australian#holiday@group.v.calendar.google.com',
    'austrian': 'en.austrian#holiday@group.v.calendar.google.com',
    'az': 'en.azerbaijani#holiday@group.v.calendar.google.com',
    'bs': 'en.bahamian#holiday@group.v.calendar.google.com',
    'bh': 'en.bahraini#holiday@group.v.calendar.google.com',
    'bd': 'en.bangladeshi#holiday@group.v.calendar.google.com',
    'bb': 'en.barbadian#holiday@group.v.calendar.google.com',
    'by': 'en.belarusian#holiday@group.v.calendar.google.com',
    'be': 'en.belgian#holiday@group.v.calendar.google.com',
    'bz': 'en.belizean#holiday@group.v.calendar.google.com',
    'bj': 'en.beninese#holiday@group.v.calendar.google.com',
    'bm': 'en.bermudan#holiday@group.v.calendar.google.com',
    'bt': 'en.bhutanese#holiday@group.v.calendar.google.com',
    'bo': 'en.bolivian#holiday@group.v.calendar.google.com',
    'ba': 'en.bosnian#holiday@group.v.calendar.google.com',
    'bw': 'en.botswanan#holiday@group.v.calendar.google.com',
    'brazilian': 'en.brazilian#holiday@group.v.calendar.google.com',
    'vg': 'en.british_virgin_islands#holiday@group.v.calendar.google.com',
    'bn': 'en.bruneian#holiday@group.v.calendar.google.com',
    'bulgarian': 'en.bulgarian#holiday@group.v.calendar.google.com',
    'bf': 'en.burkina_faso#holiday@group.v.calendar.google.com',
    'bi': 'en.burundian#holiday@group.v.calendar.google.com',
    'kh': 'en.cambodian#holiday@group.v.calendar.google.com',
    'cm': 'en.cameroonian#holiday@group.v.calendar.google.com',
    'canadian': 'en.canadian#holiday@group.v.calendar.google.com',
    'cv': 'en.cape_verdean#holiday@group.v.calendar.google.com',
    'ky': 'en.cayman_islands#holiday@group.v.calendar.google.com',
    'cf': 'en.central_african_republic#holiday@group.v.calendar.google.com',
    'td': 'en.chadian#holiday@group.v.calendar.google.com',
    'cl': 'en.chilean#holiday@group.v.calendar.google.com',
    'china': 'en.chinese#holiday@group.v.calendar.google.com',
    'co': 'en.colombian#holiday@group.v.calendar.google.com',
    'km': 'en.comorian#holiday@group.v.calendar.google.com',
    'cg': 'en.congo_brazzaville#holiday@group.v.calendar.google.com',
    'cd': 'en.congo_kinshasa#holiday@group.v.calendar.google.com',
    'ck': 'en.cook_islands#holiday@group.v.calendar.google.com',
    'cr': 'en.costa_rican#holiday@group.v.calendar.google.com',
    'ci': 'en.cote_divoire#holiday@group.v.calendar.google.com',
    'croatian': 'en.croatian#holiday@group.v.calendar.google.com',
    'cu': 'en.cuban#holiday@group.v.calendar.google.com',
    'cw': 'en.curacao#holiday@group.v.calendar.google.com',
    'cy': 'en.cypriot#holiday@group.v.calendar.google.com',
    'czech': 'en.czech#holiday@group.v.calendar.google.com',
    'danish': 'en.danish#holiday@group.v.calendar.google.com',
    'dj': 'en.djiboutian#holiday@group.v.calendar.google.com',
    'dm': 'en.dominican_commonwealth#holiday@group.v.calendar.google.com',
    'do': 'en.dominican_republic#holiday@group.v.calendar.google.com',
    'ec': 'en.ecuadorian#holiday@group.v.calendar.google.com',
    'eg': 'en.egyptian#holiday@group.v.calendar.google.com',
    'sv': 'en.el_salvador#holiday@group.v.calendar.google.com',
    'gq': 'en.equatorial_guinea#holiday@group.v.calendar.google.com',
    'er': 'en.eritrean#holiday@group.v.calendar.google.com',
    'ee': 'en.estonian#holiday@group.v.calendar.google.com',
    'sz': 'en.eswatini#holiday@group.v.calendar.google.com',
    'et': 'en.ethiopian#holiday@group.v.calendar.google.com',
    'fk': 'en.falkland_islands#holiday@group.v.calendar.google.com',
    'fo': 'en.faroe_islands#holiday@group.v.calendar.google.com',
    'fj': 'en.fijian#holiday@group.v.calendar.google.com',
    'finnish': 'en.finnish#holiday@group.v.calendar.google.com',
    'french': 'en.french#holiday@group.v.calendar.google.com',
    'gf': 'en.french_guiana#holiday@group.v.calendar.google.com',
    'pf': 'en.french_polynesia#holiday@group.v.calendar.google.com',
    'ga': 'en.gabonese#holiday@group.v.calendar.google.com',
    'gm': 'en.gambian#holiday@group.v.calendar.google.com',
    'ge': 'en.georgian#holiday@group.v.calendar.google.com',
    'german': 'en.german#holiday@group.v.calendar.google.com',
    'gh': 'en.ghanaian#holiday@group.v.calendar.google.com',
    'gi': 'en.gibraltar#holiday@group.v.calendar.google.com',
    'greek': 'en.greek#holiday@group.v.calendar.google.com',
    'gl': 'en.greenlandic#holiday@group.v.calendar.google.com',
    'gd': 'en.grenadian#holiday@group.v.calendar.google.com',
    'gp': 'en.guadeloupe#holiday@group.v.calendar.google.com',
    'gu': 'en.guamanian#holiday@group.v.calendar.google.com',
    'gt': 'en.guatemalan#holiday@group.v.calendar.google.com',
    'gg': 'en.guernsey#holiday@group.v.calendar.google.com',
    'gn': 'en.guinean#holiday@group.v.calendar.google.com',
    'gw': 'en.guinea_bissau#holiday@group.v.calendar.google.com',
    'gy': 'en.guyanese#holiday@group.v.calendar.google.com',
    'ht': 'en.haitian#holiday@group.v.calendar.google.com',
    'hn': 'en.honduran#holiday@group.v.calendar.google.com',
    'hong_kong': 'en.hong_kong#holiday@group.v.calendar.google.com',
    'hungarian': 'en.hungarian#holiday@group.v.calendar.google.com',
    'is': 'en.icelandic#holiday@group.v.calendar.google.com',
    'indian': 'en.indian#holiday@group.v.calendar.google.com',
    'indonesian': 'en.indonesian#holiday@group.v.calendar.google.com',
    'ir': 'en.iranian#holiday@group.v.calendar.google.com',
    'iq': 'en.iraqi#holiday@group.v.calendar.google.com',
    'irish': 'en.irish#holiday@group.v.calendar.google.com',
    'im': 'en.isle_of_man#holiday@group.v.calendar.google.com',
    'jewish': 'en.jewish#holiday@group.v.calendar.google.com',
    'italian': 'en.italian#holiday@group.v.calendar.google.com',
    'jm': 'en.jamaican#holiday@group.v.calendar.google.com',
    'japanese': 'en.japanese#holiday@group.v.calendar.google.com',
    'je': 'en.jersey#holiday@group.v.calendar.google.com',
    'jo': 'en.jordanian#holiday@group.v.calendar.google.com',
    'kz': 'en.kazakhstani#holiday@group.v.calendar.google.com',
    'ke': 'en.kenyan#holiday@group.v.calendar.google.com',
    'ki': 'en.kiribati#holiday@group.v.calendar.google.com',
    'xk': 'en.kosovo#holiday@group.v.calendar.google.com',
    'kw': 'en.kuwaiti#holiday@group.v.calendar.google.com',
    'kg': 'en.kyrgyzstani#holiday@group.v.calendar.google.com',
    'la': 'en.laotian#holiday@group.v.calendar.google.com',
    'latvian': 'en.latvian#holiday@group.v.calendar.google.com',
    'lb': 'en.lebanese#holiday@group.v.calendar.google.com',
    'ls': 'en.lesotho#holiday@group.v.calendar.google.com',
    'lr': 'en.liberian#holiday@group.v.calendar.google.com',
    'ly': 'en.libyan#holiday@group.v.calendar.google.com',
    'li': 'en.liechtenstein#holiday@group.v.calendar.google.com',
    'lithuanian': 'en.lithuanian#holiday@group.v.calendar.google.com',
    'lu': 'en.luxembourgish#holiday@group.v.calendar.google.com',
    'mo': 'en.macau#holiday@group.v.calendar.google.com',
    'mg': 'en.malagasy#holiday@group.v.calendar.google.com',
    'mw': 'en.malawian#holiday@group.v.calendar.google.com',
    'malaysia': 'en.malaysia#holiday@group.v.calendar.google.com',
    'mv': 'en.maldivian#holiday@group.v.calendar.google.com',
    'ml': 'en.malian#holiday@group.v.calendar.google.com',
    'mt': 'en.maltese#holiday@group.v.calendar.google.com',
    'mh': 'en.marshall_islands#holiday@group.v.calendar.google.com',
    'mq': 'en.martinique#holiday@group.v.calendar.google.com',
    'mr': 'en.mauritanian#holiday@group.v.calendar.google.com',
    'mu': 'en.mauritian#holiday@group.v.calendar.google.com',
    'yt': 'en.mayotte#holiday@group.v.calendar.google.com',
    'mexican': 'en.mexican#holiday@group.v.calendar.google.com',
    'fm': 'en.micronesian#holiday@group.v.calendar.google.com',
    'md': 'en.moldovan#holiday@group.v.calendar.google.com',
    'mc': 'en.monacan#holiday@group.v.calendar.google.com',
    'mn': 'en.mongolian#holiday@group.v.calendar.google.com',
    'me': 'en.montenegrin#holiday@group.v.calendar.google.com',
    'ms': 'en.montserrat#holiday@group.v.calendar.google.com',
    'ma': 'en.moroccan#holiday@group.v.calendar.google.com',
    'mz': 'en.mozambican#holiday@group.v.calendar.google.com',
    'mm': 'en.myanmar#holiday@group.v.calendar.google.com',
    'na': 'en.namibian#holiday@group.v.calendar.google.com',
    'nr': 'en.nauruan#holiday@group.v.calendar.google.com',
    'np': 'en.nepalese#holiday@group.v.calendar.google.com',
    'dutch': 'en.dutch#holiday@group.v.calendar.google.com',
    'nc': 'en.new_caledonia#holiday@group.v.calendar.google.com',
    'new_zealand': 'en.new_zealand#holiday@group.v.calendar.google.com',
    'ni': 'en.nicaraguan#holiday@group.v.calendar.google.com',
    'ne': 'en.nigerien#holiday@group.v.calendar.google.com',
    'ng': 'en.nigerian#holiday@group.v.calendar.google.com',
    'mp': 'en.northern_mariana_islands#holiday@group.v.calendar.google.com',
    'kp': 'en.north_korea#holiday@group.v.calendar.google.com',
    'mk': 'en.north_macedonia#holiday@group.v.calendar.google.com',
    'norwegian': 'en.norwegian#holiday@group.v.calendar.google.com',
    'om': 'en.omani#holiday@group.v.calendar.google.com',
    'pk': 'en.pakistani#holiday@group.v.calendar.google.com',
    'pw': 'en.palauan#holiday@group.v.calendar.google.com',
    'pa': 'en.panamanian#holiday@group.v.calendar.google.com',
    'pg': 'en.papua_new_guinea#holiday@group.v.calendar.google.com',
    'py': 'en.paraguayan#holiday@group.v.calendar.google.com',
    'pe': 'en.peruvian#holiday@group.v.calendar.google.com',
    'philippines': 'en.philippines#holiday@group.v.calendar.google.com',
    'polish': 'en.polish#holiday@group.v.calendar.google.com',
    'portuguese': 'en.portuguese#holiday@group.v.calendar.google.com',
    'pr': 'en.puerto_rico#holiday@group.v.calendar.google.com',
    'qa': 'en.qatari#holiday@group.v.calendar.google.com',
    're': 'en.reunion#holiday@group.v.calendar.google.com',
    'romanian': 'en.romanian#holiday@group.v.calendar.google.com',
    'russian': 'en.russian#holiday@group.v.calendar.google.com',
    'rw': 'en.rwandan#holiday@group.v.calendar.google.com',
    'ws': 'en.samoan#holiday@group.v.calendar.google.com',
    'sm': 'en.san_marino#holiday@group.v.calendar.google.com',
    'st': 'en.sao_tome_principe#holiday@group.v.calendar.google.com',
    'saudiarabian': 'en.saudiarabian#holiday@group.v.calendar.google.com',
    'sn': 'en.senegalese#holiday@group.v.calendar.google.com',
    'rs': 'en.serbian#holiday@group.v.calendar.google.com',
    'sc': 'en.seychellois#holiday@group.v.calendar.google.com',
    'sl': 'en.sierra_leonean#holiday@group.v.calendar.google.com',
    'singapore': 'en.singapore#holiday@group.v.calendar.google.com',
    'sx': 'en.sint_maarten#holiday@group.v.calendar.google.com',
    'slovak': 'en.slovak#holiday@group.v.calendar.google.com',
    'slovenian': 'en.slovenian#holiday@group.v.calendar.google.com',
    'sb': 'en.solomon_islands#holiday@group.v.calendar.google.com',
    'so': 'en.somali#holiday@group.v.calendar.google.com',
    'sa': 'en.south_african#holiday@group.v.calendar.google.com',
    'south_korea': 'en.south_korea#holiday@group.v.calendar.google.com',
    'ss': 'en.south_sudan#holiday@group.v.calendar.google.com',
    'spain': 'en.spain#holiday@group.v.calendar.google.com',
    'lk': 'en.sri_lankan#holiday@group.v.calendar.google.com',
    'bl': 'en.st_barthelemy#holiday@group.v.calendar.google.com',
    'sh': 'en.st_helena#holiday@group.v.calendar.google.com',
    'kn': 'en.st_kitts_nevis#holiday@group.v.calendar.google.com',
    'lc': 'en.st_lucia#holiday@group.v.calendar.google.com',
    'mf': 'en.st_martin#holiday@group.v.calendar.google.com',
    'pm': 'en.st_pierre_miquelon#holiday@group.v.calendar.google.com',
    'vc': 'en.st_vincent_grenadines#holiday@group.v.calendar.google.com',
    'sd': 'en.sudanese#holiday@group.v.calendar.google.com',
    'sr': 'en.surinamese#holiday@group.v.calendar.google.com',
    'swedish': 'en.swedish#holiday@group.v.calendar.google.com',
    'ch': 'en.swiss#holiday@group.v.calendar.google.com',
    'sy': 'en.syrian#holiday@group.v.calendar.google.com',
    'taiwan': 'en.taiwan#holiday@group.v.calendar.google.com',
    'tj': 'en.tajikistani#holiday@group.v.calendar.google.com',
    'tz': 'en.tanzanian#holiday@group.v.calendar.google.com',
    'th': 'en.thai#holiday@group.v.calendar.google.com',
    'tl': 'en.timor_leste#holiday@group.v.calendar.google.com',
    'tg': 'en.togolese#holiday@group.v.calendar.google.com',
    'to': 'en.tongan#holiday@group.v.calendar.google.com',
    'tt': 'en.trinidad_tobago#holiday@group.v.calendar.google.com',
    'tn': 'en.tunisian#holiday@group.v.calendar.google.com',
    'turkish': 'en.turkish#holiday@group.v.calendar.google.com',
    'tm': 'en.turkmenistani#holiday@group.v.calendar.google.com',
    'tc': 'en.turks_caicos_islands#holiday@group.v.calendar.google.com',
    'tv': 'en.tuvaluan#holiday@group.v.calendar.google.com',
    'vi': 'en.us_virgin_islands#holiday@group.v.calendar.google.com',
    'ug': 'en.ugandan#holiday@group.v.calendar.google.com',
    'ukrainian': 'en.ukrainian#holiday@group.v.calendar.google.com',
    'ae': 'en.united_arab_emirates#holiday@group.v.calendar.google.com',
    'uk': 'en.uk#holiday@group.v.calendar.google.com',
    'usa': 'en.usa#holiday@group.v.calendar.google.com',
    'uy': 'en.uruguayan#holiday@group.v.calendar.google.com',
    'uz': 'en.uzbekistani#holiday@group.v.calendar.google.com',
    'vu': 'en.vanuatuan#holiday@group.v.calendar.google.com',
    'va': 'en.vatican_city#holiday@group.v.calendar.google.com',
    've': 'en.venezuelan#holiday@group.v.calendar.google.com',
    'vietnamese': 'en.vietnamese#holiday@group.v.calendar.google.com',
    'wf': 'en.wallis_futuna#holiday@group.v.calendar.google.com',
    'ye': 'en.yemeni#holiday@group.v.calendar.google.com',
    'zm': 'en.zambian#holiday@group.v.calendar.google.com',
    'zw': 'en.zimbabwean#holiday@group.v.calendar.google.com',
}


class HolidayData:
    """Data class to represent holiday information."""
    
    def __init__(self, name: str, date: str, description: str = "", holiday_type: str = "Public holiday"):
        self.name = name
        self.date = date
        self.description = description
        self.holiday_type = holiday_type
    
    def __repr__(self):
        return f"HolidayData(name='{self.name}', date='{self.date}', type='{self.holiday_type}')"
    
    def to_dict(self) -> Dict[str, str]:
        """Convert to dictionary format."""
        return {
            'name': self.name,
            'date': self.date,
            'description': self.description,
            'type': self.holiday_type
        }


class HolidayAPIError(Exception):
    """Custom exception for holiday API errors."""
    pass


@lru_cache(maxsize=256)
def get_holiday_calendar_id(country_code: str) -> str:
    """
    Get the Google Calendar ID for a given country code.
    
    Args:
        country_code (str): The country code (e.g., 'usa', 'uk', 'saudiarabian')
        
    Returns:
        str: The Google Calendar ID for the country
        
    Raises:
        ValueError: If the country code is not supported
    """
    calendar_id = COUNTRY_CALENDAR_MAPPING.get(country_code.lower())
    if not calendar_id:
        supported_countries = list(COUNTRY_CALENDAR_MAPPING.keys())
        raise ValueError(
            f"Country code '{country_code}' is not supported. "
            f"Supported countries: {', '.join(sorted(supported_countries))}"
        )
    return calendar_id


def format_date_for_api(date_obj: Union[datetime, date, str]) -> str:
    """
    Format a date object for the Google Calendar API.
    
    Args:
        date_obj: datetime, date, or string date
        
    Returns:
        str: ISO formatted date string
    """
    if isinstance(date_obj, str):
        # Try to parse the string date
        try:
            if 'T' in date_obj:
                # Already in ISO format
                return date_obj
            else:
                # Parse as date string
                parsed_date = datetime.strptime(date_obj, '%Y-%m-%d')
                return parsed_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        except ValueError:
            raise ValueError(f"Invalid date string format: {date_obj}. Use YYYY-MM-DD format.")
    
    elif isinstance(date_obj, date):
        return date_obj.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    elif isinstance(date_obj, datetime):
        return date_obj.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    else:
        raise ValueError(f"Unsupported date type: {type(date_obj)}")


def get_holidays(
    country_code: str,
    start_date: Union[datetime, date, str],
    end_date: Union[datetime, date, str],
    include_observances: bool = True
) -> List[HolidayData]:
    """
    Fetch holiday data for a specific country and date range.
    
    Args:
        country_code (str): The country code (e.g., 'usa', 'uk', 'saudiarabian')
        start_date: Start date for the holiday search (datetime, date, or 'YYYY-MM-DD' string)
        end_date: End date for the holiday search (datetime, date, or 'YYYY-MM-DD' string)
        include_observances (bool): Whether to include observances (default: True)
        
    Returns:
        List[HolidayData]: List of holiday data objects
        
    Raises:
        HolidayAPIError: If there's an error fetching data from the API
        ValueError: If the country code is invalid or dates are malformed
    """
    try:
        # Get the calendar ID for the country
        calendar_id = get_holiday_calendar_id(country_code)
        
        # Format dates for API
        time_min = format_date_for_api(start_date)
        time_max = format_date_for_api(end_date)
        
        # URL encode the calendar ID
        encoded_calendar_id = quote(calendar_id, safe='')
        
        # Construct the API URL
        url = f"{GOOGLE_CALENDAR_API_BASE_URL}/{encoded_calendar_id}/events"
        
        # Prepare query parameters
        params = {
            'timeMin': time_min,
            'timeMax': time_max,
            'key': API_KEY,
            'singleEvents': 'true',
            'orderBy': 'startTime'
        }
        
        # Make the API request
        logger.info(f"Fetching holidays for {country_code} from {time_min} to {time_max}")
        response = requests.get(url, params=params, timeout=30)
        
        # Check for HTTP errors
        response.raise_for_status()
        
        # Parse the JSON response
        data = response.json()
        
        # Extract holiday events
        holidays = []
        for event in data.get('items', []):
            # Skip observances if not requested
            if not include_observances and 'Observance' in event.get('description', ''):
                continue
                
            # Extract event details
            summary = event.get('summary', 'Unknown Holiday')
            description = event.get('description', '')
            
            # Determine holiday type
            holiday_type = "Public holiday"
            if 'Observance' in description:
                holiday_type = "Observance"
            
            # Get the start date
            start_info = event.get('start', {})
            if 'date' in start_info:
                holiday_date = start_info['date']
            elif 'dateTime' in start_info:
                # Convert datetime to date
                holiday_date = start_info['dateTime'][:10]
            else:
                continue  # Skip events without valid dates
            
            # Create holiday data object
            holiday = HolidayData(
                name=summary,
                date=holiday_date,
                description=description,
                holiday_type=holiday_type
            )
            holidays.append(holiday)
        
        logger.info(f"Retrieved {len(holidays)} holidays for {country_code}")
        return holidays
        
    except requests.RequestException as e:
        error_msg = f"Failed to fetch holiday data: {str(e)}"
        logger.error(error_msg)
        raise HolidayAPIError(error_msg)
    
    except (ValueError, KeyError) as e:
        error_msg = f"Error processing holiday data: {str(e)}"
        logger.error(error_msg)
        raise HolidayAPIError(error_msg)
    
    except Exception as e:
        error_msg = f"Unexpected error fetching holidays: {str(e)}"
        logger.error(error_msg)
        raise HolidayAPIError(error_msg)


def get_holidays_for_year(country_code: str, year: int, include_observances: bool = True) -> List[HolidayData]:
    """
    Fetch all holidays for a specific country and year.
    
    Args:
        country_code (str): The country code (e.g., 'usa', 'uk', 'saudiarabian')
        year (int): The year to fetch holidays for
        include_observances (bool): Whether to include observances (default: True)
        
    Returns:
        List[HolidayData]: List of holiday data objects for the year
    """
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    return get_holidays(country_code, start_date, end_date, include_observances)


def get_holidays_for_month(country_code: str, year: int, month: int, include_observances: bool = True) -> List[HolidayData]:
    """
    Fetch all holidays for a specific country, year, and month.
    
    Args:
        country_code (str): The country code (e.g., 'usa', 'uk', 'saudiarabian')
        year (int): The year
        month (int): The month (1-12)
        include_observances (bool): Whether to include observances (default: True)
        
    Returns:
        List[HolidayData]: List of holiday data objects for the month
    """
    if not 1 <= month <= 12:
        raise ValueError("Month must be between 1 and 12")
    
    start_date = f"{year}-{month:02d}-01"
    
    # Calculate the last day of the month
    if month == 12:
        end_date = f"{year}-12-31"
    else:
        next_month = datetime(year, month + 1, 1)
        last_day = (next_month - datetime.timedelta(days=1)).day
        end_date = f"{year}-{month:02d}-{last_day}"
    
    return get_holidays(country_code, start_date, end_date, include_observances)


def get_supported_countries() -> List[str]:
    """
    Get a list of all supported country codes.
    
    Returns:
        List[str]: List of supported country codes
    """
    return sorted(COUNTRY_CALENDAR_MAPPING.keys())


def is_holiday(country_code: str, check_date: Union[datetime, date, str]) -> bool:
    """
    Check if a specific date is a holiday in the given country.
    
    Args:
        country_code (str): The country code
        check_date: The date to check (datetime, date, or 'YYYY-MM-DD' string)
        
    Returns:
        bool: True if the date is a holiday, False otherwise
    """
    # Format the check date
    if isinstance(check_date, str):
        date_str = check_date
    elif isinstance(check_date, (datetime, date)):
        date_str = check_date.strftime('%Y-%m-%d')
    else:
        raise ValueError(f"Unsupported date type: {type(check_date)}")
    
    # Get holidays for a small range around the date (API works better with ranges)
    try:
        # Use a 3-day range to ensure we capture the holiday
        from datetime import timedelta
        check_dt = datetime.strptime(date_str, '%Y-%m-%d')
        start_date = (check_dt - timedelta(days=1)).strftime('%Y-%m-%d')
        end_date = (check_dt + timedelta(days=1)).strftime('%Y-%m-%d')
        
        holidays = get_holidays(country_code, start_date, end_date)
        # Check if any holiday matches the exact date
        return any(holiday.date == date_str for holiday in holidays)
    except HolidayAPIError:
        # If there's an API error, return False
        return False


# Convenience functions for common use cases
def get_saudi_holidays(start_date: Union[datetime, date, str], end_date: Union[datetime, date, str]) -> List[HolidayData]:
    """Get holidays for Saudi Arabia."""
    return get_holidays('saudiarabian', start_date, end_date)


def get_us_holidays(start_date: Union[datetime, date, str], end_date: Union[datetime, date, str]) -> List[HolidayData]:
    """Get holidays for the United States."""
    return get_holidays('usa', start_date, end_date)


def get_uk_holidays(start_date: Union[datetime, date, str], end_date: Union[datetime, date, str]) -> List[HolidayData]:
    """Get holidays for the United Kingdom."""
    return get_holidays('uk', start_date, end_date) 