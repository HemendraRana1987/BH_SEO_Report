import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urljoin, parse_qs, urlencode, urlunparse
from dataclasses import dataclass, field
from typing import List, Dict, Set, Union, Optional, Tuple
import time
import pandas as pd
import zipfile
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import smtplib
import shutil
import logging
from contextlib import contextmanager
import warnings
from datetime import datetime, timedelta
import random
import math
from queue import Queue
import threading
import json
import hashlib
import re
import socket
from functools import lru_cache

# Suppress urllib3 warnings
warnings.filterwarnings('ignore', category=Warning)

# --- Configuration and Setup ---

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Suppress urllib3 connection warnings
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)

# --- Data Classes ---

@dataclass
class EmailConfig:
    smtp_server: str = 'smtp.gmail.com'
    smtp_port: int = 587
    username: str = 'hemendra.rana@deptagency.com'
    password: str = 'azvp nyjw leel rtdt' 
    sender: str = 'hemendra.rana@deptagency.com'

@dataclass
class SiteConfig:
    name: str
    sitemaps: List[str]
    output_dir: str
    recipients: List[str]
    zip_filename: str
    use_slower_rate: bool = False
    extra_headers: Dict[str, str] = field(default_factory=dict)
    verify_ssl: bool = True

@dataclass
class LinkResult:
    url: str
    status_code: Union[int, str]
    text: str = ""
    next_tag_data: str = ""

@dataclass
class PageResult:
    url: str
    response_code: Union[int, str]
    robots_meta: str = ""
    broken_links: List[LinkResult] = field(default_factory=list)
    broken_images: List[LinkResult] = field(default_factory=list)

@dataclass
class SitemapStatus:
    """Track individual sitemap processing status"""
    url: str
    status: str  # 'SUCCESS', 'FAILED', 'EMPTY'
    status_code: Union[int, str] = ""
    urls_found: int = 0
    error_message: str = ""
    timestamp: str = ""
    scan_time: float = 0.0

@dataclass
class SitemapHistoricalData:
    """Store historical scan data for comparison"""
    sitemap_url: str
    scan_date: str
    total_pages: int
    pages_with_broken_links: int
    pages_with_broken_images: int
    total_broken_links: int
    total_broken_images: int
    noindex_nofollow_count: int

@dataclass
class URLCacheEntry:
    """Cache entry for URL status"""
    url: str
    status_code: Union[int, str]
    timestamp: float
    check_count: int = 1

class URLCache:
    """Cache to store URL status and avoid duplicate checks"""
    def __init__(self, max_size: int = 50000, ttl: int = 1800):  # 30 minutes TTL
        self.cache: Dict[str, URLCacheEntry] = {}
        self.max_size = max_size
        self.ttl = ttl  # Time to live in seconds
        self.lock = threading.Lock()
        self.hits = 0
        self.misses = 0
    
    def _get_key(self, url: str) -> str:
        """Generate cache key for URL"""
        try:
            parsed = urlparse(url)
            # Remove common tracking parameters
            query_params = parse_qs(parsed.query)
            filtered_params = {k: v for k, v in query_params.items() 
                             if k.lower() not in ['utm_source', 'utm_medium', 'utm_campaign', 
                                                'utm_term', 'utm_content', 'fbclid', 
                                                'gclid', 'msclkid', 'ga_cid', '_ga']}
            new_query = urlencode(filtered_params, doseq=True)
            normalized_url = urlunparse((
                parsed.scheme,
                parsed.netloc.lower(),  # Lowercase domain
                parsed.path.rstrip('/'),  # Remove trailing slash
                parsed.params,
                new_query,
                parsed.fragment
            ))
            return normalized_url
        except:
            return url.lower()
    
    def get(self, url: str) -> Optional[Union[int, str]]:
        """Get cached status for URL if valid"""
        with self.lock:
            key = self._get_key(url)
            if key in self.cache:
                entry = self.cache[key]
                # Check if cache entry is still valid
                if time.time() - entry.timestamp < self.ttl:
                    self.hits += 1
                    entry.check_count += 1
                    return entry.status_code
                else:
                    # Entry expired, remove it
                    del self.cache[key]
            self.misses += 1
            return None
    
    def set(self, url: str, status_code: Union[int, str]):
        """Cache URL status"""
        with self.lock:
            key = self._get_key(url)
            # Remove oldest entries if cache is full
            if len(self.cache) >= self.max_size:
                # Remove 10% of oldest entries
                to_remove = sorted(self.cache.items(), 
                                 key=lambda x: x[1].timestamp)[:self.max_size // 10]
                for k, _ in to_remove:
                    del self.cache[k]
            
            self.cache[key] = URLCacheEntry(
                url=url,
                status_code=status_code,
                timestamp=time.time()
            )
    
    def get_stats(self) -> Dict:
        """Get cache statistics"""
        with self.lock:
            total = self.hits + self.misses
            return {
                'size': len(self.cache),
                'hits': self.hits,
                'misses': self.misses,
                'hit_rate': self.hits / total if total > 0 else 0,
                'avg_checks_per_url': sum(e.check_count for e in self.cache.values()) / len(self.cache) if self.cache else 0
            }
    
    def clear(self):
        """Clear the cache"""
        with self.lock:
            self.cache.clear()
            self.hits = 0
            self.misses = 0

# --- Constants ---

BROKEN_STATUS_CODES = {404, 400, 403, 500, 502, 503, 504, "Timeout/Error", "Error", "SSL Error"}
SKIP_SCHEMES = ('javascript:', 'mailto:', 'tel:', '#', 'data:')
MAX_WORKERS = 4
MAX_RESOURCE_WORKERS = 8
SITEMAP_WORKERS = 2
REQUEST_TIMEOUT = 30
SITEMAP_TIMEOUT = 60  # Increased for Asian Paints
MAX_TEXT_LENGTH = 100
MAX_EMAIL_SIZE_MB = 15
SESSION_POOL_SIZE = 25
HISTORY_DIR = 'scan_history'
HISTORY_FILE = 'scan_history.json'

# Enhanced User-Agent rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (iPad; CPU OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
]

# Add delay between requests to avoid rate limiting
INTER_REQUEST_DELAY = 0.8
INTER_SITE_DELAY = 15

# --- Global State ---

EMAIL_CONFIG = EmailConfig()

# Asian Paints specific siteBH_SEO_V6.pymap discovery
ASIAN_PAINTS_SITEMAPS = ["https://www.asianpaints.com/sitemap-main-shop.xml",
            "https://www.asianpaints.com/sitemap-main-services.xml",
            "https://www.asianpaints.com/sitemap-main-products.xml",
            "https://www.asianpaints.com/sitemap-main-blogs.xml",
            "https://www.asianpaints.com/sitemap-main-misc.xml",
            "https://www.asianpaints.com/sitemap-main-more.xml",
            "https://www.asianpaints.com/sitemap-main-aphomes.xml",
            "https://www.asianpaints.com/sitemap-main-catalogue.xml",
            "https://www.asianpaints.com/sitemap-main-painting-contractor.xml",
            "https://www.asianpaints.com/sitemap-main-furnishing.xml",
            "https://www.asianpaints.com/sitemap-main-wheretheheartis.xml",
            "https://www.asianpaints.com/sitemap-main-safepaintingservices.xml",
            "https://www.asianpaints.com/sitemap-main-store-locator.xml",
            "https://www.asianpaints.com/sitemap-main-home-decor.xml",
            "https://www.asianpaints.com/sitemap-main-colour-inspiration-zone.xml",
            "https://www.asianpaints.com/sitemap-main-decorpro.xml",
            "https://www.asianpaints.com/sitemap-web-stories.xml",
    # Try multiple possible sitemap locations
    
]

SITES = [
    SiteConfig(
        name="AsianPaints",
        sitemaps=ASIAN_PAINTS_SITEMAPS,
        output_dir='AsianPaints_broken_links_reports',
        recipients=["Bhuwan.pandey@deptagency.com"],
        zip_filename='ASIAN_PAINTS_Broken_Image_Link.zip',
        use_slower_rate=True,
        verify_ssl=True,
        extra_headers={
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
            'TE': 'trailers',
        }
    ),
    SiteConfig(
        name="BeautifulHomes",
        sitemaps=[""],
        output_dir='BeautifulHomes_broken_links_reports',
        recipients=["Bhuwan.pandey@deptagency.com"],
        zip_filename='BEAUTIFULHOMES_Broken_Image_Link.zip',
        use_slower_rate=True,
        verify_ssl=True
    )
]

# Global session pool
SESSION_POOL = Queue(maxsize=SESSION_POOL_SIZE)
SESSION_LOCK = threading.Lock()
SITEMAP_STATUS_LOG = []

# Global URL cache
URL_CACHE = URLCache(max_size=50000, ttl=1800)

# --- Session Management Functions ---

def init_session_pool():
    """Initialize session pool for connection reuse"""
    logger.info(f"Initializing session pool with {SESSION_POOL_SIZE} sessions...")
    
    for _ in range(SESSION_POOL_SIZE):
        session = requests.Session()
        
        # Rotate user agents
        user_agent = random.choice(USER_AGENTS)
        
        session.headers.update({
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
            'Referer': 'https://www.google.com/',
        })
        
        # More aggressive retry strategy for Asian Paints
        retry_strategy = Retry(
            total=5,
            backoff_factor=1.5,
            status_forcelist=[408, 429, 500, 502, 503, 504, 522, 524],
            allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS"],
            raise_on_status=False
        )
        
        adapter = HTTPAdapter(
            pool_connections=100,
            pool_maxsize=100,
            max_retries=retry_strategy,
            pool_block=False
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        
        SESSION_POOL.put(session)
    
    logger.info("Session pool initialized successfully")
    
    # Log server information
    try:
        hostname = socket.gethostname()
        ip_address = socket.gethostbyname(hostname)
        logger.info(f"Server hostname: {hostname}")
        logger.info(f"Server IP: {ip_address}")
        print(f"🖥️  Server Information:")
        print(f"   Hostname: {hostname}")
        print(f"   IP Address: {ip_address}")
        
        # Test connectivity
        test_response = requests.get('https://www.google.com', timeout=5)
        print(f"   Internet Connectivity: ✓ ({test_response.status_code})")
    except Exception as e:
        logger.warning(f"Could not determine server info: {str(e)}")

def cleanup_session_pool():
    """Close and clear all sessions in the pool."""
    logger.info("Cleaning up session pool...")
    while not SESSION_POOL.empty():
        session = SESSION_POOL.get(block=False)
        try:
            session.close()
        except:
            pass
    logger.info("Session pool cleaned up.")

@contextmanager
def get_session():
    """Get session from pool"""
    session = SESSION_POOL.get()
    try:
        yield session
    finally:
        SESSION_POOL.put(session)

def test_sitemap_connectivity(sitemap_url: str) -> Dict:
    """Test connectivity to sitemap with multiple methods"""
    results = {
        'url': sitemap_url,
        'direct': {'status': 'Not tested', 'code': None, 'error': None},
        'with_proxy': {'status': 'Not tested', 'code': None, 'error': None},
        'with_browser_headers': {'status': 'Not tested', 'code': None, 'error': None}
    }
    
    # Test 1: Direct connection
    try:
        response = requests.get(sitemap_url, timeout=10, verify=True)
        results['direct'] = {
            'status': 'Success' if response.status_code == 200 else 'Failed',
            'code': response.status_code,
            'error': None,
            'headers': dict(response.headers),
            'content_preview': response.text[:200] if response.text else ''
        }
        response.close()
    except Exception as e:
        results['direct'] = {
            'status': 'Error',
            'code': None,
            'error': str(e),
            'headers': {},
            'content_preview': ''
        }
    
    # Test 2: With browser headers
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/xml,text/xml,application/xhtml+xml,text/html;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
        }
        response = requests.get(sitemap_url, headers=headers, timeout=10, verify=True)
        results['with_browser_headers'] = {
            'status': 'Success' if response.status_code == 200 else 'Failed',
            'code': response.status_code,
            'error': None,
            'headers': dict(response.headers),
            'content_preview': response.text[:200] if response.text else ''
        }
        response.close()
    except Exception as e:
        results['with_browser_headers'] = {
            'status': 'Error',
            'code': None,
            'error': str(e),
            'headers': {},
            'content_preview': ''
        }
    
    return results

# --- History Tracking Functions ---

def load_scan_history() -> Dict:
    """Load scan history from JSON file"""
    history_path = os.path.join(HISTORY_DIR, HISTORY_FILE)
    
    if not os.path.exists(history_path):
        return {}
    
    try:
        with open(history_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading scan history: {str(e)}")
        return {}

def save_current_scan(site_name: str, scan_date: str, sitemap_results: List, history: Dict):
    """Save current scan results to history - keeps only last 2 scans"""
    try:
        os.makedirs(HISTORY_DIR, exist_ok=True)
        
        if site_name not in history:
            history[site_name] = []
        
        sitemap_data = []
        for results, sitemap_url, _, _, _, success, sitemap_status in sitemap_results:
            if success and results:
                stats = calculate_stats(results)
                sitemap_data.append({
                    'sitemap_url': sitemap_url,
                    'scan_date': scan_date,
                    'total_pages': stats['total_pages'],
                    'pages_with_broken_links': stats['pages_with_broken_links'],
                    'pages_with_broken_images': stats['pages_with_broken_images'],
                    'total_broken_links': stats['broken_links'],
                    'total_broken_images': stats['broken_images'],
                    'noindex_nofollow_count': stats['noindex_nofollow_count']
                })
        
        site_scan = {
            'site_name': site_name,
            'scan_date': scan_date,
            'sitemap_data': sitemap_data
        }
        
        history[site_name].append(site_scan)
        
        # Keep only last 2 scans per site (current + previous)
        if len(history[site_name]) > 2:
            history[site_name] = history[site_name][-2:]
        
        history_path = os.path.join(HISTORY_DIR, HISTORY_FILE)
        with open(history_path, 'w', encoding='utf-8') as f:
            json.dump(history, f, indent=2)
        
        logger.info(f"Saved scan history for {site_name} (keeping last 2 scans)")
    except Exception as e:
        logger.error(f"Error saving scan history: {str(e)}")

def get_previous_sitemap_data(sitemap_url: str, site_name: str) -> Tuple[Optional[Dict], str, str]:
    """Get previous scan data for a specific sitemap"""
    history = load_scan_history()
    
    if site_name not in history or len(history[site_name]) == 0:
        return None, "", ""
    
    # Get the most recent previous scan (not current)
    previous_scan = history[site_name][-1]
    previous_date = previous_scan['scan_date']
    
    # Find matching sitemap in previous scan
    for sitemap_data in previous_scan.get('sitemap_data', []):
        if sitemap_data['sitemap_url'] == sitemap_url:
            return sitemap_data, previous_date, previous_date
    
    return None, previous_date, previous_date

# --- Utility Functions ---

def extract_robots_meta(soup: BeautifulSoup) -> str:
    """Extract robots meta tag content value"""
    try:
        robots_tag = soup.find('meta', attrs={'name': 'robots'})
        if robots_tag and robots_tag.get('content'):
            return robots_tag.get('content').strip()
        return ""
    except Exception as e:
        logger.debug(f"Error extracting robots meta: {str(e)}")
        return ""

def is_broken_status(status_code) -> bool:
    """Check if status code indicates a broken resource"""
    if isinstance(status_code, str):
        return status_code in BROKEN_STATUS_CODES
    return status_code in BROKEN_STATUS_CODES

def check_url_status_cached(url: str, session: requests.Session, max_retries: int = 3) -> Union[int, str]:
    """Check URL status with caching and enhanced error handling"""
    # First check cache
    cached_status = URL_CACHE.get(url)
    if cached_status is not None:
        logger.debug(f"Cache hit for URL: {url} -> {cached_status}")
        return cached_status
    
    # Not in cache, perform actual check
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                logger.debug(f"Retry {attempt} for {url}, waiting {wait_time:.2f}s")
                time.sleep(wait_time)
            
            # Small delay between requests
            time.sleep(random.uniform(0.2, 0.5))
            
            # Try HEAD first for speed
            response = session.head(url, timeout=(10, 20), allow_redirects=True, verify=True)
            status = response.status_code
            response.close()
            
            # If HEAD fails or returns certain status codes, try GET
            if status in [403, 405, 408, 429] or status >= 500:
                time.sleep(random.uniform(0.3, 0.7))
                response = session.get(url, timeout=(15, 30), allow_redirects=True, stream=True, verify=True)
                status = response.status_code
                response.close()
            
            # Cache the result
            URL_CACHE.set(url, status)
            return status
            
        except requests.exceptions.SSLError:
            if attempt == max_retries - 1:
                error_status = "SSL Error"
                URL_CACHE.set(url, error_status)
                return error_status
        except (requests.exceptions.Timeout, requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout):
            if attempt == max_retries - 1:
                error_status = "Timeout/Error"
                URL_CACHE.set(url, error_status)
                return error_status
        except requests.exceptions.TooManyRedirects:
            if attempt == max_retries - 1:
                error_status = "Too Many Redirects"
                URL_CACHE.set(url, error_status)
                return error_status
        except requests.exceptions.ConnectionError:
            if attempt == max_retries - 1:
                error_status = "Connection Error"
                URL_CACHE.set(url, error_status)
                return error_status
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                error_status = "Error"
                URL_CACHE.set(url, error_status)
                return error_status
        except Exception as e:
            if attempt == max_retries - 1:
                error_status = "Error"
                URL_CACHE.set(url, error_status)
                return error_status
    
    error_status = "Error"
    URL_CACHE.set(url, error_status)
    return error_status

def normalize_url(base_url: str, href: str) -> Union[str, None]:
    """Normalize and validate URL"""
    if not href or href.startswith(SKIP_SCHEMES):
        return None
    
    try:
        # Handle protocol-relative URLs
        if href.startswith('//'):
            href = 'https:' + href
        
        # Parse the URL
        parsed = urlparse(href)
        
        # Handle relative URLs
        if not parsed.scheme:
            # Check if it's an absolute path
            if href.startswith('/'):
                base_parsed = urlparse(base_url)
                href = f"{base_parsed.scheme}://{base_parsed.netloc}{href}"
            else:
                href = urljoin(base_url, href)
            
            # Re-parse after joining
            parsed = urlparse(href)
        
        # Ensure proper scheme
        if not parsed.scheme:
            return None
        
        # Clean up the URL
        path = parsed.path
        if path.endswith('/'):
            path = path.rstrip('/')
        
        # Reconstruct URL
        normalized = urlunparse((
            parsed.scheme,
            parsed.netloc.lower(),
            path,
            parsed.params,
            parsed.query,
            parsed.fragment
        ))
        
        return normalized
        
    except Exception as e:
        logger.debug(f"Error normalizing URL {href}: {str(e)}")
        return None

def get_next_sibling_text(element, max_length: int = 200) -> str:
    """Get text content from the next sibling element for context"""
    try:
        next_sibling = element.find_next_sibling()
        if next_sibling:
            text = next_sibling.get_text(strip=True, separator=' ')[:max_length]
            if text:
                return text
            tag_info = f"<{next_sibling.name}"
            if next_sibling.get('class'):
                tag_info += f" class='{' '.join(next_sibling.get('class'))}'"
            if next_sibling.get('id'):
                tag_info += f" id='{next_sibling.get('id')}'"
            tag_info += ">"
            return tag_info
        return "No next sibling"
    except Exception as e:
        return f"Error: {str(e)}"

def check_resource_batch(resources: List[tuple], session: requests.Session) -> List[LinkResult]:
    """Check multiple resources in parallel with caching - only returns broken resources"""
    results = []
    
    # Separate cached and uncached resources
    uncached_resources = []
    cached_results = []
    
    for resource in resources:
        url, text = resource[0], resource[1]
        next_tag_data = resource[2] if len(resource) > 2 else ""
        
        # Check cache first
        cached_status = URL_CACHE.get(url)
        if cached_status is not None:
            if is_broken_status(cached_status):
                cached_results.append(LinkResult(
                    url=url,
                    status_code=cached_status,
                    text=text,
                    next_tag_data=next_tag_data
                ))
        else:
            uncached_resources.append(resource)
    
    # Add cached broken results
    results.extend(cached_results)
    
    if not uncached_resources:
        return results
    
    # Process uncached resources with limited concurrency
    batch_size = 20
    for i in range(0, len(uncached_resources), batch_size):
        batch = uncached_resources[i:i+batch_size]
        
        with ThreadPoolExecutor(max_workers=min(MAX_RESOURCE_WORKERS, len(batch))) as executor:
            futures = {
                executor.submit(check_url_status_cached, resource[0], session): resource 
                for resource in batch
            }
            
            for future in as_completed(futures):
                resource = futures[future]
                url, text = resource[0], resource[1]
                next_tag_data = resource[2] if len(resource) > 2 else ""
                
                try:
                    status = future.result()
                    if is_broken_status(status):
                        results.append(LinkResult(
                            url=url,
                            status_code=status,
                            text=text,
                            next_tag_data=next_tag_data
                        ))
                except Exception as e:
                    logger.error(f"Error checking {url}: {str(e)}")
                    error_result = LinkResult(
                        url=url,
                        status_code="Error",
                        text=text,
                        next_tag_data=next_tag_data
                    )
                    results.append(error_result)
                    URL_CACHE.set(url, "Error")
    
    return results

def process_url(url: str, site_config: SiteConfig = None) -> PageResult:
    """Process single URL: fetch, parse, check robots meta, and check links/images"""
    try:
        with get_session() as session:
            # Add site-specific headers
            if site_config and site_config.extra_headers:
                session.headers.update(site_config.extra_headers)
            
            # Add random delay for Asian Paints
            if site_config and site_config.name == "AsianPaints":
                time.sleep(random.uniform(1.0, 2.0))
            
            verify_ssl = site_config.verify_ssl if site_config else True
            
            response = session.get(
                url, 
                allow_redirects=True, 
                timeout=REQUEST_TIMEOUT,
                verify=verify_ssl
            )
            response_code = response.status_code
            
            if response_code != 200:
                logger.warning(f"Non-200 response for {url}: {response_code}")
                response.close()
                return PageResult(url=url, response_code=response_code, robots_meta="")
            
            try:
                soup = BeautifulSoup(response.content, "lxml")
            except:
                soup = BeautifulSoup(response.content, "html.parser")
            
            robots_meta = extract_robots_meta(soup)
            response.close()
            
            links_to_check = []
            checked_urls: Set[str] = set()
            
            for link in soup.find_all("a", href=True):
                link_url = normalize_url(url, link.get("href"))
                if link_url and link_url not in checked_urls:
                    checked_urls.add(link_url)
                    link_text = link.get_text().strip()[:MAX_TEXT_LENGTH] or "No Text"
                    links_to_check.append((link_url, link_text, ""))
            
            images_to_check = []
            checked_images: Set[str] = set()
            
            for img in soup.find_all("img", src=True):
                img_url = normalize_url(url, img.get("src"))
                if img_url and img_url not in checked_images:
                    checked_images.add(img_url)
                    alt_text = img.get("alt", "No Alt Text")[:MAX_TEXT_LENGTH]
                    next_tag_text = get_next_sibling_text(img)
                    images_to_check.append((img_url, alt_text, next_tag_text))
            
            # Check resources in batches
            broken_links = []
            broken_images = []
            
            if links_to_check:
                broken_links = check_resource_batch(links_to_check, session)
            
            if images_to_check:
                broken_images = check_resource_batch(images_to_check, session)
            
            return PageResult(
                url=url,
                response_code=response_code,
                robots_meta=robots_meta,
                broken_links=broken_links,
                broken_images=broken_images
            )
            
    except Exception as e:
        logger.error(f"Error processing {url}: {str(e)}")
        return PageResult(url=url, response_code="Error", robots_meta="")

# --- Enhanced Sitemap Functions ---

def discover_sitemaps_from_robots(domain: str) -> List[str]:
    """Discover sitemaps from robots.txt"""
    sitemaps = []
    robots_url = f"{domain.rstrip('/')}/robots.txt"
    
    try:
        with get_session() as session:
            # Special headers for robots.txt
            headers = {
                'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)',
                'Accept': 'text/plain',
            }
            
            response = session.get(robots_url, headers=headers, timeout=10, verify=True)
            
            if response.status_code == 200:
                for line in response.text.split('\n'):
                    line = line.strip()
                    if line.lower().startswith('sitemap:'):
                        sitemap_url = line.split(':', 1)[1].strip()
                        sitemaps.append(sitemap_url)
                        logger.info(f"Found sitemap in robots.txt: {sitemap_url}")
            
            response.close()
    except Exception as e:
        logger.warning(f"Could not fetch robots.txt from {domain}: {str(e)}")
    
    return sitemaps

def fetch_sitemap_urls(sitemap_url: str, site_config: SiteConfig = None) -> tuple:
    """Enhanced sitemap fetching for Asian Paints"""
    status = SitemapStatus(
        url=sitemap_url,
        status='FAILED',
        urls_found=0,
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )
    
    try:
        with get_session() as session:
            # Enhanced headers for Asian Paints
            enhanced_headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/xml,text/xml,application/xhtml+xml,text/html;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Cache-Control': 'max-age=0',
            }
            
            if site_config and site_config.extra_headers:
                enhanced_headers.update(site_config.extra_headers)
            
            session.headers.update(enhanced_headers)
            
            logger.info(f"Fetching sitemap: {sitemap_url}")
            print(f"🌐 Fetching: {sitemap_url}")
            
            start_time = time.time()
            
            # Try multiple times with delays
            response = None
            for attempt in range(3):
                try:
                    verify_ssl = site_config.verify_ssl if site_config else True
                    response = session.get(
                        sitemap_url, 
                        timeout=SITEMAP_TIMEOUT, 
                        verify=verify_ssl,
                        allow_redirects=True
                    )
                    break
                except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                    if attempt == 2:
                        raise
                    logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                    time.sleep(3 * (attempt + 1))
            
            if not response:
                status.error_message = "No response received"
                SITEMAP_STATUS_LOG.append(status)
                return ([], status)
            
            status_code = response.status_code
            status.status_code = status_code
            
            print(f"📋 Status Code: [{status_code}] - {sitemap_url}")
            logger.info(f"Sitemap returned status code: {status_code}")
            
            if status_code != 200:
                # Log response details for debugging
                logger.error(f"Non-200 response. Headers: {dict(response.headers)}")
                if response.text:
                    logger.error(f"Response preview (500 chars): {response.text[:500]}")
                
                status.error_message = f"HTTP {status_code}"
                response.close()
                SITEMAP_STATUS_LOG.append(status)
                return ([], status)
            
            # Parse response
            content_type = response.headers.get('content-type', '').lower()
            content = response.text
            
            logger.info(f"Content-Type: {content_type}")
            logger.info(f"Content length: {len(content)} chars")
            
            # Try different parsing methods
            urls = []
            
            # Method 1: Try as XML
            if 'xml' in content_type or content.strip().startswith('<?xml'):
                try:
                    soup = BeautifulSoup(content, 'xml')
                    # Look for URLs in various XML sitemap formats
                    url_tags = soup.find_all(['loc', 'url'])
                    for tag in url_tags:
                        if tag.name == 'loc' and tag.text:
                            urls.append(tag.text.strip())
                        elif tag.name == 'url':
                            loc_tag = tag.find('loc')
                            if loc_tag and loc_tag.text:
                                urls.append(loc_tag.text.strip())
                    
                    logger.info(f"XML parsing found {len(urls)} URLs")
                except Exception as e:
                    logger.warning(f"XML parsing failed: {str(e)}")
            
            # Method 2: Try regex patterns
            if not urls:
                try:
                    # Common sitemap patterns
                    patterns = [
                        r'<loc>\s*(https?://[^<]+?)\s*</loc>',
                        r'<url>\s*<loc>\s*(https?://[^<]+?)\s*</loc>',
                        r'https?://[^<\s"\']+',  # Fallback for any URL
                    ]
                    
                    for pattern in patterns:
                        found_urls = re.findall(pattern, content, re.IGNORECASE)
                        if found_urls:
                            urls = list(set(found_urls))  # Remove duplicates
                            logger.info(f"Regex pattern found {len(urls)} URLs")
                            break
                except Exception as e:
                    logger.warning(f"Regex parsing failed: {str(e)}")
            
            # Method 3: Try line-by-line parsing
            if not urls:
                try:
                    for line in content.split('\n'):
                        line = line.strip()
                        if 'http' in line.lower():
                            # Extract URL from line
                            url_match = re.search(r'(https?://[^\s<>"\']+)', line)
                            if url_match:
                                urls.append(url_match.group(0))
                    logger.info(f"Line parsing found {len(urls)} URLs")
                except Exception as e:
                    logger.warning(f"Line parsing failed: {str(e)}")
            
            response.close()
            
            if not urls:
                # Save response for debugging
                debug_filename = f"debug_sitemap_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
                with open(debug_filename, 'w', encoding='utf-8') as f:
                    f.write(f"URL: {sitemap_url}\n")
                    f.write(f"Status: {status_code}\n")
                    f.write(f"Headers: {dict(response.headers)}\n")
                    f.write(f"Content:\n{content[:5000]}\n")
                
                logger.error(f"No URLs found in sitemap. Debug saved to {debug_filename}")
                print(f"⚠️  No URLs found. Debug file: {debug_filename}")
                
                status.status = 'EMPTY'
                status.error_message = f"No URLs found - see {debug_filename}"
                SITEMAP_STATUS_LOG.append(status)
                return ([], status)
            
            # Process and deduplicate URLs
            unique_urls = []
            seen_urls = set()
            
            for url in urls:
                try:
                    # Clean and normalize URL
                    url = url.strip()
                    if not url:
                        continue
                    
                    # Parse and reconstruct
                    parsed = urlparse(url)
                    if not parsed.scheme or not parsed.netloc:
                        continue
                    
                    # Add qaAutomation parameter
                    query_params = parse_qs(parsed.query)
                    query_params["qaAutomation"] = ["true"]
                    new_query = urlencode(query_params, doseq=True)
                    
                    normalized_url = urlunparse((
                        parsed.scheme,
                        parsed.netloc.lower(),
                        parsed.path.rstrip('/'),
                        parsed.params,
                        new_query,
                        parsed.fragment
                    ))
                    
                    if normalized_url not in seen_urls:
                        seen_urls.add(normalized_url)
                        unique_urls.append(normalized_url)
                        
                except Exception as e:
                    logger.warning(f"Error processing URL {url}: {str(e)}")
                    if url not in seen_urls:
                        seen_urls.add(url)
                        unique_urls.append(url)
            
            status.status = 'SUCCESS'
            status.urls_found = len(unique_urls)
            status.scan_time = time.time() - start_time
            
            logger.info(f"✓ Successfully fetched {len(unique_urls)} unique URLs")
            print(f"✅ Found {len(unique_urls)} URLs")
            SITEMAP_STATUS_LOG.append(status)
            return (unique_urls, status)
            
    except requests.exceptions.Timeout:
        status.status_code = "Timeout"
        status.error_message = f"Timeout after {SITEMAP_TIMEOUT}s"
        logger.error(f"✗ Timeout fetching sitemap: {sitemap_url}")
        SITEMAP_STATUS_LOG.append(status)
        return ([], status)
    except requests.exceptions.SSLError as e:
        status.status_code = "SSL Error"
        status.error_message = f"SSL Error: {str(e)}"
        logger.error(f"✗ SSL Error fetching sitemap: {sitemap_url}")
        SITEMAP_STATUS_LOG.append(status)
        return ([], status)
    except Exception as e:
        status.status_code = "Error"
        status.error_message = str(e)
        logger.error(f"✗ Error fetching sitemap {sitemap_url}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        SITEMAP_STATUS_LOG.append(status)
        return ([], status)

def process_sitemap(sitemap_url: str, output_dir: str, project_name: str, site_config: SiteConfig = None) -> tuple:
    """Process sitemap with progress tracking and report saving"""
    logger.info(f"Processing sitemap: {sitemap_url}")
    print(f"\n📊 Processing: {sitemap_url}")
    
    scan_start_time = time.time()
    scan_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    urls, sitemap_status = fetch_sitemap_urls(sitemap_url, site_config)
    
    if not urls:
        logger.warning(f"No URLs to process for sitemap: {sitemap_url}")
        print(f"⚠️  No URLs found in sitemap")
        return ([], sitemap_url, project_name, scan_datetime, 0, False, sitemap_status)
    
    logger.info(f"Found {len(urls)} URLs to check")
    print(f"🔍 Checking {len(urls)} URLs...")
    
    results = []
    
    # Process URLs in batches to manage memory
    batch_size = 50
    total_batches = (len(urls) + batch_size - 1) // batch_size
    
    for batch_num in range(total_batches):
        batch_start = batch_num * batch_size
        batch_end = min(batch_start + batch_size, len(urls))
        batch_urls = urls[batch_start:batch_end]
        
        print(f"   Batch {batch_num + 1}/{total_batches}: URLs {batch_start + 1}-{batch_end}")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_url, url, site_config): url for url in batch_urls}
            
            for i, future in enumerate(as_completed(futures), 1):
                try:
                    result = future.result()
                    results.append(result)
                    
                    progress = batch_start + i
                    if progress % 10 == 0 or progress == len(urls):
                        percent = (progress / len(urls)) * 100
                        print(f"   Progress: {progress}/{len(urls)} ({percent:.1f}%)")
                except Exception as e:
                    logger.error(f"Error processing URL: {str(e)}")
        
        # Small delay between batches
        if batch_num < total_batches - 1:
            time.sleep(2)
    
    scan_time = (time.time() - scan_start_time) / 60
    save_report(results, sitemap_url, output_dir, project_name, scan_datetime, scan_time)
    
    logger.info(f"✓ Completed sitemap in {scan_time:.2f} minutes")
    print(f"✅ Completed in {scan_time:.2f} minutes")
    
    return (results, sitemap_url, project_name, scan_datetime, scan_time, True, sitemap_status)

# --- Reporting and Email Functions ---

def calculate_stats(results: List[PageResult]) -> Dict:
    """Calculate summary statistics including robots meta tag counts"""
    total_broken_links = sum(len(r.broken_links) for r in results)
    total_broken_images = sum(len(r.broken_images) for r in results)
    pages_with_broken_links = sum(1 for r in results if r.broken_links)
    pages_with_broken_images = sum(1 for r in results if r.broken_images)
    
    noindex_nofollow_count = 0
    for r in results:
        if r.robots_meta:
            meta_lower = r.robots_meta.lower()
            if 'noindex' in meta_lower and 'nofollow' in meta_lower:
                noindex_nofollow_count += 1
    
    return {
        'total_pages': len(results),
        'broken_links': total_broken_links,
        'broken_images': total_broken_images,
        'pages_with_broken_links': pages_with_broken_links,
        'pages_with_broken_images': pages_with_broken_images,
        'noindex_nofollow_count': noindex_nofollow_count
    }

def create_comparison_sheet_data(current_stats: Dict, previous_data: Optional[Dict], 
                                current_date: str, previous_date: str) -> List[Dict]:
    """Create data for comparison sheet"""
    comparison_data = []
    
    # Basic info comparison
    comparison_data.append({
        'Metric': 'Scan Information',
        'Current Value': current_date,
        'Previous Value': previous_date if previous_data else 'First Scan',
        'Change/Difference': f"Time between scans: {calculate_time_difference(current_date, previous_date) if previous_data else 'N/A'}"
    })
    
    # Total Pages comparison
    current_total = current_stats['total_pages']
    previous_total = previous_data['total_pages'] if previous_data else 0
    pages_change = current_total - previous_total
    comparison_data.append({
        'Metric': 'Total Pages Checked',
        'Current Value': current_total,
        'Previous Value': previous_total if previous_data else 'N/A',
        'Change/Difference': f"{pages_change:+d}",
        'Trend': get_trend_emoji(pages_change)
    })
    
    # Broken Links comparison
    current_links = current_stats['broken_links']
    previous_links = previous_data['total_broken_links'] if previous_data else 0
    links_change = current_links - previous_links
    comparison_data.append({
        'Metric': 'Total Broken Links',
        'Current Value': current_links,
        'Previous Value': previous_links if previous_data else 'N/A',
        'Change/Difference': f"{links_change:+d}",
        'Trend': get_trend_emoji(links_change, reverse=True)
    })
    
    # Broken Images comparison
    current_images = current_stats['broken_images']
    previous_images = previous_data['total_broken_images'] if previous_data else 0
    images_change = current_images - previous_images
    comparison_data.append({
        'Metric': 'Total Broken Images',
        'Current Value': current_images,
        'Previous Value': previous_images if previous_data else 'N/A',
        'Change/Difference': f"{images_change:+d}",
        'Trend': get_trend_emoji(images_change, reverse=True)
    })
    
    # Pages with broken links comparison
    current_pages_links = current_stats['pages_with_broken_links']
    previous_pages_links = previous_data['pages_with_broken_links'] if previous_data else 0
    pages_links_change = current_pages_links - previous_pages_links
    comparison_data.append({
        'Metric': 'Pages with Broken Links',
        'Current Value': current_pages_links,
        'Previous Value': previous_pages_links if previous_data else 'N/A',
        'Change/Difference': f"{pages_links_change:+d}",
        'Trend': get_trend_emoji(pages_links_change, reverse=True)
    })
    
    # Pages with broken images comparison
    current_pages_images = current_stats['pages_with_broken_images']
    previous_pages_images = previous_data['pages_with_broken_images'] if previous_data else 0
    pages_images_change = current_pages_images - previous_pages_images
    comparison_data.append({
        'Metric': 'Pages with Broken Images',
        'Current Value': current_pages_images,
        'Previous Value': previous_pages_images if previous_data else 'N/A',
        'Change/Difference': f"{pages_images_change:+d}",
        'Trend': get_trend_emoji(pages_images_change, reverse=True)
    })
    
    # NoIndex/NoFollow comparison
    current_noindex = current_stats['noindex_nofollow_count']
    previous_noindex = previous_data['noindex_nofollow_count'] if previous_data else 0
    noindex_change = current_noindex - previous_noindex
    comparison_data.append({
        'Metric': 'Pages with [noindex, nofollow]',
        'Current Value': current_noindex,
        'Previous Value': previous_noindex if previous_data else 'N/A',
        'Change/Difference': f"{noindex_change:+d}",
        'Trend': get_trend_emoji(noindex_change, reverse=True)
    })
    
    # Calculate percentage changes
    if previous_data and previous_total > 0:
        links_percent = (links_change / previous_total) * 100
        images_percent = (images_change / previous_total) * 100
        
        comparison_data.append({
            'Metric': 'Broken Links % of Total Pages',
            'Current Value': f"{(current_links/current_total)*100:.1f}%" if current_total > 0 else "0%",
            'Previous Value': f"{(previous_links/previous_total)*100:.1f}%" if previous_total > 0 else "0%",
            'Change/Difference': f"{links_percent:+.1f}%",
            'Trend': get_trend_emoji(links_percent, reverse=True)
        })
        
        comparison_data.append({
            'Metric': 'Broken Images % of Total Pages',
            'Current Value': f"{(current_images/current_total)*100:.1f}%" if current_total > 0 else "0%",
            'Previous Value': f"{(previous_images/previous_total)*100:.1f}%" if previous_total > 0 else "0%",
            'Change/Difference': f"{images_percent:+.1f}%",
            'Trend': get_trend_emoji(images_percent, reverse=True)
        })
    
    return comparison_data

def get_trend_emoji(change: float, reverse: bool = False) -> str:
    """Get trend emoji based on change value"""
    if change > 0:
        return "🔴" if not reverse else "🟢"
    elif change < 0:
        return "🟢" if not reverse else "🔴"
    else:
        return "➖"

def calculate_time_difference(current_date: str, previous_date: str) -> str:
    """Calculate time difference between two dates"""
    try:
        current_dt = datetime.strptime(current_date, "%Y-%m-%d %H:%M:%S")
        previous_dt = datetime.strptime(previous_date, "%Y-%m-%d %H:%M:%S")
        
        diff = current_dt - previous_dt
        days = diff.days
        hours = diff.seconds // 3600
        minutes = (diff.seconds % 3600) // 60
        
        if days > 0:
            return f"{days} days, {hours} hours"
        elif hours > 0:
            return f"{hours} hours, {minutes} minutes"
        else:
            return f"{minutes} minutes"
    except:
        return "N/A"

def save_report(results: List[PageResult], sitemap_url: str, output_dir: str, 
                project_name: str, scan_datetime: str, scan_time: float):
    """Save comprehensive Excel report with separate comparison sheet"""
    if not results:
        logger.warning(f"No results to save for {sitemap_url}")
        return
    
    # Get current stats
    current_stats = calculate_stats(results)
    
    # Get previous data for this sitemap
    previous_data, previous_date, previous_time = get_previous_sitemap_data(sitemap_url, project_name)
    
    # Create comparison data
    comparison_data = create_comparison_sheet_data(current_stats, previous_data, scan_datetime, previous_date)
    
    # Create safe filename
    domain_name = urlparse(sitemap_url).netloc.replace('.', '_')
    path_name = urlparse(sitemap_url).path.replace('/', '_').replace('.', '_')
    if len(path_name) > 50:
        path_name = path_name[:50]
    
    excel_filename = f"{domain_name}{path_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    excel_path = os.path.join(output_dir, excel_filename)
    
    try:
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            # Summary Report
            summary_data = {
                "Metric": [
                    "Project Name", "Sitemap URL", "Date & Time of Scan", "Total Pages Checked",
                    "Pages with Broken Links", "Pages with Broken Images",
                    "Total Broken Links (4xx, 5xx errors)", "Total Broken Images (4xx, 5xx errors)",
                    "Pages with [noindex, nofollow]",
                    "Total Time for Scan (minutes)"
                ],
                "Value": [
                    project_name, sitemap_url, scan_datetime, current_stats['total_pages'],
                    current_stats['pages_with_broken_links'], current_stats['pages_with_broken_images'],
                    current_stats['broken_links'], current_stats['broken_images'],
                    current_stats['noindex_nofollow_count'],
                    f"{scan_time:.2f}"
                ]
            }
            
            df_summary = pd.DataFrame(summary_data)
            df_summary.to_excel(writer, sheet_name='Summary Report', index=False)
            
            # Comparison Sheet
            if comparison_data:
                df_comparison = pd.DataFrame(comparison_data)
                df_comparison.to_excel(writer, sheet_name='Scan Comparison', index=False)
            
            # Pages Overview
            pages_data = []
            for r in results:
                pages_data.append({
                    "URL": r.url,
                    "Page Status Code": r.response_code,
                    "Robots Meta Tag": r.robots_meta if r.robots_meta else "Not Found",
                    "Broken Links Count": len(r.broken_links),
                    "Broken Images Count": len(r.broken_images)
                })
            
            if pages_data:
                pd.DataFrame(pages_data).to_excel(writer, sheet_name='Pages Overview', index=False)
            
            # Broken Links
            broken_links = []
            for r in results:
                for link in r.broken_links:
                    broken_links.append({
                        "Page URL": r.url,
                        "Link URL": link.url,
                        "Status Code": link.status_code,
                        "Link Text": link.text[:100]
                    })
            
            if broken_links:
                pd.DataFrame(broken_links).to_excel(writer, sheet_name='Broken Links', index=False)
            
            # Broken Images
            broken_images = []
            for r in results:
                for img in r.broken_images:
                    broken_images.append({
                        "Page URL": r.url,
                        "Image URL": img.url,
                        "Status Code": img.status_code,
                        "Alt Text": img.text[:100],
                        "Next Tag Content": img.next_tag_data[:200] if img.next_tag_data else ""
                    })
            
            if broken_images:
                pd.DataFrame(broken_images).to_excel(writer, sheet_name='Broken Images', index=False)
            
            # Format worksheets
            workbook = writer.book
            
            # Auto-adjust column widths
            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            cell_length = len(str(cell.value))
                            if cell_length > max_length:
                                max_length = cell_length
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
        
        logger.info(f"Report saved: {excel_path}")
        print(f"📄 Report saved: {excel_filename}")
        
    except Exception as e:
        logger.error(f"Error saving report: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

def print_sitemap_status_table(site_name: str, sitemap_statuses: List[SitemapStatus]):
    """Print a formatted table of sitemap statuses with status codes"""
    print(f"\n{'='*120}")
    print(f"SITEMAP STATUS REPORT - {site_name}")
    print(f"{'='*120}")
    print(f"{'#':<4} {'Status':<10} {'Code':<8} {'URLs':<8} {'Time(s)':<10} {'Sitemap URL':<50} {'Error':<20}")
    print(f"{'-'*120}")
    
    for idx, status in enumerate(sitemap_statuses, 1):
        status_symbol = "✓" if status.status == 'SUCCESS' else "✗" if status.status == 'FAILED' else "⚠"
        time_str = f"{status.scan_time:.2f}" if status.scan_time > 0 else "N/A"
        error_str = status.error_message[:18] + ".." if len(status.error_message) > 20 else status.error_message
        code_str = str(status.status_code) if status.status_code else "N/A"
        
        print(f"{idx:<4} {status_symbol} {status.status:<8} {code_str:<8} {status.urls_found:<8} {time_str:<10} "
              f"{status.url[:48]:<50} {error_str:<20}")
    
    print(f"{'-'*120}")
    success_count = sum(1 for s in sitemap_statuses if s.status == 'SUCCESS')
    failed_count = sum(1 for s in sitemap_statuses if s.status == 'FAILED')
    empty_count = sum(1 for s in sitemap_statuses if s.status == 'EMPTY')
    
    print(f"SUMMARY: Total: {len(sitemap_statuses)} | "
          f"Success: {success_count} | Failed: {failed_count} | Empty: {empty_count}")
    print(f"{'='*120}\n")

def write_error_log(site_name: str, sitemap_statuses: List[SitemapStatus], output_dir: str = ".") -> Optional[str]:
    """Write error log to text file with status codes"""
    failed_statuses = [s for s in sitemap_statuses if s.status in ['FAILED', 'EMPTY']]
    
    if not failed_statuses:
        logger.info(f"No errors to log for {site_name}")
        return None
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"{site_name}_Sitemap_Errors_{timestamp}.txt"
    log_path = os.path.join(output_dir, log_filename)
    
    try:
        with open(log_path, 'w', encoding='utf-8') as f:
            f.write("="*100 + "\n")
            f.write(f"SITEMAP ERROR LOG - {site_name}\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("="*100 + "\n\n")
            
            f.write(f"Total Sitemaps Checked: {len(sitemap_statuses)}\n")
            f.write(f"Failed Sitemaps: {len(failed_statuses)}\n")
            success_rate = ((len(sitemap_statuses)-len(failed_statuses))/len(sitemap_statuses)*100) if sitemap_statuses else 0
            f.write(f"Success Rate: {success_rate:.1f}%\n\n")
            
            f.write("="*100 + "\n")
            f.write("FAILED SITEMAP DETAILS\n")
            f.write("="*100 + "\n\n")
            
            for idx, status in enumerate(failed_statuses, 1):
                f.write(f"[{idx}] {status.status} - {status.timestamp}\n")
                f.write(f"    Sitemap URL: {status.url}\n")
                f.write(f"    Status Code: {status.status_code}\n")
                f.write(f"    Error Message: {status.error_message}\n")
                f.write(f"    URLs Found: {status.urls_found}\n")
                f.write(f"    Scan Time: {status.scan_time:.2f}s\n")
                f.write("-"*100 + "\n\n")
            
            f.write("="*100 + "\n")
            f.write("END OF ERROR LOG\n")
            f.write("="*100 + "\n")
        
        logger.info(f"✓ Error log created: {log_path}")
        return log_path
        
    except Exception as e:
        logger.error(f"Failed to write error log: {str(e)}")
        return None

def get_file_size_mb(file_path: str) -> float:
    """Get file size in MB"""
    if os.path.exists(file_path):
        return os.path.getsize(file_path) / (1024 * 1024)
    return 0

def create_zip(output_dir: str, zip_path: str) -> float:
    """Create zip file and return size in MB"""
    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(output_dir):
                for file in files:
                    if file.endswith('.xlsx') or file.endswith('.txt'):
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, output_dir)
                        zipf.write(file_path, arcname=arcname)
        
        size_mb = get_file_size_mb(zip_path)
        logger.info(f"Created zip: {zip_path} ({size_mb:.2f} MB)")
        return size_mb
    except Exception as e:
        logger.error(f"Error creating zip: {str(e)}")
        raise

def split_files_into_groups(output_dir: str, num_groups: int) -> List[List[str]]:
    """Split Excel files into groups of roughly equal size"""
    excel_files = [f for f in os.listdir(output_dir) if f.endswith('.xlsx')]
    
    if not excel_files:
        return []
    
    file_sizes = []
    for f in excel_files:
        path = os.path.join(output_dir, f)
        size_mb = get_file_size_mb(path)
        file_sizes.append((f, size_mb))
    
    file_sizes.sort(key=lambda x: x[1], reverse=True)
    
    groups = [[] for _ in range(num_groups)]
    group_sizes = [0.0] * num_groups
    
    for filename, size in file_sizes:
        min_idx = group_sizes.index(min(group_sizes))
        groups[min_idx].append(filename)
        group_sizes[min_idx] += size
    
    return [g for g in groups if g]

def create_split_zips(output_dir: str, base_zip_name: str, num_parts: int) -> List[str]:
    """Create multiple ZIP files by splitting Excel reports"""
    file_groups = split_files_into_groups(output_dir, num_parts)
    
    if not file_groups:
        return []
    
    zip_files = []
    base_name = base_zip_name.replace('.zip', '')
    
    for idx, file_group in enumerate(file_groups, 1):
        zip_name = f"{base_name}_Part{idx}of{len(file_groups)}.zip"
        
        with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for filename in file_group:
                file_path = os.path.join(output_dir, filename)
                zipf.write(file_path, arcname=filename)
        
        size_mb = get_file_size_mb(zip_name)
        logger.info(f"Created split ZIP {idx}/{len(file_groups)}: {zip_name} ({size_mb:.2f} MB, {len(file_group)} files)")
        zip_files.append(zip_name)
    
    return zip_files

def send_email(subject: str, body: str, recipients: List[str], attachment_path: str):
    """Send email with attachment"""
    recipients = [email.strip() for email in recipients if email.strip()]
    
    if not recipients:
        logger.error("No valid recipients")
        return
    
    msg = MIMEMultipart()
    msg['From'] = EMAIL_CONFIG.sender
    msg['To'] = ', '.join(recipients)
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))
    
    try:
        file_size = get_file_size_mb(attachment_path)
        if file_size > 0:
            with open(attachment_path, 'rb') as f:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(attachment_path)}"')
                msg.attach(part)
            
            logger.info(f"Attachment: {os.path.basename(attachment_path)} ({file_size:.2f} MB)")
        else:
            logger.error(f"Attachment file not found or empty: {attachment_path}")
            return
    except Exception as e:
        logger.error(f"Error attaching file: {str(e)}")
        return
    
    try:
        with smtplib.SMTP(EMAIL_CONFIG.smtp_server, EMAIL_CONFIG.smtp_port) as server:
            server.starttls()
            server.login(EMAIL_CONFIG.username, EMAIL_CONFIG.password)
            server.sendmail(EMAIL_CONFIG.sender, recipients, msg.as_string())
        logger.info(f"✓ Email sent to {', '.join(recipients)}")
    except Exception as e:
        logger.error(f"✗ Email failed: {str(e)}")

def generate_email_body(site: SiteConfig, stats: Dict, execution_time: float, 
                       zip_filename: str, sitemap_summary: Dict, part_info: str = "", 
                       has_comparison: bool = False, is_first_scan: bool = False,
                       cache_stats: Optional[Dict] = None) -> str:
    """Generate email body with comparison info"""
    part_text = f"\n{part_info}\n" if part_info else ""
    
    if is_first_scan:
        comparison_text = "\n\n📊 NOTE: This is the FIRST SCAN for this site. Baseline data has been recorded. Comparison reports will be available from the next scan onwards."
    elif has_comparison:
        comparison_text = "\n\n📊 COMPARISON REPORT INCLUDED: Each sitemap report now contains a separate 'Scan Comparison' sheet showing detailed comparison with the previous scan including date/time differences and trend analysis."
    else:
        comparison_text = ""
    
    cache_info = ""
    if cache_stats:
        hit_rate_percent = cache_stats['hit_rate'] * 100
        cache_info = f"\n🔍 CACHE STATISTICS:\n  - URLs checked from cache: {cache_stats['hits']:,}\n  - URLs checked from network: {cache_stats['misses']:,}\n  - Cache hit rate: {hit_rate_percent:.1f}%\n  - Total URLs cached: {cache_stats['size']:,}\n  - Average checks per URL: {cache_stats['avg_checks_per_url']:.1f}"
    
    return f"""Greetings, {site.name} Team.

Kindly review the outcomes of a recent Broken Links and Images check for the website.{part_text}

Report Summary:
- Total XML Sitemaps Analyzed: {sitemap_summary['total']}
- Successfully Processed Sitemaps: {sitemap_summary['success']}
- Failed Sitemaps (Empty/Error): {sitemap_summary['failed']}
- Total Pages Checked: {stats['total_pages']:,}
- Total Broken Links Found: {stats['broken_links']:,}
- Total Broken Images Found: {stats['broken_images']:,}
- Pages with [noindex, nofollow]: {stats['noindex_nofollow_count']:,}
- Total Scan Execution Time (all sitemaps for site): {execution_time:.2f} minutes{cache_info}{comparison_text}

The report is attached as '{zip_filename}'.

Report Contents:
  1. Summary Report - Overview with project name, sitemap URL, scan date/time, and key metrics
  2. Scan Comparison - Separate sheet showing detailed comparison with previous scan including dates/times and trend analysis
  3. Pages Overview - List of all pages with robots meta tags and issue counts
  4. Broken Links - Complete list of broken links with status codes
  5. Broken Images - Complete list of broken images with status codes and context

Each report now includes a dedicated comparison sheet showing changes from the previous scan with date/time information.

Please feel free to review the attached reports and let us know if you have any questions or concerns.

Thanks & Regards,
Q.A Automation Team,
DEPT®"""

def process_sitemap_batch(sitemaps: List[str], output_dir: str, project_name: str, site_config: SiteConfig = None) -> tuple:
    """Process multiple sitemaps with controlled parallelism"""
    all_results = []
    site_sitemap_statuses = []
    
    logger.info(f"Processing {len(sitemaps)} sitemaps with {SITEMAP_WORKERS} workers")
    print(f"\n🚀 Processing {len(sitemaps)} sitemaps...")
    
    with ThreadPoolExecutor(max_workers=SITEMAP_WORKERS) as executor:
        futures = {
            executor.submit(process_sitemap, sitemap, output_dir, project_name, site_config): sitemap 
            for sitemap in sitemaps
        }
        
        completed = 0
        for future in as_completed(futures):
            sitemap = futures[future]
            completed += 1
            
            try:
                result_tuple = future.result()
                results, sitemap_url, proj_name, scan_dt, scan_t, success, sitemap_status = result_tuple
                
                all_results.append(result_tuple)
                site_sitemap_statuses.append(sitemap_status)
                
                status_code_str = f"[{sitemap_status.status_code}]" if sitemap_status.status_code else ""
                success_status = f"✓ {sitemap_status.status} {status_code_str}"
                logger.info(f"[{completed}/{len(sitemaps)}] {success_status:<20}: {sitemap}")
                
                # Print progress
                print(f"[{completed}/{len(sitemaps)}] {sitemap_status.status}: {sitemap_url}")
                print(f"   URLs: {sitemap_status.urls_found}, Time: {sitemap_status.scan_time:.1f}s")
                
            except Exception as e:
                logger.error(f"[{completed}/{len(sitemaps)}] ✗ FAILED    : {sitemap} - {str(e)}")
                error_status = SitemapStatus(
                    url=sitemap,
                    status='FAILED',
                    status_code='Error',
                    urls_found=0,
                    error_message=str(e),
                    timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                )
                site_sitemap_statuses.append(error_status)
                all_results.append(([], sitemap, project_name, "", 0, False, error_status))
    
    return all_results, site_sitemap_statuses

def discover_sitemaps_for_site(site_config: SiteConfig) -> List[str]:
    """Discover sitemaps for a site using multiple methods"""
    discovered_sitemaps = []
    
    logger.info(f"\n🔍 Discovering sitemaps for {site_config.name}")
    
    # Start with configured sitemaps
    discovered_sitemaps.extend(site_config.sitemaps)
    
    # Try to discover sitemaps from robots.txt
    if site_config.sitemaps and len(site_config.sitemaps) > 0:
        first_sitemap = site_config.sitemaps[0]
        try:
            parsed = urlparse(first_sitemap)
            domain = f"{parsed.scheme}://{parsed.netloc}"
            robots_sitemaps = discover_sitemaps_from_robots(domain)
            discovered_sitemaps.extend(robots_sitemaps)
        except:
            pass
    
    # Remove duplicates while preserving order
    unique_sitemaps = []
    seen = set()
    for sitemap in discovered_sitemaps:
        if sitemap not in seen:
            seen.add(sitemap)
            unique_sitemaps.append(sitemap)
    
    logger.info(f"Discovered {len(unique_sitemaps)} unique sitemaps for {site_config.name}")
    
    return unique_sitemaps

def test_sitemap_connectivity_for_site(site_config: SiteConfig):
    """Test connectivity to all sitemaps for a site"""
    print(f"\n🧪 Testing connectivity for {site_config.name}...")
    
    working_sitemaps = []
    
    for sitemap_url in site_config.sitemaps[:5]:  # Test first 5
        print(f"\n  Testing: {sitemap_url}")
        results = test_sitemap_connectivity(sitemap_url)
        
        if results['direct']['status'] == 'Success' or results['with_browser_headers']['status'] == 'Success':
            working_sitemaps.append(sitemap_url)
            print(f"    ✅ Accessible")
        else:
            print(f"    ❌ Not accessible")
            print(f"    Direct: {results['direct']['error']}")
            print(f"    With headers: {results['with_browser_headers']['error']}")
    
    return working_sitemaps

def process_site(site: SiteConfig, global_start_time: float) -> tuple:
    """Process all sitemaps for a site"""
    print(f"\n{'='*80}")
    print(f"🌐 PROCESSING SITE: {site.name}")
    print(f"{'='*80}")
    
    os.makedirs(site.output_dir, exist_ok=True)
    
    logger.info(f"\n{'='*70}")
    logger.info(f"Starting site: {site.name}")
    logger.info(f"Total sitemaps: {len(site.sitemaps)}")
    if site.use_slower_rate:
        logger.info(f"⚠️  SLOWER RATE ENABLED for {site.name}")
        print(f"🐌 Slower crawling rate enabled for {site.name} to avoid blocking")
    logger.info(f"{'='*70}\n")
    
    # Discover additional sitemaps
    all_sitemaps = discover_sitemaps_for_site(site)
    
    if not all_sitemaps:
        logger.error(f"No sitemaps found for {site.name}")
        print(f"❌ No sitemaps found for {site.name}")
        return (site, calculate_stats([]), {'total': 0, 'success': 0, 'failed': 0}, None, 0.0, True, URL_CACHE.get_stats())
    
    print(f"📋 Found {len(all_sitemaps)} sitemaps to check")
    
    site_start_time = time.time()
    scan_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Load history BEFORE scanning to check if this is first scan
    history = load_scan_history()
    is_first_scan = site.name not in history or len(history[site.name]) == 0
    
    # Process sitemaps
    sitemap_results, site_sitemap_statuses = process_sitemap_batch(all_sitemaps, site.output_dir, site.name, site)
    site_execution_time = (time.time() - site_start_time) / 60
    
    # Print status table
    print_sitemap_status_table(site.name, site_sitemap_statuses)
    
    # Write error log if any failures
    error_log_path = write_error_log(site.name, site_sitemap_statuses, site.output_dir)
    
    # Save current scan to history
    save_current_scan(site.name, scan_date, sitemap_results, history)
    
    # Calculate statistics
    total_sitemaps = len(all_sitemaps)
    successful_sitemaps = sum(1 for status in site_sitemap_statuses if status.status == 'SUCCESS')
    failed_sitemaps = total_sitemaps - successful_sitemaps
    
    sitemap_summary = {
        'total': total_sitemaps,
        'success': successful_sitemaps,
        'failed': failed_sitemaps
    }
    
    all_page_results = []
    for results, _, _, _, _, _, sitemap_status in sitemap_results:
        if sitemap_status.status == 'SUCCESS':
            all_page_results.extend(results)
    
    stats = calculate_stats(all_page_results)
    
    # Get cache statistics
    cache_stats = URL_CACHE.get_stats()
    
    logger.info(f"\n{'='*70}")
    logger.info(f"Completed site: {site.name}")
    logger.info(f"Sitemaps: {successful_sitemaps}/{total_sitemaps} successful")
    logger.info(f"Pages checked: {stats['total_pages']:,}")
    logger.info(f"Broken links: {stats['broken_links']:,}")
    logger.info(f"Broken images: {stats['broken_images']:,}")
    logger.info(f"Pages with [noindex, nofollow]: {stats['noindex_nofollow_count']:,}")
    logger.info(f"Site Execution Time: {site_execution_time:.2f} minutes")
    logger.info(f"Cache Stats: {cache_stats['hits']:,} hits, {cache_stats['misses']:,} misses ({cache_stats['hit_rate']*100:.1f}% hit rate)")
    if error_log_path:
        logger.info(f"Error log: {error_log_path}")
    logger.info(f"{'='*70}\n")
    
    print(f"\n📊 SITE SUMMARY - {site.name}")
    print(f"   Sitemaps: {successful_sitemaps}/{total_sitemaps} successful")
    print(f"   Pages checked: {stats['total_pages']:,}")
    print(f"   Broken links: {stats['broken_links']:,}")
    print(f"   Broken images: {stats['broken_images']:,}")
    print(f"   Execution time: {site_execution_time:.2f} minutes")
    print(f"   Cache hit rate: {cache_stats['hit_rate']*100:.1f}%")
    print(f"{'='*80}\n")
    
    return (site, stats, sitemap_summary, error_log_path, site_execution_time, is_first_scan, cache_stats)

def process_all_sites(sites: List[SiteConfig], global_start_time: float) -> List[tuple]:
    """Process all sites SEQUENTIALLY to avoid resource exhaustion"""
    site_results = []
    
    for idx, site in enumerate(sites, 1):
        print(f"\n{'#'*80}")
        print(f"# Processing Site {idx}/{len(sites)}: {site.name}")
        print(f"{'#'*80}")
        
        try:
            # Test connectivity first
            working_sitemaps = test_sitemap_connectivity_for_site(site)
            
            if not working_sitemaps and site.sitemaps:
                print(f"⚠️  WARNING: No sitemaps accessible for {site.name}")
                print(f"   Trying with enhanced methods...")
            
            result = process_site(site, global_start_time)
            site_results.append(result)
            
            # Clear cache between sites to free memory
            URL_CACHE.clear()
            
            if idx < len(sites):
                logger.info(f"\nWaiting {INTER_SITE_DELAY} seconds before next site...")
                print(f"\n⏳ Waiting {INTER_SITE_DELAY} seconds before next site...")
                time.sleep(INTER_SITE_DELAY)
                
        except Exception as e:
            logger.error(f"Error processing site {site.name}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            site_results.append((
                site, 
                calculate_stats([]), 
                {'total': len(site.sitemaps), 'success': 0, 'failed': len(site.sitemaps)}, 
                None, 
                0.0, 
                False,
                URL_CACHE.get_stats()
            ))
    
    return site_results

def main():
    """Main execution with session pool initialization and cleanup"""
    global SITEMAP_STATUS_LOG
    SITEMAP_STATUS_LOG = []
    start_time = time.time()
    
    print("\n" + "="*80)
    print("🚀 BROKEN LINKS & IMAGES CHECKER - ENHANCED FOR ASIAN PAINTS")
    print("="*80)
    print("Optimized with URL caching and enhanced sitemap discovery")
    print("="*80)
    
    # Initialize session pool
    init_session_pool()
    
    try:
        print(f"\nStarting sequential processing of {len(SITES)} sites...")
        logger.info(f"\nStarting sequential processing of {len(SITES)} sites...")
        
        site_results = process_all_sites(SITES, start_time)
        
        print(f"\n{'='*80}")
        print("📧 SENDING REPORTS VIA EMAIL")
        print(f"{'='*80}")
        
        for site, stats, sitemap_summary, error_log_path, site_execution_time, is_first_scan, cache_stats in site_results:
            print(f"\n📤 Processing email for: {site.name}")
            logger.info(f"\n{'='*70}")
            logger.info(f"Post-processing {site.name}")
            logger.info(f"{'='*70}")
            
            try:
                # Check if we have any reports
                if not os.path.exists(site.output_dir) or not os.listdir(site.output_dir):
                    print(f"   ⚠️  No reports generated for {site.name}")
                    logger.warning(f"No reports generated for {site.name}")
                    continue
                
                # Create zip file
                zip_size_mb = create_zip(site.output_dir, site.zip_filename)
                
                if zip_size_mb > MAX_EMAIL_SIZE_MB:
                    print(f"   📦 ZIP too large ({zip_size_mb:.2f} MB > {MAX_EMAIL_SIZE_MB} MB). Splitting...")
                    logger.warning(f"ZIP too large ({zip_size_mb:.2f} MB > {MAX_EMAIL_SIZE_MB} MB). Splitting...")
                    
                    num_parts = math.ceil(zip_size_mb / MAX_EMAIL_SIZE_MB)
                    
                    if os.path.exists(site.zip_filename):
                        os.remove(site.zip_filename)
                        
                    split_zips = create_split_zips(site.output_dir, site.zip_filename, num_parts)
                    
                    for idx, zip_file in enumerate(split_zips, 1):
                        part_info = f"📧 This is PART {idx} of {len(split_zips)}"
                        body = generate_email_body(site, stats, site_execution_time, 
                                                os.path.basename(zip_file), sitemap_summary, part_info, 
                                                has_comparison=True, is_first_scan=is_first_scan,
                                                cache_stats=cache_stats)
                        subject = f"{site.name} Broken Links & Images Report - Part {idx}/{len(split_zips)}"
                        
                        print(f"   📤 Sending part {idx}/{len(split_zips)}...")
                        send_email(subject, body, site.recipients, zip_file)
                        
                        os.remove(zip_file)
                        
                        if idx < len(split_zips):
                            time.sleep(2)
                    
                    print(f"   ✅ Sent {len(split_zips)} separate emails for {site.name}")
                    logger.info(f"Sent {len(split_zips)} separate emails for {site.name}")
                    
                else:
                    body = generate_email_body(site, stats, site_execution_time, site.zip_filename, 
                                             sitemap_summary, has_comparison=True,
                                             is_first_scan=is_first_scan, cache_stats=cache_stats)
                    subject = f"{site.name} Broken Links & Images Report"
                    
                    print(f"   📤 Sending email...")
                    send_email(subject, body, site.recipients, site.zip_filename)
                    
                    if os.path.exists(site.zip_filename):
                        os.remove(site.zip_filename)
                    
                    print(f"   ✅ Email sent for {site.name}")
                
                # Clean up output directory
                shutil.rmtree(site.output_dir, ignore_errors=True)
                logger.info(f"Cleaned up directory: {site.output_dir}")
                
            except Exception as e:
                logger.error(f"Error in post-processing/email for {site.name}: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                print(f"   ❌ Error: {str(e)}")

    except KeyboardInterrupt:
        print("\n\n⏹️  Scan interrupted by user")
        logger.warning("Scan interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error in main: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        print(f"\n❌ Unexpected error: {str(e)}")
    finally:
        # Cleanup
        cleanup_session_pool()
        
        # Print final cache statistics
        final_cache_stats = URL_CACHE.get_stats()
        print(f"\n{'='*80}")
        print("📊 FINAL CACHE STATISTICS")
        print(f"{'='*80}")
        print(f"Total URLs cached: {final_cache_stats['size']:,}")
        print(f"Cache hits: {final_cache_stats['hits']:,}")
        print(f"Cache misses: {final_cache_stats['misses']:,}")
        print(f"Cache hit rate: {final_cache_stats['hit_rate']*100:.1f}%")
        print(f"Average checks per URL: {final_cache_stats['avg_checks_per_url']:.1f}")
        print(f"{'='*80}")
        
        # Clear cache
        URL_CACHE.clear()
        
        # Print final sitemap status summary
        if SITEMAP_STATUS_LOG:
            print("\n" + "="*120)
            print("📋 FINAL SITEMAP STATUS SUMMARY - ALL SITES")
            print("="*120)
            
            site_status_map = {}
            for site in SITES:
                site_statuses = [s for s in SITEMAP_STATUS_LOG if any(sitemap in s.url for sitemap in site.sitemaps)]
                if site_statuses:
                    site_status_map[site.name] = site_statuses
            
            for site_name, statuses in site_status_map.items():
                print_sitemap_status_table(site_name, statuses)

        total_time = (time.time() - start_time) / 60
        print(f"\n{'='*80}")
        print("✅ PROCESS COMPLETED!")
        print(f"{'='*80}")
        print(f"Total overall execution time: {total_time:.2f} minutes")
        print(f"Total sites processed: {len(SITES)}")
        print(f"{'='*80}\n")
        
        logger.info(f"\n{'='*70}")
        logger.info(f"PROCESS COMPLETED!")
        logger.info(f"Total overall execution time: {total_time:.2f} minutes")
        logger.info(f"{'='*70}")

if __name__ == "__main__":
    main()
