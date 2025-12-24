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
class SiteHistoricalData:
    """Store site-level historical data"""
    site_name: str
    scan_date: str
    sitemap_data: List[SitemapHistoricalData]
    url: str
    status: str
    status_code: Union[int, str] = ""
    urls_found: int = 0
    error_message: str = ""
    timestamp: str = ""
    scan_time: float = 0.0

# --- Constants ---

BROKEN_STATUS_CODES = {404, 400, 403, 500, 502, 503, "Timeout/Error", "Error"}
SKIP_SCHEMES = ('javascript:', 'mailto:', 'tel:', '#')
MAX_WORKERS = 4
MAX_RESOURCE_WORKERS = 6
SITEMAP_WORKERS = 2
REQUEST_TIMEOUT = 30
SITEMAP_TIMEOUT = 45
MAX_TEXT_LENGTH = 100
MAX_EMAIL_SIZE_MB = 15
SESSION_POOL_SIZE = 20
HISTORY_DIR = 'scan_history'
HISTORY_FILE = 'scan_history.json'

# Enhanced User-Agent rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15'
]

# Add delay between requests to avoid rate limiting
INTER_REQUEST_DELAY = 0.5
INTER_SITE_DELAY = 10

# --- Global State ---

EMAIL_CONFIG = EmailConfig()
SITES = [
    SiteConfig(
        name="AsianPaints",
        sitemaps=[
            # Add sitemap URLs here
        ],
        output_dir='AsianPaints_broken_links_reports',
        recipients=["Bhuwan.pandey@deptagency.com"],
        zip_filename='ASIAN_PAINTS_Broken_Image_Link.zip'
    ),
    SiteConfig(
        name="BeautifulHomes",
        sitemaps=["https://www.beautifulhomes.asianpaints.com/en.sitemap.blogs-sitemap.xml",
            "https://www.beautifulhomes.asianpaints.com/en.sitemap.interior-designs-sitemap.xml",
            "https://www.beautifulhomes.asianpaints.com/en.sitemap.store-locator-sitemap.xml",
            "https://www.beautifulhomes.asianpaints.com/en.sitemap.decor-products-sitemap.xml",
            "https://www.beautifulhomes.asianpaints.com/en.sitemap.magazine-sitemap.xml",
            "https://www.beautifulhomes.asianpaints.com/en.sitemap.web-stories-sitemap.xml",
            "https://www.beautifulhomes.asianpaints.com/en.sitemap.interior-design-ideas-sitemap.xml",
            "https://www.beautifulhomes.asianpaints.com/en.sitemap.xml"],
        output_dir='BeautifulHomes_broken_links_reports',
        recipients=["Bhuwan.pandey@deptagency.com"],
        zip_filename='BEAUTIFULHOMES_Broken_Image_Link.zip'
    )
]

# Global session pool
SESSION_POOL = Queue(maxsize=SESSION_POOL_SIZE)
SESSION_LOCK = threading.Lock()
SITEMAP_STATUS_LOG = []

# --- Session Management Functions ---

def init_session_pool():
    """Initialize session pool for connection reuse"""
    logger.info(f"Initializing session pool with {SESSION_POOL_SIZE} sessions...")
    for _ in range(SESSION_POOL_SIZE):
        session = requests.Session()
        
        session.headers.update({
            'User-Agent': random.choice(USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
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
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        
        adapter = HTTPAdapter(
            pool_connections=50,
            pool_maxsize=50,
            max_retries=retry_strategy,
            pool_block=False
        )
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        SESSION_POOL.put(session)
    logger.info("Session pool initialized successfully")
    
    try:
        import socket
        hostname = socket.gethostname()
        ip_address = socket.gethostbyname(hostname)
        logger.info(f"Server hostname: {hostname}")
        logger.info(f"Server IP: {ip_address}")
        print(f"🖥️  Server IP: {ip_address}")
    except:
        logger.warning("Could not determine server IP")

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

def get_site_level_previous_data(site_name: str) -> Tuple[Optional[Dict], str]:
    """Get aggregated previous scan data for entire site"""
    history = load_scan_history()
    
    if site_name not in history or len(history[site_name]) == 0:
        return None, ""
    
    previous_scan = history[site_name][-1]
    previous_date = previous_scan['scan_date']
    
    # Aggregate data from all sitemaps in previous scan
    aggregated_stats = {
        'total_pages': 0,
        'pages_with_broken_links': 0,
        'pages_with_broken_images': 0,
        'total_broken_links': 0,
        'total_broken_images': 0,
        'noindex_nofollow_count': 0
    }
    
    for sitemap_data in previous_scan.get('sitemap_data', []):
        aggregated_stats['total_pages'] += sitemap_data['total_pages']
        aggregated_stats['pages_with_broken_links'] += sitemap_data['pages_with_broken_links']
        aggregated_stats['pages_with_broken_images'] += sitemap_data['pages_with_broken_images']
        aggregated_stats['total_broken_links'] += sitemap_data['total_broken_links']
        aggregated_stats['total_broken_images'] += sitemap_data['total_broken_images']
        aggregated_stats['noindex_nofollow_count'] += sitemap_data['noindex_nofollow_count']
    
    return aggregated_stats, previous_date

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
    return status_code in BROKEN_STATUS_CODES

def check_url_status(url: str, session: requests.Session, max_retries: int = 3) -> Union[int, str]:
    """Check URL status with exponential backoff and HEAD/GET fallback"""
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                logger.debug(f"Retry {attempt} for {url}, waiting {wait_time:.2f}s")
                time.sleep(wait_time)
            
            time.sleep(random.uniform(0.1, 0.3))
            
            response = session.head(url, timeout=(10, 20), allow_redirects=True)
            status = response.status_code
            response.close()
            
            if status in [403, 405] or status >= 500:
                time.sleep(random.uniform(0.2, 0.5))
                response = session.get(url, timeout=(15, 30), allow_redirects=True, stream=True)
                status = response.status_code
                response.close()
            
            return status
            
        except (requests.exceptions.Timeout, requests.exceptions.ConnectTimeout):
            if attempt == max_retries - 1:
                logger.debug(f"Timeout for {url} after {max_retries} attempts")
                return "Timeout/Error"
            continue
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                logger.debug(f"Error for {url}: {str(e)}")
                return "Error"
            continue
    
    return "Error"

def normalize_url(base_url: str, href: str) -> Union[str, None]:
    """Normalize and validate URL"""
    if not href or href.startswith(SKIP_SCHEMES):
        return None
    
    try:
        if href.startswith('//'):
            return 'https:' + href
        
        parsed = urlparse(href)
        if not parsed.scheme:
            normalized = urljoin(base_url, href)
            return normalized
        
        return href
    except:
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
    """Check multiple resources in parallel - only returns broken resources"""
    results = []
    
    with ThreadPoolExecutor(max_workers=MAX_RESOURCE_WORKERS) as executor:
        futures = {
            executor.submit(check_url_status, resource[0], session): resource 
            for resource in resources
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
                results.append(LinkResult(
                    url=url,
                    status_code="Error",
                    text=text,
                    next_tag_data=next_tag_data
                ))
    
    return results

def process_url(url: str) -> PageResult:
    """Process single URL: fetch, parse, check robots meta, and check links/images"""
    try:
        with get_session() as session:
            response = session.get(url, allow_redirects=True, timeout=REQUEST_TIMEOUT)
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
            
            broken_links = check_resource_batch(links_to_check, session) if links_to_check else []
            broken_images = check_resource_batch(images_to_check, session) if images_to_check else []
            
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

def fetch_sitemap_urls(sitemap_url: str, site_config: SiteConfig = None) -> tuple:
    """Fetch URLs from sitemap, inject 'qaAutomation' parameter, and return status object"""
    status = SitemapStatus(
        url=sitemap_url,
        status='FAILED',
        urls_found=0,
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )
    
    try:
        with get_session() as session:
            logger.info(f"Fetching sitemap: {sitemap_url}")
            start_time = time.time()
            
            response = session.get(sitemap_url, timeout=SITEMAP_TIMEOUT)
            status_code = response.status_code
            status.status_code = status_code
            
            print(f"📋 Sitemap Status Code: [{status_code}] - {sitemap_url}")
            logger.info(f"Sitemap returned status code: {status_code}")
            
            if status_code != 200:
                status.error_message = f"HTTP {status_code}"
                logger.error(f"✗ Sitemap returned status {status_code}: {sitemap_url}")
                SITEMAP_STATUS_LOG.append(status)
                return ([], status)
            
            soup = BeautifulSoup(response.text, 'xml')
            urls = [url.text for url in soup.find_all('loc')]
            response.close()
            
            if not urls:
                status.status = 'EMPTY'
                status.error_message = "No URLs found in sitemap"
                logger.warning(f"⚠ Sitemap is empty: {sitemap_url}")
                SITEMAP_STATUS_LOG.append(status)
                return ([], status)
            
            updated_urls = []
            for url in urls:
                parsed = urlparse(url)
                query_params = parse_qs(parsed.query)
                query_params["qaAutomation"] = ["true"]
                new_query = urlencode(query_params, doseq=True)
                updated_url = urlunparse((
                    parsed.scheme,
                    parsed.netloc,
                    parsed.path,
                    parsed.params,
                    new_query,
                    parsed.fragment
                ))
                updated_urls.append(updated_url)
            
            status.status = 'SUCCESS'
            status.urls_found = len(updated_urls)
            status.scan_time = time.time() - start_time
            
            logger.info(f"✓ Fetched {len(updated_urls)} URLs from sitemap: {sitemap_url}")
            SITEMAP_STATUS_LOG.append(status)
            return (updated_urls, status)
            
    except requests.exceptions.Timeout:
        status.status_code = "Timeout"
        status.error_message = f"Timeout after {SITEMAP_TIMEOUT}s"
        logger.error(f"✗ Timeout fetching sitemap: {sitemap_url}")
        SITEMAP_STATUS_LOG.append(status)
        return ([], status)
    except Exception as e:
        status.status_code = "Error"
        status.error_message = str(e)
        logger.error(f"✗ Error fetching sitemap {sitemap_url}: {str(e)}")
        SITEMAP_STATUS_LOG.append(status)
        return ([], status)

def process_sitemap(sitemap_url: str, output_dir: str, project_name: str, site_config: SiteConfig = None) -> tuple:
    """Process sitemap with progress tracking and report saving"""
    logger.info(f"Processing sitemap: {sitemap_url}")
    
    scan_start_time = time.time()
    scan_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    urls, sitemap_status = fetch_sitemap_urls(sitemap_url, site_config)
    
    if not urls:
        logger.warning(f"No URLs to process for sitemap: {sitemap_url}")
        return ([], sitemap_url, project_name, scan_datetime, 0, False, sitemap_status)
    
    logger.info(f"Found {len(urls)} URLs to check")
    
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_url, url): url for url in urls}
        
        for i, future in enumerate(as_completed(futures), 1):
            try:
                result = future.result()
                results.append(result)
                if i % 10 == 0 or i == len(urls):
                    logger.info(f"Progress: {i}/{len(urls)} URLs processed ({(i/len(urls)*100):.1f}%)")
            except Exception as e:
                logger.error(f"Error processing URL: {str(e)}")
    
    scan_time = (time.time() - scan_start_time) / 60
    save_report(results, sitemap_url, output_dir, project_name, scan_datetime, scan_time)
    
    logger.info(f"✓ Completed sitemap in {scan_time:.2f} minutes")
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
        'Change/Difference': f"{pages_change} ({'+' if pages_change > 0 else ''}{pages_change})",
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
        'Change/Difference': f"{links_change} ({'+' if links_change > 0 else ''}{links_change})",
        'Trend': get_trend_emoji(links_change, reverse=True)  # Reverse because fewer is better
    })
    
    # Broken Images comparison
    current_images = current_stats['broken_images']
    previous_images = previous_data['total_broken_images'] if previous_data else 0
    images_change = current_images - previous_images
    comparison_data.append({
        'Metric': 'Total Broken Images',
        'Current Value': current_images,
        'Previous Value': previous_images if previous_data else 'N/A',
        'Change/Difference': f"{images_change} ({'+' if images_change > 0 else ''}{images_change})",
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
        'Change/Difference': f"{pages_links_change} ({'+' if pages_links_change > 0 else ''}{pages_links_change})",
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
        'Change/Difference': f"{pages_images_change} ({'+' if pages_images_change > 0 else ''}{pages_images_change})",
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
        'Change/Difference': f"{noindex_change} ({'+' if noindex_change > 0 else ''}{noindex_change})",
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
        return
    
    # Get current stats
    current_stats = calculate_stats(results)
    
    # Get previous data for this sitemap
    previous_data, previous_date, previous_time = get_previous_sitemap_data(sitemap_url, project_name)
    
    # Create comparison data
    comparison_data = create_comparison_sheet_data(current_stats, previous_data, scan_datetime, previous_date)
    
    domain_name = urlparse(sitemap_url).netloc + urlparse(sitemap_url).path.replace("/", "_").replace(".", "-")
    excel_path = os.path.join(output_dir, f'{domain_name}.xlsx')
    
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
            df_comparison = pd.DataFrame(comparison_data)
            df_comparison.to_excel(writer, sheet_name='Scan Comparison', index=False)
            
            # Pages Overview
            pd.DataFrame([
                {
                    "URL": r.url,
                    "Page Status Code": r.response_code,
                    "Robots Meta Tag": r.robots_meta if r.robots_meta else "Not Found",
                    "Broken Links Count": len(r.broken_links),
                    "Broken Images Count": len(r.broken_images)
                }
                for r in results
            ]).to_excel(writer, sheet_name='Pages Overview', index=False)
            
            # Broken Links
            broken_links = [
                {
                    "Page URL": r.url,
                    "Link URL": link.url,
                    "Status Code": link.status_code,
                    "Link Text": link.text
                }
                for r in results for link in r.broken_links
            ]
            if broken_links:
                pd.DataFrame(broken_links).to_excel(writer, sheet_name='Broken Links', index=False)
            
            # Broken Images
            broken_images = [
                {
                    "Page URL": r.url,
                    "Image URL": img.url,
                    "Status Code": img.status_code,
                    "Alt Text": img.text,
                    "Next Tag Content": img.next_tag_data if img.status_code == 404 else ""
                }
                for r in results for img in r.broken_images
            ]
            if broken_images:
                pd.DataFrame(broken_images).to_excel(writer, sheet_name='Broken Images', index=False)
            
            # Format the comparison sheet
            workbook = writer.book
            worksheet = writer.sheets['Scan Comparison']
            
            # Set column widths
            for i, col in enumerate(df_comparison.columns):
                column_width = max(df_comparison[col].astype(str).map(len).max(), len(col)) + 2
                worksheet.column_dimensions[chr(65 + i)].width = min(column_width, 30)
            
            # Add header note
            worksheet.cell(row=1, column=1, value=f"Scan Comparison Report - {project_name}")
            worksheet.insert_rows(1)
            worksheet.cell(row=1, column=1, value=f"Generated: {scan_datetime}")
            worksheet.cell(row=2, column=1, value=f"Sitemap: {sitemap_url}")
            
            if previous_data:
                worksheet.cell(row=3, column=1, value=f"Comparing: {previous_date} → {scan_datetime}")
            else:
                worksheet.cell(row=3, column=1, value="First Scan - No previous data for comparison")
        
        logger.info(f"Report saved with separate comparison sheet: {excel_path}")
    except Exception as e:
        logger.error(f"Error saving report: {str(e)}")

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
            f.write(f"Success Rate: {((len(sitemap_statuses)-len(failed_statuses))/len(sitemap_statuses)*100):.1f}%\n\n")
            
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
    return os.path.getsize(file_path) / (1024 * 1024)

def create_zip(output_dir: str, zip_path: str) -> float:
    """Create zip file and return size in MB"""
    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(output_dir):
                for file in files:
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
        with open(attachment_path, 'rb') as f:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(attachment_path)}"')
            msg.attach(part)
        
        file_size = get_file_size_mb(attachment_path)
        logger.info(f"Attachment: {os.path.basename(attachment_path)} ({file_size:.2f} MB)")
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
                       has_comparison: bool = False, is_first_scan: bool = False) -> str:
    """Generate email body with comparison info"""
    part_text = f"\n{part_info}\n" if part_info else ""
    
    if is_first_scan:
        comparison_text = "\n\n📊 NOTE: This is the FIRST SCAN for this site. Baseline data has been recorded. Comparison reports will be available from the next scan onwards."
    elif has_comparison:
        comparison_text = "\n\n📊 COMPARISON REPORT INCLUDED: Each sitemap report now contains a separate 'Scan Comparison' sheet showing detailed comparison with the previous scan including date/time differences and trend analysis."
    else:
        comparison_text = ""
    
    return f"""Greetings, {site.name} Team.

Kindly review the outcomes of a recent Broken Links and Images check for the website.{part_text}

Report Summary:
- Total XML Sitemaps Analyzed: {sitemap_summary['total']}
- Successfully Processed Sitemaps: {sitemap_summary['success']}
- Failed Sitemaps (Empty/Error): {sitemap_summary['failed']}
- Total Pages Checked: {stats['total_pages']}
- Total Broken Links Found: {stats['broken_links']}
- Total Broken Images Found: {stats['broken_images']}
- Pages with [noindex, nofollow]: {stats['noindex_nofollow_count']}
- Total Scan Execution Time (all sitemaps for site): {execution_time:.2f} minutes{comparison_text}

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

def process_site(site: SiteConfig, global_start_time: float) -> tuple:
    """Process all sitemaps for a site"""
    os.makedirs(site.output_dir, exist_ok=True)
    
    logger.info(f"\n{'='*70}")
    logger.info(f"Starting site: {site.name}")
    logger.info(f"Total sitemaps: {len(site.sitemaps)}")
    if site.use_slower_rate:
        logger.info(f"⚠️  SLOWER RATE ENABLED for {site.name}")
        print(f"🐌 Slower crawling rate enabled for {site.name} to avoid blocking")
    logger.info(f"{'='*70}\n")
    
    site_start_time = time.time()
    scan_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Load history BEFORE scanning to check if this is first scan
    history = load_scan_history()
    is_first_scan = site.name not in history or len(history[site.name]) == 0
    
    sitemap_results, site_sitemap_statuses = process_sitemap_batch(site.sitemaps, site.output_dir, site.name, site)
    site_execution_time = (time.time() - site_start_time) / 60
    
    print_sitemap_status_table(site.name, site_sitemap_statuses)
    
    error_log_path = write_error_log(site.name, site_sitemap_statuses, site.output_dir)
    
    # Save current scan to history
    save_current_scan(site.name, scan_date, sitemap_results, history)
    
    total_sitemaps = len(site.sitemaps)
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
    
    logger.info(f"\n{'='*70}")
    logger.info(f"Completed site: {site.name}")
    logger.info(f"Sitemaps: {successful_sitemaps}/{total_sitemaps} successful")
    logger.info(f"Pages checked: {stats['total_pages']}")
    logger.info(f"Broken links: {stats['broken_links']}")
    logger.info(f"Broken images: {stats['broken_images']}")
    logger.info(f"Pages with [noindex, nofollow]: {stats['noindex_nofollow_count']}")
    logger.info(f"Site Execution Time: {site_execution_time:.2f} minutes")
    if error_log_path:
        logger.info(f"Error log: {error_log_path}")
    logger.info(f"{'='*70}\n")
    
    return (site, stats, sitemap_summary, error_log_path, site_execution_time, is_first_scan)

def process_all_sites(sites: List[SiteConfig], global_start_time: float) -> List[tuple]:
    """Process all sites SEQUENTIALLY to avoid resource exhaustion"""
    site_results = []
    
    for idx, site in enumerate(sites, 1):
        logger.info(f"\n{'#'*70}")
        logger.info(f"# Processing Site {idx}/{len(sites)}: {site.name}")
        logger.info(f"{'#'*70}")
        
        try:
            result = process_site(site, global_start_time)
            site_results.append(result)
            
            if idx < len(sites):
                logger.info(f"\nWaiting 10 seconds before next site...")
                time.sleep(10)
                
        except Exception as e:
            logger.error(f"Error processing site {site.name}: {str(e)}")
            site_results.append((
                site, 
                calculate_stats([]), 
                {'total': len(site.sitemaps), 'success': 0, 'failed': len(site.sitemaps)}, 
                None, 
                0.0, 
                False  # is_first_scan
            ))
    
    return site_results

def main():
    """Main execution with session pool initialization and cleanup"""
    global SITEMAP_STATUS_LOG
    SITEMAP_STATUS_LOG = []
    start_time = time.time()
    
    logger.info("="*70)
    logger.info("BROKEN LINKS & IMAGES CHECKER - SERVER OPTIMIZED")
    logger.info("="*70)
    
    init_session_pool()
    
    try:
        logger.info(f"\nStarting sequential processing of {len(SITES)} sites...")
        site_results = process_all_sites(SITES, start_time)
        
        for site, stats, sitemap_summary, error_log_path, site_execution_time, is_first_scan in site_results:
            logger.info(f"\n{'='*70}")
            logger.info(f"Post-processing {site.name}")
            logger.info(f"{'='*70}")
            
            try:
                zip_size_mb = create_zip(site.output_dir, site.zip_filename)
                
                if zip_size_mb > MAX_EMAIL_SIZE_MB:
                    logger.warning(f"ZIP too large ({zip_size_mb:.2f} MB > {MAX_EMAIL_SIZE_MB} MB). Splitting...")
                    
                    num_parts = math.ceil(zip_size_mb / MAX_EMAIL_SIZE_MB)
                    
                    if os.path.exists(site.zip_filename):
                        os.remove(site.zip_filename)
                        
                    split_zips = create_split_zips(site.output_dir, site.zip_filename, num_parts)
                    
                    for idx, zip_file in enumerate(split_zips, 1):
                        part_info = f"📧 This is PART {idx} of {len(split_zips)}"
                        body = generate_email_body(site, stats, site_execution_time, 
                                                os.path.basename(zip_file), sitemap_summary, part_info, 
                                                has_comparison=True, is_first_scan=is_first_scan)
                        subject = f"{site.name} Broken Links & Images Report - Part {idx}/{len(split_zips)}"
                        send_email(subject, body, site.recipients, zip_file)
                        
                        os.remove(zip_file)
                        
                        if idx < len(split_zips):
                            time.sleep(2)
                    
                    logger.info(f"Sent {len(split_zips)} separate emails for {site.name}")
                    
                else:
                    body = generate_email_body(site, stats, site_execution_time, site.zip_filename, 
                                             sitemap_summary, has_comparison=True,
                                             is_first_scan=is_first_scan)
                    subject = f"{site.name} Broken Links & Images Report"
                    send_email(subject, body, site.recipients, site.zip_filename)
                    os.remove(site.zip_filename)
                
                shutil.rmtree(site.output_dir, ignore_errors=True)
                logger.info(f"Cleaned up directory: {site.output_dir}")
                
            except Exception as e:
                logger.error(f"Error in post-processing/email for {site.name}: {str(e)}")

    finally:
        cleanup_session_pool()
        
        if SITEMAP_STATUS_LOG:
            print("\n" + "="*120)
            print("FINAL SITEMAP STATUS SUMMARY - ALL SITES")
            print("="*120)
            
            site_status_map = {}
            for site in SITES:
                site_statuses = [s for s in SITEMAP_STATUS_LOG if any(sitemap in s.url for sitemap in site.sitemaps)]
                if site_statuses:
                    site_status_map[site.name] = site_statuses
            
            for site_name, statuses in site_status_map.items():
                print_sitemap_status_table(site_name, statuses)

        total_time = (time.time() - start_time) / 60
        logger.info(f"\n{'='*70}")
        logger.info(f"PROCESS COMPLETED!")
        logger.info(f"Total overall execution time: {total_time:.2f} minutes")
        logger.info(f"{'='*70}")

if __name__ == "__main__":
    main()
