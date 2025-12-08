import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urljoin, parse_qs, urlencode,urlunparse
from dataclasses import dataclass, field
from typing import List, Dict, Set, Union
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
from datetime import datetime
import random
import math

# Suppress urllib3 warnings
warnings.filterwarnings('ignore', category=Warning)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Suppress urllib3 connection warnings
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)

# Configuration
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
    broken_links: List[LinkResult] = field(default_factory=list)
    broken_images: List[LinkResult] = field(default_factory=list)

# Constants
BROKEN_STATUS_CODES = {404, 400, 403, 500, 502, 503, "Timeout/Error", "Error"}
SKIP_SCHEMES = ('javascript:', 'mailto:', 'tel:', '#')
MAX_WORKERS = 8
MAX_RESOURCE_WORKERS = 12
REQUEST_TIMEOUT = 15
MAX_TEXT_LENGTH = 100
MAX_EMAIL_SIZE_MB = 15  # Split if larger than 15MB

# Enhanced User-Agent rotation to avoid 403 errors
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15'
]

# Email configuration
EMAIL_CONFIG = EmailConfig()

# Site configurations
SITES = [
    SiteConfig(
        name="BeautifulHomes",
        sitemaps=[
            "https://www.beautifulhomes.asianpaints.com/en.sitemap.blogs-sitemap.xml",
            "https://www.beautifulhomes.asianpaints.com/en.sitemap.interior-designs-sitemap.xml",
            "https://www.beautifulhomes.asianpaints.com/en.sitemap.store-locator-sitemap.xml",
            "https://www.beautifulhomes.asianpaints.com/en.sitemap.decor-products-sitemap.xml",
            "https://www.beautifulhomes.asianpaints.com/en.sitemap.magazine-sitemap.xml",
            "https://www.beautifulhomes.asianpaints.com/en.sitemap.web-stories-sitemap.xml",
            "https://www.beautifulhomes.asianpaints.com/en.sitemap.interior-design-ideas-sitemap.xml",
            "https://www.beautifulhomes.asianpaints.com/en.sitemap.xml",
        ],
        output_dir='BeautifulHomes_broken_links_reports',
        recipients=['arjun.kulkarni@asianpaints.com','khushali.shukla@deptagency.com','gaurang.kapadia@deptagency.com','ankita.singh@asianpaints.com','abhishek.sarma@asianpaints.com '],
        zip_filename='BEAUTIFULHOMES_Broken_Image_Link.zip'
    ),
    SiteConfig(
        name="AsianPaints",
        sitemaps=[
            "https://www.asianpaints.com/sitemap-main-shop.xml",
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
        ],
        output_dir='AsianPaints_broken_links_reports',
        recipients=['jhalak.mittal@asianpaints.com ','gaurang.kapadia@deptagency.com','silpamohapatra@kpmg.com','vasiurrahmangh@kpmg.com','arjun.kulkarni@asianpaints.com','ankita.singh@asianpaints.com'],
        zip_filename='ASIAN_PAINTS_Broken_Image_Link.zip'
    )
]

@contextmanager
def get_session():
    """Context manager for requests session with enhanced headers to avoid 403"""
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
        'Cache-Control': 'max-age=0',
    })
    
    retry_strategy = Retry(
        total=2,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    
    adapter = HTTPAdapter(
        pool_connections=100,
        pool_maxsize=100,
        max_retries=retry_strategy,
        pool_block=False
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    try:
        yield session
    finally:
        session.close()

def is_broken_status(status_code) -> bool:
    """Check if status code indicates a broken resource"""
    return status_code in BROKEN_STATUS_CODES

def check_url_status(url: str, session: requests.Session) -> Union[int, str]:
    """Check URL status with enhanced headers and retry logic"""
    try:
        time.sleep(random.uniform(0.1, 0.3))
        
        response = session.head(url, timeout=(5, 10), allow_redirects=True)
        status = response.status_code
        response.close()
        
        if status == 403:
            time.sleep(random.uniform(0.2, 0.5))
            response = session.get(url, timeout=(10, 60), allow_redirects=True, stream=True)
            status = response.status_code
            response.close()
        
        return status
    except (requests.exceptions.Timeout, requests.exceptions.ConnectTimeout):
        return "Timeout/Error"
    except requests.exceptions.RequestException:
        try:
            time.sleep(random.uniform(0.2, 0.5))
            response = session.get(url, timeout=(10, 60), allow_redirects=True, stream=True)
            status = response.status_code
            response.close()
            return status
        except (requests.exceptions.Timeout, requests.exceptions.ConnectTimeout):
            return "Timeout/Error"
        except:
            return "Error"

def normalize_url(base_url: str, href: str) -> Union[str, None]:
    """Normalize and validate URL"""
    if not href or href.startswith(SKIP_SCHEMES):
        return None
    
    try:
        parsed = urlparse(href)
        if not parsed.scheme:
            return urljoin(base_url, href)
        return href
    except:
        return None

def get_next_sibling_text(element, max_length: int = 200) -> str:
    """Get text content from the next sibling element"""
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
            try:
                status = future.result()
                if is_broken_status(status):
                    results.append(LinkResult(
                        url=resource[0],
                        status_code=status,
                        text=resource[1],
                        next_tag_data=resource[2] if len(resource) > 2 else ""
                    ))
            except Exception as e:
                logger.error(f"Error checking {resource[0]}: {str(e)}")
                results.append(LinkResult(
                    url=resource[0],
                    status_code="Error",
                    text=resource[1],
                    next_tag_data=resource[2] if len(resource) > 2 else ""
                ))
    
    return results

def process_url(url: str) -> PageResult:
    """Process single URL with improved error handling"""
    try:
        with get_session() as session:
            response = session.get(url, allow_redirects=True, timeout=REQUEST_TIMEOUT)
            response_code = response.status_code
            
            if response_code != 200:
                logger.warning(f"Non-200 response for {url}: {response_code}")
                response.close()
                return PageResult(url=url, response_code=response_code)
            
            soup = BeautifulSoup(response.content, "html.parser")
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
                broken_links=broken_links,
                broken_images=broken_images
            )
            
    except Exception as e:
        logger.error(f"Error processing {url}: {str(e)}")
        return PageResult(url=url, response_code="Error")

def fetch_sitemap_urls(sitemap_url: str) -> List[str]:
    """Fetch URLs from sitemap"""
    try:
        with get_session() as session:
            response = session.get(sitemap_url, timeout=REQUEST_TIMEOUT)
            soup = BeautifulSoup(response.text, 'xml')
            urls = [url.text for url in soup.find_all('loc')]
            response.close()
            updated_urls = []
            for url in urls:
                parsed = urlparse(url)

                # keep any existing query params
                query_params = parse_qs(parsed.query)

                # add your required param
                query_params["qaAutomation"] = ["true"]

                # rebuild query string
                new_query = urlencode(query_params, doseq=True)

                # reconstruct final URL
                updated_url = urlunparse((
                    parsed.scheme,
                    parsed.netloc,
                    parsed.path,
                    parsed.params,
                    new_query,
                    parsed.fragment
                ))

                updated_urls.append(updated_url)
            logger.info(f"Fetched {len(updated_urls)} URLs from sitemap")
            return updated_urls
    except Exception as e:
        logger.error(f"Error fetching sitemap {sitemap_url}: {str(e)}")
        return []

def process_sitemap(sitemap_url: str, output_dir: str, project_name: str) -> tuple:
    """Process sitemap with progress tracking and return (results, sitemap_url, project_name, scan_datetime, scan_time, success)"""
    logger.info(f"Processing sitemap: {sitemap_url}")
    
    scan_start_time = time.time()
    scan_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    urls = fetch_sitemap_urls(sitemap_url)
    if not urls:
        logger.warning(f"No URLs found in sitemap: {sitemap_url}")
        return ([], sitemap_url, project_name, scan_datetime, 0, False)  # False = failed
    
    logger.info(f"Found {len(urls)} URLs to check")
    
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_url, url): url for url in urls}
        
        for i, future in enumerate(as_completed(futures), 1):
            try:
                result = future.result()
                results.append(result)
                if i % 10 == 0:
                    logger.info(f"Processed {i}/{len(urls)} URLs")
            except Exception as e:
                logger.error(f"Error processing URL: {str(e)}")
    
    scan_time = (time.time() - scan_start_time) / 60
    save_report(results, sitemap_url, output_dir, project_name, scan_datetime, scan_time)
    
    return (results, sitemap_url, project_name, scan_datetime, scan_time, True)  # True = success

def calculate_stats(results: List[PageResult]) -> Dict:
    """Calculate summary statistics"""
    total_broken_links = sum(len(r.broken_links) for r in results)
    total_broken_images = sum(len(r.broken_images) for r in results)
    pages_with_broken_links = sum(1 for r in results if r.broken_links)
    pages_with_broken_images = sum(1 for r in results if r.broken_images)
    
    return {
        'total_pages': len(results),
        'broken_links': total_broken_links,
        'broken_images': total_broken_images,
        'pages_with_broken_links': pages_with_broken_links,
        'pages_with_broken_images': pages_with_broken_images
    }

def save_report(results: List[PageResult], sitemap_url: str, output_dir: str, 
                project_name: str, scan_datetime: str, scan_time: float):
    """Save comprehensive Excel report"""
    if not results:
        return
    
    stats = calculate_stats(results)
    domain_name = urlparse(sitemap_url).netloc + urlparse(sitemap_url).path.replace("/", "_")
    excel_path = os.path.join(output_dir, f'{domain_name}.xlsx')
    
    try:
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            pd.DataFrame({
                "Metric": [
                    "Project Name",
                    "Sitemap URL",
                    "Date & Time of Scan",
                    "Total Pages Checked",
                    "Pages with Broken Links",
                    "Pages with Broken Images",
                    "Total Broken Links (4xx, 5xx errors)",
                    "Total Broken Images (4xx, 5xx errors)",
                    "Total Time for Scan (minutes)"
                ],
                "Value": [
                    project_name,
                    sitemap_url,
                    scan_datetime,
                    stats['total_pages'],
                    stats['pages_with_broken_links'],
                    stats['pages_with_broken_images'],
                    stats['broken_links'],
                    stats['broken_images'],
                    f"{scan_time:.2f}"
                ]
            }).to_excel(writer, sheet_name='Summary Report', index=False)
            
            pd.DataFrame([
                {
                    "URL": r.url,
                    "Page Status Code": r.response_code,
                    "Broken Links Count": len(r.broken_links),
                    "Broken Images Count": len(r.broken_images)
                }
                for r in results
            ]).to_excel(writer, sheet_name='Pages Overview', index=False)
            
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
        
        logger.info(f"Report saved: {excel_path}")
        logger.info(f"Summary: {stats['broken_links']} broken links, {stats['broken_images']} broken images")
    except Exception as e:
        logger.error(f"Error saving report: {str(e)}")

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
    
    # Get file sizes
    file_sizes = []
    for f in excel_files:
        path = os.path.join(output_dir, f)
        size_mb = get_file_size_mb(path)
        file_sizes.append((f, size_mb))
    
    # Sort by size (largest first)
    file_sizes.sort(key=lambda x: x[1], reverse=True)
    
    # Initialize groups
    groups = [[] for _ in range(num_groups)]
    group_sizes = [0.0] * num_groups
    
    # Greedy allocation - put each file in the smallest group
    for filename, size in file_sizes:
        min_idx = group_sizes.index(min(group_sizes))
        groups[min_idx].append(filename)
        group_sizes[min_idx] += size
    
    return [g for g in groups if g]  # Remove empty groups

def create_split_zips(output_dir: str, base_zip_name: str, num_parts: int) -> List[str]:
    """Create multiple ZIP files by splitting Excel reports"""
    file_groups = split_files_into_groups(output_dir, num_parts)
    zip_files = []
    
    base_name = base_zip_name.replace('.zip', '')
    
    for idx, file_group in enumerate(file_groups, 1):
        zip_name = f"{base_name}_Part{idx}of{num_parts}.zip"
        
        with zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for filename in file_group:
                file_path = os.path.join(output_dir, filename)
                zipf.write(file_path, arcname=filename)
        
        size_mb = get_file_size_mb(zip_name)
        logger.info(f"Created split ZIP {idx}/{num_parts}: {zip_name} ({size_mb:.2f} MB, {len(file_group)} files)")
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
        logger.info(f"Email sent to {', '.join(recipients)}")
    except Exception as e:
        logger.error(f"Email failed: {str(e)}")

def generate_email_body(site: SiteConfig, stats: Dict, execution_time: float, 
                       zip_filename: str, sitemap_summary: Dict, part_info: str = "") -> str:
    """Generate email body with sitemap success count"""
    part_text = f"\n{part_info}\n" if part_info else ""
    
    return f"""Greetings, {site.name} Team.

Kindly review the outcomes of a recent Broken Links and Images check for the website.{part_text}
Report Summary:
- Total XML Sitemaps Analyzed: {sitemap_summary['total']}
- Successfully Processed Sitemaps: {sitemap_summary['success']}
- Failed Sitemaps: {sitemap_summary['failed']}
- Total Pages Checked: {stats['total_pages']}
- Total Broken Links Found: {stats['broken_links']}
- Total Broken Images Found: {stats['broken_images']}
- Execution Time: {execution_time:.2f} minutes

The report is attached as '{zip_filename}'.
The report contains:
  1. Summary Report - Overview with project name, sitemap URL, scan date/time, and metrics
  2. Pages Overview - List of all pages with issue counts
  3. Broken Links - Complete list of broken links with status codes
  4. Broken Images - Complete list of broken images with status codes and next tag content for 404 errors

Please feel free to review the attached parameters and let us know if you have any questions or concerns.

Thanks & Regards,
Q.A Automation Team,
DEPT® """

def process_sitemap_batch(sitemaps: List[str], output_dir: str, project_name: str) -> List[tuple]:
    """Process multiple sitemaps in parallel and track success/failure"""
    all_results = []
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(process_sitemap, sitemap, output_dir, project_name): sitemap 
            for sitemap in sitemaps
        }
        
        for future in as_completed(futures):
            sitemap = futures[future]
            try:
                result_tuple = future.result()
                all_results.append(result_tuple)
            except Exception as e:
                logger.error(f"Error processing sitemap {sitemap}: {str(e)}")
                # Add failed result
                all_results.append(([], sitemap, project_name, "", 0, False))
    
    return all_results

def process_site(site: SiteConfig) -> tuple:
    """Process all sitemaps for a site and return (stats, sitemap_summary)"""
    os.makedirs(site.output_dir, exist_ok=True)
    
    sitemap_results = process_sitemap_batch(site.sitemaps, site.output_dir, site.name)
    
    # Calculate sitemap success count
    total_sitemaps = len(site.sitemaps)
    successful_sitemaps = sum(1 for _, _, _, _, _, success in sitemap_results if success)
    failed_sitemaps = total_sitemaps - successful_sitemaps
    
    sitemap_summary = {
        'total': total_sitemaps,
        'success': successful_sitemaps,
        'failed': failed_sitemaps
    }
    
    # Combine all page results for overall stats
    all_page_results = []
    for results, _, _, _, _, success in sitemap_results:
        if success:  # Only include successful sitemaps
            all_page_results.extend(results)
    
    stats = calculate_stats(all_page_results)
    
    return (stats, sitemap_summary)

def process_all_sites(sites: List[SiteConfig]) -> List[tuple]:
    """Process all sites in parallel"""
    site_results = []
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(process_site, site): site 
            for site in sites
        }
        
        for future in as_completed(futures):
            site = futures[future]
            try:
                stats, sitemap_summary = future.result()
                site_results.append((site, stats, sitemap_summary))
            except Exception as e:
                logger.error(f"Error processing site {site.name}: {str(e)}")
    
    return site_results

def main():
    """Main execution with ZIP splitting if needed"""
    start_time = time.time()
    
    logger.info(f"Starting parallel processing of {len(SITES)} sites...")
    site_results = process_all_sites(SITES)
    
    for site, stats, sitemap_summary in site_results:
        logger.info(f"\n{'='*60}")
        logger.info(f"Post-processing {site.name}")
        logger.info(f"Sitemap Summary: {sitemap_summary['success']}/{sitemap_summary['total']} successful")
        logger.info(f"{'='*60}")
        
        try:
            # Create initial ZIP
            zip_size_mb = create_zip(site.output_dir, site.zip_filename)
            
            execution_time = (time.time() - start_time) / 60
            
            # Check if ZIP needs to be split
            if zip_size_mb > MAX_EMAIL_SIZE_MB:
                logger.warning(f"ZIP too large ({zip_size_mb:.2f} MB > {MAX_EMAIL_SIZE_MB} MB). Splitting into parts...")
                
                # Calculate number of parts needed
                num_parts = math.ceil(zip_size_mb / MAX_EMAIL_SIZE_MB)
                
                # Delete the large ZIP
                os.remove(site.zip_filename)
                
                # Create split ZIPs
                split_zips = create_split_zips(site.output_dir, site.zip_filename, num_parts)
                
                # Send separate email for each part
                for idx, zip_file in enumerate(split_zips, 1):
                    part_info = f"📧 This is PART {idx} of {len(split_zips)}"
                    body = generate_email_body(site, stats, execution_time, 
                                             os.path.basename(zip_file), sitemap_summary, part_info)
                    subject = f"{site.name} Broken Links & Images Report - Part {idx}/{len(split_zips)}"
                    send_email(subject, body, site.recipients, zip_file)
                    
                    # Small delay between emails
                    if idx < len(split_zips):
                        time.sleep(2)
                
                logger.info(f"Sent {len(split_zips)} separate emails for {site.name}")
                
            else:
                # ZIP size is OK, send single email
                body = generate_email_body(site, stats, execution_time, site.zip_filename, sitemap_summary)
                subject = f"{site.name} Broken Links & Images Report"
                send_email(subject, body, site.recipients, site.zip_filename)
            
            # Cleanup
            shutil.rmtree(site.output_dir)
            logger.info(f"Cleaned up directory: {site.output_dir}")
            
        except Exception as e:
            logger.error(f"Error in post-processing for {site.name}: {str(e)}")
    
    total_time = (time.time() - start_time) / 60
    logger.info(f"\nProcess completed!")
    logger.info(f"Total execution time: {total_time:.2f} minutes")

if __name__ == "__main__":
    main()