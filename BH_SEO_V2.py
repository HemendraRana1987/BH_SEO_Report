import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urljoin
from dataclasses import dataclass, field
from typing import List, Dict, Set
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
    status_code: int | str
    text: str = ""

@dataclass
class PageResult:
    url: str
    response_code: int | str
    broken_links: List[LinkResult] = field(default_factory=list)
    broken_images: List[LinkResult] = field(default_factory=list)

# Constants
BROKEN_STATUS_CODES = {404, 400, 403, 500, 502, 503, "Timeout/Error", "Error"}
SKIP_SCHEMES = ('javascript:', 'mailto:', 'tel:', '#')
MAX_WORKERS = 8
MAX_RESOURCE_WORKERS = 12
REQUEST_TIMEOUT = 15
MAX_TEXT_LENGTH = 100

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

# Site configurations - FIXED zip filenames (removed forward slashes)
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
    "https://www.beautifulhomes.asianpaints.com/en.sitemap.xml"
        ],
        output_dir='BeautifulHomes_broken_links_reports',
       # recipients=['bhuwan.pandey@deptagency.com','gaurang.kapadia@deptagency.com','sudha.murthy@deptagency.com'],
        recipients=['arjun.kulkarni@asianpaints.com','abhishek.sarma@asianpaints.com','ankita.singh@asianpaints.com ','khushali.shukla@deptagency.com','gaurang.kapadia@deptagency.com','sudha.murthy@deptagency.com'],
        zip_filename='BEAUTIFULHOMES_Broken_Image_Link.zip'  # FIXED: removed forward slash
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
    "https://www.asianpaints.com/sitemap-web-stories.xml"],
        output_dir='AsianPaints_broken_links_reports',
        #recipients=['bhuwan.pandey@deptagency.com','gaurang.kapadia@deptagency.com','sudha.murthy@deptagency.com'],
        recipients=['arjun.kulkarni@asianpaints.com','ankita.singh@asianpaints.com ','jhalak.mittal@asianpaints.com','silpamohapatra@kpmg.com ','vasiurrahmangh@kpmg.com','khushali.shukla@deptagency.com','gaurang.kapadia@deptagency.com','sudha.murthy@deptagency.com'],
        zip_filename='ASIAN_PAINTS_Broken_Image_Link.zip'  # FIXED: removed forward slash
    )
]

@contextmanager
def get_session():
    """Context manager for requests session with enhanced headers to avoid 403"""
    session = requests.Session()
    
    # Enhanced headers to appear more like a real browser
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
        total=2,  # Increased retries
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

def check_url_status(url: str, session: requests.Session) -> int | str:
    """Check URL status with enhanced headers and retry logic"""
    try:
        # Add small random delay to avoid rate limiting
        time.sleep(random.uniform(0.1, 0.3))
        
        # Try HEAD request first
        response = session.head(
            url, 
            timeout=(5, 10),
            allow_redirects=True
        )
        status = response.status_code
        response.close()
        
        # If HEAD returns 403, try GET (some servers block HEAD requests)
        if status == 403:
            time.sleep(random.uniform(0.2, 0.5))
            response = session.get(
                url, 
                timeout=(5, 10),
                allow_redirects=True,
                stream=True
            )
            status = response.status_code
            response.close()
        
        return status
    except (requests.exceptions.Timeout, requests.exceptions.ConnectTimeout):
        return "Timeout/Error"
    except requests.exceptions.RequestException:
        try:
            # Fallback to GET request
            time.sleep(random.uniform(0.2, 0.5))
            response = session.get(
                url, 
                timeout=(10, 60),
                allow_redirects=True, 
                stream=True
            )
            status = response.status_code
            response.close()
            return status
        except (requests.exceptions.Timeout, requests.exceptions.ConnectTimeout):
            return "Timeout/Error"
        except:
            return "Error"

def normalize_url(base_url: str, href: str) -> str | None:
    """Normalize and validate URL"""
    if not href or href.startswith(SKIP_SCHEMES):
        return None
    
    parsed = urlparse(href)
    if not parsed.scheme:
        return urljoin(base_url, href)
    return href

def check_resource_batch(resources: List[tuple], session: requests.Session) -> List[LinkResult]:
    """Check multiple resources in parallel with connection management"""
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
                results.append(LinkResult(
                    url=resource[0],
                    status_code=status,
                    text=resource[1]
                ))
            except Exception as e:
                logger.error(f"Error checking {resource[0]}: {str(e)}")
                results.append(LinkResult(
                    url=resource[0],
                    status_code="Error",
                    text=resource[1]
                ))
    
    return results

def process_url(url: str) -> PageResult:
    """Process single URL with improved error handling and parallel resource checking"""
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
            
            # Collect unique links
            links_to_check = []
            checked_urls: Set[str] = set()
            
            for link in soup.find_all("a", href=True):
                link_url = normalize_url(url, link.get("href"))
                if link_url and link_url not in checked_urls:
                    checked_urls.add(link_url)
                    link_text = link.get_text().strip()[:MAX_TEXT_LENGTH] or "No Text"
                    links_to_check.append((link_url, link_text))
            
            # Collect unique images
            images_to_check = []
            checked_images: Set[str] = set()
            
            for img in soup.find_all("img", src=True):
                img_url = normalize_url(url, img.get("src"))
                if img_url and img_url not in checked_images:
                    checked_images.add(img_url)
                    alt_text = img.get("alt", "No Alt Text")[:MAX_TEXT_LENGTH]
                    images_to_check.append((img_url, alt_text))
            
            # Check all resources in parallel with the same session
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
    """Fetch URLs from sitemap with enhanced headers"""
    try:
        with get_session() as session:
            response = session.get(sitemap_url, timeout=REQUEST_TIMEOUT)
            soup = BeautifulSoup(response.text, 'xml')
            urls = [url.text for url in soup.find_all('loc')]
            response.close()
            return urls
    except Exception as e:
        logger.error(f"Error fetching sitemap {sitemap_url}: {str(e)}")
        return []

def process_sitemap(sitemap_url: str, output_dir: str, project_name: str) -> tuple:
    """Process sitemap with progress tracking and return results with metadata"""
    logger.info(f"Processing sitemap: {sitemap_url}")
    
    scan_start_time = time.time()
    scan_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    urls = fetch_sitemap_urls(sitemap_url)
    if not urls:
        return ([], sitemap_url, project_name, scan_datetime, 0)
    
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
    
    return (results, sitemap_url, project_name, scan_datetime, scan_time)

def calculate_stats(results: List[PageResult]) -> Dict:
    """Calculate summary statistics"""
    total_broken_links = sum(
        sum(1 for link in r.broken_links if is_broken_status(link.status_code))
        for r in results
    )
    total_broken_images = sum(
        sum(1 for img in r.broken_images if is_broken_status(img.status_code))
        for r in results
    )
    pages_with_broken_links = sum(
        1 for r in results 
        if any(is_broken_status(link.status_code) for link in r.broken_links)
    )
    pages_with_broken_images = sum(
        1 for r in results 
        if any(is_broken_status(img.status_code) for img in r.broken_images)
    )
    
    return {
        'total_pages': len(results),
        'broken_links': total_broken_links,
        'broken_images': total_broken_images,
        'pages_with_broken_links': pages_with_broken_links,
        'pages_with_broken_images': pages_with_broken_images
    }

def save_report(results: List[PageResult], sitemap_url: str, output_dir: str, 
                project_name: str, scan_datetime: str, scan_time: float):
    """Save comprehensive Excel report with enhanced summary"""
    if not results:
        return
    
    stats = calculate_stats(results)
    domain_name = urlparse(sitemap_url).netloc + urlparse(sitemap_url).path.replace("/", "_")
    excel_path = os.path.join(output_dir, f'{domain_name}.xlsx')
    
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        # Enhanced Summary sheet
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
        
        # Pages overview
        pd.DataFrame([
            {
                "URL": r.url,
                "Page Status Code": r.response_code,
                "Broken Links Count": sum(1 for link in r.broken_links if is_broken_status(link.status_code)),
                "Broken Images Count": sum(1 for img in r.broken_images if is_broken_status(img.status_code))
            }
            for r in results
        ]).to_excel(writer, sheet_name='Pages Overview', index=False)
        
        # All links details
        all_links = [
            {
                "Page URL": r.url,
                "Link URL": link.url,
                "Status Code": link.status_code,
                "Link Text": link.text,
                "Status": "Broken" if is_broken_status(link.status_code) else "Working"
            }
            for r in results for link in r.broken_links
        ]
        if all_links:
            pd.DataFrame(all_links).to_excel(writer, sheet_name='All Links Details', index=False)
        
        # All images details
        all_images = [
            {
                "Page URL": r.url,
                "Image URL": img.url,
                "Status Code": img.status_code,
                "Alt Text": img.text,
                "Status": "Broken" if is_broken_status(img.status_code) else "Working"
            }
            for r in results for img in r.broken_images
        ]
        if all_images:
            pd.DataFrame(all_images).to_excel(writer, sheet_name='All Images Details', index=False)
    
    logger.info(f"Report saved: {excel_path}")
    logger.info(f"Summary: {stats['broken_links']} broken links, {stats['broken_images']} broken images")

def create_zip(output_dir: str, zip_path: str):
    """Create zip file from directory"""
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(output_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, output_dir)
                zipf.write(file_path, arcname=arcname)
    logger.info(f"Created zip: {zip_path}")

def send_email(subject: str, body: str, recipients: List[str], attachment_path: str):
    """Send email with attachment"""
    msg = MIMEMultipart()
    msg['From'] = EMAIL_CONFIG.sender
    msg['To'] = ', '.join(recipients)
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))
    
    with open(attachment_path, 'rb') as f:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(attachment_path)}"')
        msg.attach(part)
    
    try:
        with smtplib.SMTP(EMAIL_CONFIG.smtp_server, EMAIL_CONFIG.smtp_port) as server:
            server.starttls()
            server.login(EMAIL_CONFIG.username, EMAIL_CONFIG.password)
            server.sendmail(EMAIL_CONFIG.sender, recipients, msg.as_string())
        logger.info(f"Email sent to {', '.join(recipients)}")
    except Exception as e:
        logger.error(f"Email failed: {str(e)}")

def generate_email_body(site: SiteConfig, stats: Dict, execution_time: float) -> str:
    """Generate email body"""
    return f"""Greetings, {site.name} Team.

Kindly review the outcomes of a recent Broken Links and Images check for the website.

Report Summary:
- Total XML Sitemaps Analyzed: {len([s for s in site.sitemaps if s.endswith('.xml')])}
- Total Pages Checked: {stats['total_pages']}
- Total Broken Links Found: {stats['broken_links']}
- Total Broken Images Found: {stats['broken_images']}
- Execution Time: {execution_time:.2f} minutes

The detailed report is attached in a ZIP folder named '{site.zip_filename}'.
The report contains:
  1. Summary Report - Overview with project name, sitemap URL, scan date/time, and metrics
  2. Pages Overview - List of all pages with issue counts
  3. All Links Details - Complete list of all links with status codes
  4. All Images Details - Complete list of all images with status codes

Please feel free to review the attached parameters and let us know if you have any questions or concerns.

Thanks & Regards,
Q.A Automation Team,
DEPT® """

def process_sitemap_batch(sitemaps: List[str], output_dir: str, project_name: str) -> List[tuple]:
    """Process multiple sitemaps in parallel and return metadata"""
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
    
    return all_results

def process_site(site: SiteConfig) -> Dict:
    """Process all sitemaps for a site in parallel"""
    os.makedirs(site.output_dir, exist_ok=True)
    
    # Process all sitemaps in parallel with metadata
    sitemap_results = process_sitemap_batch(site.sitemaps, site.output_dir, site.name)
    
    # Combine all page results for overall stats
    all_page_results = []
    for results, _, _, _, _ in sitemap_results:
        all_page_results.extend(results)
    
    return calculate_stats(all_page_results)

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
                stats = future.result()
                site_results.append((site, stats))
            except Exception as e:
                logger.error(f"Error processing site {site.name}: {str(e)}")
    
    return site_results

def main():
    """Main execution with full parallelization"""
    start_time = time.time()
    
    # Process all sites in parallel
    logger.info(f"Starting parallel processing of {len(SITES)} sites...")
    site_results = process_all_sites(SITES)
    
    # Handle post-processing sequentially (zipping, emailing, cleanup)
    for site, stats in site_results:
        logger.info(f"\n{'='*60}")
        logger.info(f"Post-processing {site.name}")
        logger.info(f"{'='*60}")
        
        # Create zip
        create_zip(site.output_dir, site.zip_filename)
        
        # Send email
        execution_time = (time.time() - start_time) / 60
        body = generate_email_body(site, stats, execution_time)
        subject = f"{site.name} Broken Links & Images Report"
        send_email(subject, body, site.recipients, site.zip_filename)
        
        # Cleanup
        shutil.rmtree(site.output_dir)
    
    total_time = (time.time() - start_time) / 60
    logger.info(f"\nProcess completed!")
    logger.info(f"Total execution time: {total_time:.2f} minutes")

if __name__ == "__main__":
    main()