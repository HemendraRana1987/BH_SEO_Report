import subprocess
import socket
import requests
import os
from datetime import datetime
url = "https://www.asianpaints.com/sitemap-main-shop.xml"
domain = "www.asianpaints.com"
# Create output file with timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_file = f"diagnostic_report_{timestamp}.txt"
# Open file for writing
with open(output_file, 'w', encoding='utf-8') as f:
    f.write("=== Diagnostic Report ===\n")
    f.write(f"Timestamp: {datetime.now()}\n")
    f.write(f"URL: {url}\n")
    f.write("=" * 50 + "\n\n")
    print("=== Diagnostic Report ===\n")
    # 1. DNS Check
    try:
        ip = socket.gethostbyname(domain)
        dns_result = f":white_tick: DNS Resolution: {domain} -> {ip}"
    except Exception as e:
        dns_result = f":x: DNS Failed: {e}"
    print(dns_result)
    f.write(dns_result + "\n\n")
    # 2. HTTP Request Check
    try:
        resp = requests.get(url, timeout=30)
        http_result = f":white_tick: HTTP Status: {resp.status_code}"
        print(http_result)
        f.write(http_result + "\n\n")
        # Write response headers
        f.write("=== Response Headers ===\n")
        for header, value in resp.headers.items():
            f.write(f"{header}: {value}\n")
        f.write("\n")
        # Write response content
        f.write("=== Response Content ===\n")
        f.write(resp.text)
        f.write("\n\n")
    except Exception as e:
        http_result = f":x: HTTP Failed: {type(e).__name__} - {e}"
        print(http_result)
        f.write(http_result + "\n\n")
    # 3. Environment Variables
    proxy_info = f"""
=== Environment Variables ===
HTTP_PROXY: {os.environ.get('HTTP_PROXY', 'Not Set')}
HTTPS_PROXY: {os.environ.get('HTTPS_PROXY', 'Not Set')}
http_proxy: {os.environ.get('http_proxy', 'Not Set')}
https_proxy: {os.environ.get('https_proxy', 'Not Set')}
NO_PROXY: {os.environ.get('NO_PROXY', 'Not Set')}
"""
    print(proxy_info)
    f.write(proxy_info)
print(f"\n:file_folder: Report saved to: {output_file}")










