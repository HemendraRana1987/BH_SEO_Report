import subprocess
import socket
import requests
url = "https://www.asianpaints.com/sitemap-main-shop.xml"
domain = "www.asianpaints.com"
print("=== Diagnostic Report ===\n")
# 1. DNS Check
try:
    ip = socket.gethostbyname(domain)
    print(f":white_tick: DNS Resolution: {domain} -> {ip}")
except Exception as e:
    print(f":x: DNS Failed: {e}")
# 2. HTTP Request Check
try:
    resp = requests.get(url, timeout=10)
    print(f":white_tick: HTTP Status: {resp.status_code}")
except Exception as e:
    print(f":x: HTTP Failed: {type(e).__name__} - {e}")
# 3. Environment Variables
import os
print(f"\n:clipboard: HTTP_PROXY: {os.environ.get('HTTP_PROXY', 'Not Set')}")
print(f":clipboard: HTTPS_PROXY: {os.environ.get('HTTPS_PROXY', 'Not Set')}")
