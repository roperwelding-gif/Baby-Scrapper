# improved_career_scraper.py
"""
An improved polite web scraper for job listings with dynamic selector fallbacks.
Supports both static pages (BeautifulSoup) and JavaScript-rendered pages (Selenium).
"""
import csv
import logging
import time
import urllib.robotparser
from random import uniform
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

# Configuration constants
MAX_JOBS_PER_SITE = 200
REQUEST_TIMEOUT = 15
MAX_RETRIES = 3
RETRY_BACKOFF_FACTOR = 1
MIN_DELAY_SECONDS = 1.5
MAX_DELAY_SECONDS = 3.5
SELENIUM_WAIT_TIMEOUT = 15  # Reduced from 20 - we'll use multiple strategies instead
OUTPUT_CSV = "jobs_scraped.csv"

# Generic fallback selectors that work across many job sites
FALLBACK_JOB_SELECTORS = [
    # Most specific selectors first
    "[data-automation-id='jobPostingItem']",
    "[data-job-id]",
    "[data-qa='job-list-item']",
    "[data-testid='job-card']",
    ".job-listing",
    ".job-item",
    ".job-card",
    ".job-opening",
    ".opening",
    ".position",
    ".vacancy",
    ".career-opportunity",
    # More generic selectors
    "div[class*='job-']",
    "div[class*='Job']",
    "article[class*='job']",
    "li[class*='job']",
    "div[class*='posting']",
    "div[class*='opportunity']",
    "div[class*='career']",
    # Very generic - use as last resort
    "[role='listitem']",
    "tbody tr",  # For table-based layouts
]

FALLBACK_TITLE_SELECTORS = [
    "[data-automation-id='jobTitle']",
    "[data-qa='job-title']",
    "[data-testid='job-title']",
    ".job-title",
    ".title",
    ".position-title",
    ".opening-title",
    "h2 a",
    "h3 a",
    "h4 a",
    "h2",
    "h3",
    "strong",
    "[class*='title']",
    "[class*='Title']",
]

FALLBACK_LOCATION_SELECTORS = [
    "[data-automation-id='location']",
    "[data-automation-id='locations']",
    "[data-qa='job-location']",
    "[data-testid='job-location']",
    ".job-location",
    ".location",
    ".office",
    ".city",
    "[class*='location']",
    "[class*='Location']",
    "span[class*='loc']",
    "div[class*='loc']",
]

# HTTP headers - more complete browser simulation
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Session with retries (for non-Selenium requests)
session = requests.Session()
retries = Retry(
    total=MAX_RETRIES,
    backoff_factor=RETRY_BACKOFF_FACTOR,
    status_forcelist=(429, 500, 502, 503, 504)
)
session.mount("https://", HTTPAdapter(max_retries=retries))
session.headers.update(HEADERS)

# Simplified site configurations - we'll use fallbacks for selectors
SITES = [
    {
        "name": "Strategy Software (MicroStrategy)",
        "api_url": "https://api.smartrecruiters.com/v1/companies/MicroStrategy1/postings",
        "use_api": True,
    },
    {
        "name": "Blockchain Association Network",
        "jobs_url": "https://jobs.theblockchainassociation.org/jobs",
        "use_selenium": True,
        "company": "Blockchain Association",
    },
    {
        "name": "Chenega Corporation",
        "jobs_url": "https://careers.chenega.com/chenega-careers/jobs",
        "use_selenium": True,
        "company": "Chenega Corporation",
    },
    {
        "name": "Comscore Inc",
        "jobs_url": "https://www.comscore.com/About/Careers/Job-Opportunities",
        "use_selenium": True,
        "company": "Comscore Inc",
    },
    {
        "name": "GMAC (Graduate Management Admission Council)",
        "jobs_url": "https://gmac.wd1.myworkdayjobs.com/GMAC",
        "use_selenium": True,
        "company": "GMAC",
    },
    {
        "name": "Guidehouse",
        "jobs_url": "https://guidehouse.wd1.myworkdayjobs.com/External",
        "use_selenium": True,
        "company": "Guidehouse",
    },
    {
        "name": "MyEyeDr",
        "jobs_url": "https://careers.myeyedr.com/search/jobs",
        "use_selenium": True,
        "company": "MyEyeDr",
    },
    {
        "name": "Rocket Mortgage",
        "jobs_url": "https://www.myrocketcareer.com/careers",
        "use_selenium": True,
        "company": "Rocket Mortgage",
    },
    {
        "name": "Sol Systems",
        "jobs_url": "https://www.solsystems.com/careers",
        "use_selenium": True,
        "company": "Sol Systems",
    },
    {
        "name": "Stand Together",
        "jobs_url": "https://standtogether.org/careers/job-listings",
        "use_selenium": True,
        "company": "Stand Together",
    },
    {
        "name": "ThompsonGas",
        "jobs_url": "https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?cid=9a8366f5-a16e-4d31-9a6b-584b65942873&ccId=19000101_000001&type=MP&lang=en_US",
        "use_selenium": True,
        "company": "ThompsonGas",
    },
    {
        "name": "Xometry",
        "jobs_url": "https://job-boards.greenhouse.io/xometry",
        "use_selenium": True,
        "company": "Xometry",
    },
]


def get_selenium_driver() -> webdriver.Chrome:
    """
    Create and configure a Selenium Chrome WebDriver instance.
    
    Returns:
        Configured Chrome WebDriver
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in background
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument(f"user-agent={HEADERS['User-Agent']}")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    driver = webdriver.Chrome(options=chrome_options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver


def allowed_to_scrape(base_url: str, path: str = "/") -> bool:
    """
    Check if scraping is allowed according to the site's robots.txt.
    
    Args:
        base_url: The base URL of the site to check
        path: The specific path to check (default: "/")
    
    Returns:
        True if scraping is allowed, False otherwise
    """
    rp = urllib.robotparser.RobotFileParser()
    parsed = urlparse(base_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    
    try:
        rp.set_url(robots_url)
        rp.read()
        can_fetch = rp.can_fetch("*", urljoin(base_url, path))
        if not can_fetch:
            logger.warning(f"robots.txt forbids fetching {path}")
        return can_fetch
    except Exception as e:
        logger.warning(f"Could not fetch or parse robots.txt from {robots_url}: {e}")
        # If we can't fetch robots.txt, default to allowing (common practice)
        return True


def find_elements_with_fallback(driver: webdriver.Chrome, selectors: List[str], timeout: int = 5) -> Tuple[List, str]:
    """
    Try multiple selectors to find elements, with shorter timeout for each.
    
    Args:
        driver: Selenium WebDriver instance
        selectors: List of CSS selectors to try
        timeout: Timeout for each selector attempt
    
    Returns:
        Tuple of (elements found, selector that worked)
    """
    for selector in selectors:
        try:
            # Use a shorter timeout for each individual selector
            elements = WebDriverWait(driver, timeout).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, selector))
            )
            if elements:
                logger.info(f"Found {len(elements)} elements with selector: {selector}")
                return elements, selector
        except TimeoutException:
            continue
    
    # If no selectors work, return empty list
    logger.warning("No elements found with any selector")
    return [], None


def extract_text_from_element(element, selectors: List[str]) -> Optional[str]:
    """
    Try multiple selectors to extract text from an element.
    
    Args:
        element: Selenium WebElement or BeautifulSoup element
        selectors: List of CSS selectors to try
    
    Returns:
        Extracted text or None
    """
    for selector in selectors:
        try:
            # Handle both Selenium and BeautifulSoup elements
            if hasattr(element, 'find_element'):
                # Selenium
                sub_el = element.find_element(By.CSS_SELECTOR, selector)
                text = sub_el.text.strip()
                if text:
                    return text
            else:
                # BeautifulSoup
                sub_el = element.select_one(selector)
                if sub_el:
                    text = sub_el.get_text(strip=True)
                    if text:
                        return text
        except Exception:
            continue
    
    # Fallback: get all text from the element
    try:
        if hasattr(element, 'text'):
            return element.text.strip()
        else:
            return element.get_text(strip=True)
    except:
        return None


def scrape_with_selenium(site_conf: Dict, max_jobs: int = MAX_JOBS_PER_SITE) -> List[Dict[str, Optional[str]]]:
    """
    Scrape job listings using Selenium with dynamic fallback selectors.
    
    Args:
        site_conf: Dictionary containing site configuration
        max_jobs: Maximum number of jobs to scrape
    
    Returns:
        List of job dictionaries
    """
    jobs = []
    driver = None
    jobs_url = site_conf["jobs_url"]
    
    try:
        logger.info(f"[SELENIUM] Loading {jobs_url}")
        driver = get_selenium_driver()
        driver.get(jobs_url)
        
        # Allow some time for initial page load
        time.sleep(3)
        
        # Try to find job cards with various selectors
        cards, working_selector = find_elements_with_fallback(driver, FALLBACK_JOB_SELECTORS, timeout=5)
        
        if not cards:
            # If no cards found, save page source for debugging
            logger.warning(f"No job cards found for {site_conf['name']}")
            with open(f"debug_{site_conf['name'].replace(' ', '_')}.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            logger.info(f"Page source saved for debugging")
            return jobs
        
        logger.info(f"Processing {len(cards)} job cards")
        
        for card in cards[:max_jobs]:
            # Extract title with fallback selectors
            title = extract_text_from_element(card, FALLBACK_TITLE_SELECTORS)
            
            # Extract location with fallback selectors
            location = extract_text_from_element(card, FALLBACK_LOCATION_SELECTORS)
            
            # Try to find a link
            href = None
            try:
                link = card.find_element(By.TAG_NAME, "a")
                href = link.get_attribute("href")
            except:
                # If no direct link, use the jobs page URL
                href = jobs_url
            
            company = site_conf.get("company") or site_conf["name"]
            
            if title:  # Only add if we at least have a title
                jobs.append({
                    "company": company,
                    "title": title,
                    "location": location or "Not specified",
                    "url": href or jobs_url,
                })
        
        logger.info(f"Successfully extracted {len(jobs)} jobs from {site_conf['name']}")
        
    except WebDriverException as e:
        logger.error(f"Selenium error for {site_conf['name']}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error for {site_conf['name']}: {e}")
    finally:
        if driver:
            driver.quit()
    
    return jobs


def scrape_with_requests(site_conf: Dict, max_jobs: int = MAX_JOBS_PER_SITE) -> List[Dict[str, Optional[str]]]:
    """
    Scrape job listings using requests/BeautifulSoup with dynamic fallback selectors.
    
    Args:
        site_conf: Dictionary containing site configuration
        max_jobs: Maximum number of jobs to scrape
    
    Returns:
        List of job dictionaries
    """
    jobs = []
    jobs_url = site_conf["jobs_url"]
    
    logger.info(f"[REQUESTS] Fetching {jobs_url}")
    
    try:
        resp = session.get(jobs_url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Failed to fetch page: {e}")
        return jobs

    soup = BeautifulSoup(resp.text, "html.parser")
    
    # Try multiple selectors to find job cards
    cards = []
    working_selector = None
    for selector in FALLBACK_JOB_SELECTORS:
        cards = soup.select(selector)
        if cards:
            working_selector = selector
            logger.info(f"Found {len(cards)} job cards with selector: {selector}")
            break
    
    if not cards:
        logger.warning(f"No jobs found for {site_conf['name']}")
        return jobs
    
    for card in cards[:max_jobs]:
        # Extract title with fallback selectors
        title = None
        for selector in FALLBACK_TITLE_SELECTORS:
            title_el = card.select_one(selector)
            if title_el:
                title = title_el.get_text(strip=True)
                if title:
                    break
        
        # Extract location with fallback selectors
        location = None
        for selector in FALLBACK_LOCATION_SELECTORS:
            location_el = card.select_one(selector)
            if location_el:
                location = location_el.get_text(strip=True)
                if location:
                    break
        
        # Try to find a link
        link_el = card.select_one("a")
        href = link_el.get("href") if link_el else None
        if href:
            href = urljoin(jobs_url, href)
        
        company = site_conf.get("company") or site_conf["name"]

        if title:  # Only add if we at least have a title
            jobs.append({
                "company": company,
                "title": title,
                "location": location or "Not specified",
                "url": href or jobs_url,
            })

    logger.info(f"Successfully extracted {len(jobs)} jobs from {site_conf['name']}")
    return jobs


def scrape_with_api(site_conf: Dict, max_jobs: int = MAX_JOBS_PER_SITE) -> List[Dict[str, Optional[str]]]:
    """
    Scrape job listings using a JSON API.
    
    Args:
        site_conf: Dictionary containing site configuration with api_url
        max_jobs: Maximum number of jobs to scrape
    
    Returns:
        List of job dictionaries
    """
    jobs = []
    api_url = site_conf["api_url"]
    
    logger.info(f"[API] Fetching {api_url}")
    
    try:
        resp = session.get(api_url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        
        # SmartRecruiters API structure
        job_postings = data.get("content", [])
        logger.info(f"Found {len(job_postings)} jobs from API")
        
        for job in job_postings[:max_jobs]:
            location_obj = job.get("location", {})
            location_parts = []
            if location_obj.get("city"):
                location_parts.append(location_obj["city"])
            if location_obj.get("region"):
                location_parts.append(location_obj["region"])
            if location_obj.get("country"):
                location_parts.append(location_obj["country"])
            
            location_str = ", ".join(location_parts) if location_parts else "Not specified"
            
            jobs.append({
                "company": site_conf["name"],
                "title": job.get("name", "Unknown Title"),
                "location": location_str,
                "url": job.get("ref"),
            })
            
    except requests.RequestException as e:
        logger.error(f"Failed to fetch API: {e}")
    except (KeyError, ValueError) as e:
        logger.error(f"Failed to parse API response: {e}")
    
    return jobs


def scrape_site(site_conf: Dict, max_jobs: int = MAX_JOBS_PER_SITE) -> List[Dict[str, Optional[str]]]:
    """
    Scrape job listings from a single site (routes to appropriate scraper).
    
    Args:
        site_conf: Dictionary containing site configuration
        max_jobs: Maximum number of jobs to scrape
    
    Returns:
        List of job dictionaries with keys: company, title, location, url
    """
    # Check robots.txt for non-API methods
    if not site_conf.get("use_api", False):
        jobs_url = site_conf["jobs_url"]
        parsed = urlparse(jobs_url)
        
        if not allowed_to_scrape(f"{parsed.scheme}://{parsed.netloc}", parsed.path):
            logger.warning(f"[SKIP] robots.txt forbids scraping {jobs_url}")
            return []

    logger.info(f"[SCRAPING] {site_conf['name']}")
    
    # Choose scraping method based on config
    if site_conf.get("use_api", False):
        jobs = scrape_with_api(site_conf, max_jobs)
    elif site_conf.get("use_selenium", False):
        jobs = scrape_with_selenium(site_conf, max_jobs)
    else:
        jobs = scrape_with_requests(site_conf, max_jobs)
    
    # Polite delay between sites
    time.sleep(uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS))
    return jobs


def save_to_csv(rows: List[Dict], filename: str = OUTPUT_CSV) -> None:
    """
    Save job listings to a CSV file.
    
    Args:
        rows: List of job dictionaries to save
        filename: Output CSV filename (default: OUTPUT_CSV)
    """
    keys = ["company", "title", "location", "url"]
    
    try:
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(rows)
        logger.info(f"Saved {len(rows)} rows to {filename}")
    except IOError as e:
        logger.error(f"Failed to save CSV: {e}")


def main() -> None:
    """
    Main function to scrape all configured sites and save results.
    """
    all_jobs = []
    successful_sites = 0
    failed_sites = []
    
    for site in SITES:
        try:
            rows = scrape_site(site)
            if rows:
                all_jobs.extend(rows)
                successful_sites += 1
            else:
                failed_sites.append(site.get('name', 'Unknown'))
        except Exception as e:
            logger.error(f"Error scraping site {site.get('name', 'Unknown')}: {e}")
            failed_sites.append(site.get('name', 'Unknown'))
    
    # Summary
    logger.info(f"\n{'='*50}")
    logger.info(f"SCRAPING COMPLETE")
    logger.info(f"Successful sites: {successful_sites}/{len(SITES)}")
    if failed_sites:
        logger.info(f"Failed sites: {', '.join(failed_sites)}")
    logger.info(f"Total jobs collected: {len(all_jobs)}")
    logger.info(f"{'='*50}\n")
    
    save_to_csv(all_jobs)


if __name__ == "__main__":
    main()