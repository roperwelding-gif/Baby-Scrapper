# complete_career_scraper.py
"""
Comprehensive career scraper for multiple companies in the DC area and beyond.
Handles different ATS systems (Workday, Greenhouse, SmartRecruiters, Lever, etc.)
"""
import csv
import logging
import time
import urllib.robotparser
import re
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
SELENIUM_WAIT_TIMEOUT = 15
OUTPUT_CSV = "all_companies_jobs.csv"

# HTTP headers
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

# Session with retries
session = requests.Session()
retries = Retry(
    total=MAX_RETRIES,
    backoff_factor=RETRY_BACKOFF_FACTOR,
    status_forcelist=(429, 500, 502, 503, 504)
)
session.mount("https://", HTTPAdapter(max_retries=retries))
session.headers.update(HEADERS)

# Complete list of companies with their career page configurations
SITES = [
    # Original companies
    {
        "name": "MicroStrategy",
        "api_url": "https://api.smartrecruiters.com/v1/companies/MicroStrategy1/postings",
        "use_api": True,
        "company": "MicroStrategy"
    },
    {
        "name": "Blockchain Association",
        "jobs_url": "https://jobs.theblockchainassociation.org/jobs",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Blockchain Association",
    },
    {
        "name": "Chenega Corporation",
        "jobs_url": "https://careers.chenega.com/chenega-careers/jobs",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Chenega Corporation",
    },
    {
        "name": "Comscore Inc",
        "jobs_url": "https://www.comscore.com/About/Careers/Job-Opportunities",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Comscore Inc",
    },
    {
        "name": "GMAC",
        "jobs_url": "https://gmac.wd1.myworkdayjobs.com/GMAC",
        "use_selenium": True,
        "ats_type": "workday",
        "company": "GMAC",
    },
    {
        "name": "Guidehouse",
        "jobs_url": "https://guidehouse.wd1.myworkdayjobs.com/External",
        "use_selenium": True,
        "ats_type": "workday",
        "company": "Guidehouse",
    },
    {
        "name": "MyEyeDr",
        "jobs_url": "https://careers.myeyedr.com/search/jobs",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "MyEyeDr",
    },
    {
        "name": "Rocket Mortgage",
        "jobs_url": "https://www.myrocketcareer.com/careers",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Rocket Mortgage",
    },
    {
        "name": "Sol Systems",
        "jobs_url": "https://www.solsystems.com/careers",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Sol Systems",
    },
    {
        "name": "Stand Together",
        "jobs_url": "https://standtogether.org/careers/job-listings",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Stand Together",
    },
    {
        "name": "ThompsonGas",
        "jobs_url": "https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?cid=9a8366f5-a16e-4d31-9a6b-584b65942873&ccId=19000101_000001&type=MP&lang=en_US",
        "use_selenium": True,
        "ats_type": "adp",
        "company": "ThompsonGas",
    },
    {
        "name": "Xometry",
        "jobs_url": "https://job-boards.greenhouse.io/xometry",
        "use_selenium": True,
        "ats_type": "greenhouse",
        "company": "Xometry",
    },
    
    # New companies added
    {
        "name": "Akiak Technology LLC",
        "jobs_url": "https://www.akiak.com/careers",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Akiak Technology LLC",
    },
    {
        "name": "BerkleyNet",
        "jobs_url": "https://careers-berkleynet.icims.com/jobs/intro",
        "use_selenium": True,
        "ats_type": "icims",
        "company": "BerkleyNet",
    },
    {
        "name": "BigBear.ai",
        "jobs_url": "https://bigbear.ai/careers/",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "BigBear.ai",
    },
    {
        "name": "Black Canyon Consulting",
        "jobs_url": "https://www.blackcanyonconsulting.com/careers",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Black Canyon Consulting",
    },
    {
        "name": "CapTech",
        "jobs_url": "https://www.captechconsulting.com/careers/job-search",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "CapTech",
    },
    {
        "name": "Citian",
        "jobs_url": "https://careers.citian.com/",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Citian",
    },
    {
        "name": "Consumer Technology Association",
        "jobs_url": "https://www.cta.tech/Who-We-Are/CTA-Careers",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Consumer Technology Association",
    },
    {
        "name": "Elder Research",
        "jobs_url": "https://www.elderresearch.com/company/careers/",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Elder Research",
    },
    {
        "name": "GEICO",
        "jobs_url": "https://geico.wd1.myworkdayjobs.com/External",
        "use_selenium": True,
        "ats_type": "workday",
        "company": "GEICO",
    },
    {
        "name": "Higher Logic",
        "jobs_url": "https://www.higherlogic.com/about-us/careers/",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Higher Logic",
    },
    {
        "name": "HiLabs",
        "jobs_url": "https://hilabs.com/careers/",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "HiLabs",
    },
    {
        "name": "Hypergiant",
        "jobs_url": "https://www.hypergiant.com/careers/",
        "use_selenium": True,
        "ats_type": "lever",
        "company": "Hypergiant",
    },
    {
        "name": "KlariVis",
        "jobs_url": "https://www.klarivis.com/careers",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "KlariVis",
    },
    {
        "name": "Servos",
        "jobs_url": "https://www.servos.com/careers",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Servos",
    },
    {
        "name": "Millennial Software",
        "jobs_url": "https://millennialsoftware.com/careers/",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Millennial Software",
    },
    {
        "name": "Softcrylic",
        "jobs_url": "https://softcrylic.com/careers/",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Softcrylic",
    },
    {
        "name": "Weber Shandwick",
        "jobs_url": "https://www.webershandwick.com/careers/",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Weber Shandwick",
    },
    {
        "name": "10Pearls",
        "jobs_url": "https://10pearls.com/careers/",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "10Pearls",
    },
    {
        "name": "2U",
        "jobs_url": "https://2u.com/careers/",
        "use_selenium": True,
        "ats_type": "greenhouse",
        "company": "2U",
    },
    {
        "name": "AAMI",
        "jobs_url": "https://www.aami.org/careers",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "AAMI",
    },
    {
        "name": "Agile Defense",
        "jobs_url": "https://agile-defense.com/careers/",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Agile Defense",
    },
    {
        "name": "American Association for Justice",
        "jobs_url": "https://www.justice.org/who-we-are/careers",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "American Association for Justice",
    },
    {
        "name": "American Association of Colleges of Nursing",
        "jobs_url": "https://www.aacnnursing.org/About-AACN/Employment",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "AACN",
    },
    {
        "name": "American Red Cross",
        "jobs_url": "https://www.redcross.org/about-us/careers.html",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "American Red Cross",
    },
    {
        "name": "American Society for Microbiology",
        "jobs_url": "https://asm.org/Careers/Home",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "ASM",
    },
    {
        "name": "American Society of Human Genetics",
        "jobs_url": "https://www.ashg.org/about/careers/",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "ASHG",
    },
    {
        "name": "American Medical Informatics Association",
        "jobs_url": "https://amia.org/about-amia/careers-amia",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "AMIA",
    },
    {
        "name": "ASDC",
        "jobs_url": "https://www.asdc.com/careers/",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "ASDC",
    },
    {
        "name": "ASM Research",
        "jobs_url": "https://asmresearch.com/careers/",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "ASM Research",
    },
    {
        "name": "Association of Corporate Counsel",
        "jobs_url": "https://www.acc.com/careers",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "ACC",
    },
    {
        "name": "Axios",
        "jobs_url": "https://www.axios.com/careers",
        "use_selenium": True,
        "ats_type": "greenhouse",
        "company": "Axios",
    },
    {
        "name": "Black Bear Technology",
        "jobs_url": "https://blackbeartechnology.com/careers/",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Black Bear Technology",
    },
    {
        "name": "Chewy",
        "jobs_url": "https://careers.chewy.com/us/en",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Chewy",
    },
    {
        "name": "Children's National Hospital",
        "jobs_url": "https://childrensnational.org/careers-and-volunteers/careers",
        "use_selenium": True,
        "ats_type": "workday",
        "company": "Children's National Hospital",
    },
    {
        "name": "Covington & Burling",
        "jobs_url": "https://www.cov.com/en/careers",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Covington & Burling",
    },
    {
        "name": "Crowell & Moring",
        "jobs_url": "https://www.crowell.com/Careers",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Crowell & Moring",
    },
    {
        "name": "DMI",
        "jobs_url": "https://dmi.com/careers",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "DMI",
    },
    {
        "name": "Dominion Energy",
        "jobs_url": "https://careers.dominionenergy.com/",
        "use_selenium": True,
        "ats_type": "taleo",
        "company": "Dominion Energy",
    },
    {
        "name": "Draper",
        "jobs_url": "https://www.draper.com/careers",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Draper",
    },
    {
        "name": "FHLBank Office of Finance",
        "jobs_url": "https://www.fhlb-of.com/ofweb_userWeb/careers/",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "FHLBank",
    },
    {
        "name": "Fortrea",
        "jobs_url": "https://careers.fortrea.com/",
        "use_selenium": True,
        "ats_type": "workday",
        "company": "Fortrea",
    },
    {
        "name": "Freshfields",
        "jobs_url": "https://careers.freshfields.com/",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Freshfields",
    },
    {
        "name": "Geoquest USA",
        "jobs_url": "https://www.geoquestusa.com/careers/",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Geoquest USA",
    },
    {
        "name": "GoldSchmitt & Associates",
        "jobs_url": "https://www.goldschmitt.com/careers/",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "GoldSchmitt & Associates",
    },
    {
        "name": "Good360",
        "jobs_url": "https://good360.org/careers/",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Good360",
    },
    {
        "name": "Goodshuffle",
        "jobs_url": "https://www.goodshuffle.com/careers",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Goodshuffle",
    },
    {
        "name": "Groundswell",
        "jobs_url": "https://www.groundswell.com/careers",
        "use_selenium": True,
        "ats_type": "lever",
        "company": "Groundswell",
    },
    {
        "name": "Harmonia Holdings Group",
        "jobs_url": "https://www.harmoniaholdings.com/careers/",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Harmonia Holdings Group",
    },
    {
        "name": "ICF",
        "jobs_url": "https://www.icf.com/careers/jobs",
        "use_selenium": True,
        "ats_type": "workday",
        "company": "ICF",
    },
    {
        "name": "ID.me",
        "jobs_url": "https://www.id.me/careers",
        "use_selenium": True,
        "ats_type": "greenhouse",
        "company": "ID.me",
    },
    {
        "name": "Kilsay",
        "jobs_url": "https://kilsay.com/careers/",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Kilsay",
    },
    {
        "name": "Knucklepuck",
        "jobs_url": "https://www.knucklepuck.com/careers",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Knucklepuck",
    },
    {
        "name": "Long Fence",
        "jobs_url": "https://www.longfence.com/careers",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Long Fence",
    },
    {
        "name": "Moxfive",
        "jobs_url": "https://www.moxfive.com/careers",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Moxfive",
    },
    {
        "name": "Nestle",
        "jobs_url": "https://www.nestlejobs.com/",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Nestle",
    },
    {
        "name": "PCORI",
        "jobs_url": "https://www.pcori.org/about/careers",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "PCORI",
    },
    {
        "name": "Pingwin",
        "jobs_url": "https://pingwin.com/careers/",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "Pingwin",
    },
    {
        "name": "QXO",
        "jobs_url": "https://www.qxo.com/careers/",
        "use_selenium": True,
        "ats_type": "custom",
        "company": "QXO",
    },
]


def get_selenium_driver() -> webdriver.Chrome:
    """Create and configure a Selenium Chrome WebDriver instance."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
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
    """Check if scraping is allowed according to robots.txt."""
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
        return True


def clean_text(text: str) -> str:
    """Clean extracted text by removing extra whitespace and common UI elements."""
    if not text:
        return ""
    
    # Remove common UI elements and labels
    ui_patterns = [
        r'^(Location|Categories?|Filters?|Business Unit|Company|Save|View Job|Apply Now|Apply)$',
        r'^\d+ Results?$',
        r'^home$',
        r'^Remote$',
    ]
    
    for pattern in ui_patterns:
        if re.match(pattern, text.strip(), re.IGNORECASE):
            return ""
    
    # Clean whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Remove "Location:" prefix
    text = re.sub(r'^Location:\s*', '', text, flags=re.IGNORECASE)
    
    return text


def is_valid_job_title(title: str) -> bool:
    """Check if a string is likely a valid job title."""
    if not title or len(title) < 3:
        return False
    
    # Common non-job-title patterns
    invalid_patterns = [
        r'^(Filters?|Location|Categories?|Save|Apply|View|Company|Results?)$',
        r'^\d+$',  # Just numbers
        r'^[A-Z]{2,3}$',  # State codes
        r'^home$',
    ]
    
    for pattern in invalid_patterns:
        if re.match(pattern, title.strip(), re.IGNORECASE):
            return False
    
    # Job titles usually have at least one word with 3+ letters
    words = title.split()
    has_real_word = any(len(word) > 2 for word in words)
    
    return has_real_word


def scrape_workday_site(driver: webdriver.Chrome, site_conf: Dict, max_jobs: int) -> List[Dict]:
    """Special handling for Workday ATS sites."""
    jobs = []
    jobs_url = site_conf["jobs_url"]
    
    try:
        driver.get(jobs_url)
        time.sleep(5)  # Workday sites load slowly
        
        # Workday-specific selectors
        job_selectors = [
            "li[data-automation-id='jobItem']",
            "div[data-automation-id='jobItem']",
            "[role='listitem'] a[data-automation-id='jobTitle']",
        ]
        
        cards = []
        for selector in job_selectors:
            try:
                cards = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, selector))
                )
                if cards:
                    logger.info(f"Found {len(cards)} Workday job items")
                    break
            except TimeoutException:
                continue
        
        for card in cards[:max_jobs]:
            try:
                # Extract title
                title_element = card.find_element(By.CSS_SELECTOR, "[data-automation-id='jobTitle']")
                title = clean_text(title_element.text)
                
                if not is_valid_job_title(title):
                    continue
                
                # Extract location
                location = "Not specified"
                try:
                    location_element = card.find_element(By.CSS_SELECTOR, "[data-automation-id='location']")
                    location = clean_text(location_element.text)
                except:
                    pass
                
                # Get URL
                try:
                    link = title_element.get_attribute("href")
                    if not link:
                        link = card.find_element(By.TAG_NAME, "a").get_attribute("href")
                except:
                    link = jobs_url
                
                jobs.append({
                    "company": site_conf["company"],
                    "title": title,
                    "location": location or "Not specified",
                    "url": link or jobs_url
                })
                
            except Exception as e:
                logger.debug(f"Error processing Workday job card: {e}")
                continue
                
    except Exception as e:
        logger.error(f"Error scraping Workday site {site_conf['name']}: {e}")
    
    return jobs


def scrape_greenhouse_site(driver: webdriver.Chrome, site_conf: Dict, max_jobs: int) -> List[Dict]:
    """Special handling for Greenhouse ATS sites."""
    jobs = []
    jobs_url = site_conf["jobs_url"]
    
    try:
        driver.get(jobs_url)
        time.sleep(3)
        
        # Greenhouse-specific selectors
        cards = driver.find_elements(By.CSS_SELECTOR, "div.opening")
        logger.info(f"Found {len(cards)} Greenhouse job openings")
        
        for card in cards[:max_jobs]:
            try:
                # Extract title
                title_element = card.find_element(By.CSS_SELECTOR, "a")
                title = clean_text(title_element.text)
                
                if not is_valid_job_title(title):
                    continue
                
                # Extract location
                location = "Not specified"
                try:
                    location_element = card.find_element(By.CSS_SELECTOR, ".location")
                    location = clean_text(location_element.text)
                except:
                    pass
                
                # Get URL
                link = title_element.get_attribute("href")
                
                jobs.append({
                    "company": site_conf["company"],
                    "title": title,
                    "location": location,
                    "url": link or jobs_url
                })
                
            except Exception as e:
                logger.debug(f"Error processing Greenhouse job card: {e}")
                continue
                
    except Exception as e:
        logger.error(f"Error scraping Greenhouse site {site_conf['name']}: {e}")
    
    return jobs


def scrape_lever_site(driver: webdriver.Chrome, site_conf: Dict, max_jobs: int) -> List[Dict]:
    """Special handling for Lever ATS sites."""
    jobs = []
    jobs_url = site_conf["jobs_url"]
    
    try:
        driver.get(jobs_url)
        time.sleep(3)
        
        # Lever-specific selectors
        cards = driver.find_elements(By.CSS_SELECTOR, ".posting")
        if not cards:
            cards = driver.find_elements(By.CSS_SELECTOR, "a[class*='posting-title']")
        
        logger.info(f"Found {len(cards)} Lever job postings")
        
        for card in cards[:max_jobs]:
            try:
                title = clean_text(card.find_element(By.CSS_SELECTOR, ".posting-title").text)
                if not is_valid_job_title(title):
                    continue
                
                location = "Not specified"
                try:
                    location = clean_text(card.find_element(By.CSS_SELECTOR, ".posting-categories location").text)
                except:
                    pass
                
                link = card.get_attribute("href") if card.tag_name == "a" else card.find_element(By.TAG_NAME, "a").get_attribute("href")
                
                jobs.append({
                    "company": site_conf["company"],
                    "title": title,
                    "location": location,
                    "url": link or jobs_url
                })
                
            except Exception as e:
                logger.debug(f"Error processing Lever job card: {e}")
                continue
                
    except Exception as e:
        logger.error(f"Error scraping Lever site {site_conf['name']}: {e}")
    
    return jobs


def scrape_with_selenium(site_conf: Dict, max_jobs: int = MAX_JOBS_PER_SITE) -> List[Dict[str, Optional[str]]]:
    """Scrape job listings using Selenium with ATS-specific handling."""
    jobs = []
    driver = None
    
    try:
        logger.info(f"[SELENIUM] Loading {site_conf['jobs_url']}")
        driver = get_selenium_driver()
        
        # Route to ATS-specific scraper if available
        ats_type = site_conf.get("ats_type", "custom")
        
        if ats_type == "workday":
            jobs = scrape_workday_site(driver, site_conf, max_jobs)
        elif ats_type == "greenhouse":
            jobs = scrape_greenhouse_site(driver, site_conf, max_jobs)
        elif ats_type == "lever":
            jobs = scrape_lever_site(driver, site_conf, max_jobs)
        else:
            # Generic scraping with better filtering
            driver.get(site_conf["jobs_url"])
            time.sleep(4)
            
            # Try various selectors
            selectors_to_try = [
                "a[href*='/job']:not([class*='filter'])",
                "a[href*='/career']:not([class*='filter'])",
                "div[class*='job-card']",
                "li[class*='job-item']",
                "article[class*='job']",
                "div.opening",
                "[data-job-id]",
                "div[class*='posting']",
                "tbody tr:has(a)",
            ]
            
            cards = []
            for selector in selectors_to_try:
                try:
                    potential_cards = driver.find_elements(By.CSS_SELECTOR, selector)
                    if potential_cards:
                        logger.info(f"Found {len(potential_cards)} elements with selector: {selector}")
                        cards = potential_cards
                        break
                except:
                    continue
            
            if not cards:
                # Last resort: find all links that might be jobs
                all_links = driver.find_elements(By.TAG_NAME, "a")
                job_links = []
                for link in all_links:
                    href = link.get_attribute("href") or ""
                    text = link.text.strip()
                    if (("/job" in href.lower() or "/career" in href.lower() or 
                         "/position" in href.lower() or "/opening" in href.lower()) and
                        is_valid_job_title(text)):
                        job_links.append(link)
                
                if job_links:
                    logger.info(f"Found {len(job_links)} potential job links")
                    cards = job_links
            
            # Process cards
            seen_titles = set()
            for card in cards[:max_jobs * 2]:
                try:
                    card_text = card.text.strip() if hasattr(card, 'text') else card.get_attribute('innerText') or ""
                    
                    if not card_text or len(card_text) > 500:
                        continue
                    
                    lines = card_text.split('\n')
                    
                    title = None
                    location = None
                    
                    for line in lines:
                        line = clean_text(line)
                        if not line:
                            continue
                        
                        if not title and is_valid_job_title(line):
                            title = line
                        elif title and not location and len(line) > 2:
                            if not re.match(r'^(Save|Apply|View)', line, re.IGNORECASE):
                                location = line
                                break
                    
                    if not title or title in seen_titles:
                        continue
                    
                    seen_titles.add(title)
                    
                    href = None
                    try:
                        if card.tag_name == 'a':
                            href = card.get_attribute("href")
                        else:
                            link = card.find_element(By.TAG_NAME, "a")
                            href = link.get_attribute("href")
                    except:
                        href = site_conf["jobs_url"]
                    
                    jobs.append({
                        "company": site_conf["company"],
                        "title": title,
                        "location": location or "Not specified",
                        "url": href or site_conf["jobs_url"]
                    })
                    
                    if len(jobs) >= max_jobs:
                        break
                    
                except Exception as e:
                    logger.debug(f"Error processing card: {e}")
                    continue
        
        logger.info(f"Successfully extracted {len(jobs)} jobs from {site_conf['name']}")
        
    except WebDriverException as e:
        logger.error(f"Selenium error for {site_conf['name']}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error for {site_conf['name']}: {e}")
    finally:
        if driver:
            driver.quit()
    
    return jobs


def scrape_with_api(site_conf: Dict, max_jobs: int = MAX_JOBS_PER_SITE) -> List[Dict[str, Optional[str]]]:
    """Scrape job listings using a JSON API."""
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
                "company": site_conf["company"],
                "title": job.get("name", "Unknown Title"),
                "location": location_str,
                "url": job.get("ref", api_url)
            })
            
    except requests.RequestException as e:
        logger.error(f"Failed to fetch API: {e}")
    except (KeyError, ValueError) as e:
        logger.error(f"Failed to parse API response: {e}")
    
    return jobs


def scrape_site(site_conf: Dict, max_jobs: int = MAX_JOBS_PER_SITE) -> List[Dict[str, Optional[str]]]:
    """Scrape job listings from a single site."""
    # Check robots.txt for non-API methods
    if not site_conf.get("use_api", False):
        jobs_url = site_conf["jobs_url"]
        parsed = urlparse(jobs_url)
        
        if not allowed_to_scrape(f"{parsed.scheme}://{parsed.netloc}", parsed.path):
            logger.warning(f"[SKIP] robots.txt forbids scraping {jobs_url}")
            return []

    logger.info(f"[SCRAPING] {site_conf['name']}")
    
    # Choose scraping method
    if site_conf.get("use_api", False):
        jobs = scrape_with_api(site_conf, max_jobs)
    elif site_conf.get("use_selenium", False):
        jobs = scrape_with_selenium(site_conf, max_jobs)
    else:
        logger.warning(f"No scraping method specified for {site_conf['name']}")
        jobs = []
    
    # Validate and clean jobs
    cleaned_jobs = []
    for job in jobs:
        if not job.get("title") or not is_valid_job_title(job["title"]):
            continue
        
        if job.get("location"):
            job["location"] = clean_text(job["location"]) or "Not specified"
        
        cleaned_jobs.append(job)
    
    logger.info(f"Cleaned {len(cleaned_jobs)} valid jobs from {len(jobs)} raw entries")
    
    # Polite delay
    time.sleep(uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS))
    return cleaned_jobs


def save_to_csv(rows: List[Dict], filename: str = OUTPUT_CSV) -> None:
    """Save job listings to a CSV file."""
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
    """Main function to scrape all sites and save results."""
    all_jobs = []
    successful_sites = 0
    failed_sites = []
    
    print(f"\n{'='*60}")
    print(f"Starting scraper for {len(SITES)} companies...")
    print(f"{'='*60}\n")
    
    for i, site in enumerate(SITES, 1):
        try:
            print(f"[{i}/{len(SITES)}] Processing {site['name']}...", end=' ')
            rows = scrape_site(site)
            if rows:
                all_jobs.extend(rows)
                successful_sites += 1
                print(f"✓ {len(rows)} jobs")
            else:
                failed_sites.append(site.get('name', 'Unknown'))
                print(f"✗ No jobs found")
        except Exception as e:
            logger.error(f"Error scraping {site.get('name', 'Unknown')}: {e}")
            failed_sites.append(site.get('name', 'Unknown'))
            print(f"✗ Error")
    
    # Summary
    print(f"\n{'='*60}")
    print(f"SCRAPING COMPLETE")
    print(f"Successful sites: {successful_sites}/{len(SITES)}")
    if failed_sites:
        print(f"Failed sites: {len(failed_sites)}")
        print(f"  {', '.join(failed_sites[:10])}")
        if len(failed_sites) > 10:
            print(f"  ... and {len(failed_sites) - 10} more")
    print(f"Total jobs collected: {len(all_jobs)}")
    print(f"Output file: {OUTPUT_CSV}")
    print(f"{'='*60}\n")
    
    save_to_csv(all_jobs)


if __name__ == "__main__":
    main()