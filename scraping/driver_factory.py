"""
Selenium WebDriver factory for creating configured driver instances.
"""

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

from Utils.logging_config import get_logger
from scraping import config

logger = get_logger("driver_factory")


def create_driver(headless: bool = config.HEADLESS) -> webdriver.Chrome:
    """
    Create and configure a Chrome WebDriver instance.
    
    Args:
        headless: Whether to run browser in headless mode
        
    Returns:
        Configured Chrome WebDriver
    """
    logger.info(f"Creating Chrome WebDriver (headless={headless})")
    
    options = Options()
    
    if headless:
        options.add_argument("--headless=new")
    
    # Performance and stability options
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    
    # User agent to avoid detection
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
    
    # Suppress logging
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        
        # Set timeouts
        driver.set_page_load_timeout(config.PAGE_LOAD_TIMEOUT)
        driver.implicitly_wait(config.IMPLICIT_WAIT)
        
        logger.info("WebDriver created successfully")
        return driver
        
    except Exception as e:
        logger.error(f"Failed to create WebDriver: {e}")
        raise