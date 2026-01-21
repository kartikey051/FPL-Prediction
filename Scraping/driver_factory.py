"""Selenium WebDriver factory."""

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def create_driver(headless: bool = True, page_load_timeout: int = 30) -> webdriver.Chrome:
    """
    Create and configure a Selenium Chrome WebDriver.
    
    Args:
        headless: Run browser in headless mode
        page_load_timeout: Page load timeout in seconds
        
    Returns:
        Configured Chrome WebDriver instance
    """
    logger.info("Initializing Chrome WebDriver")
    
    options = Options()
    
    if headless:
        options.add_argument("--headless=new")
        logger.info("Running in headless mode")
    
    # Performance and stability options
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    
    # User agent
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
        driver.set_page_load_timeout(page_load_timeout)
        
        logger.info("WebDriver initialized successfully")
        return driver
        
    except Exception as e:
        logger.error(f"Failed to initialize WebDriver: {e}")
        raise


def close_driver(driver: Optional[webdriver.Chrome]) -> None:
    """
    Safely close WebDriver instance.
    
    Args:
        driver: WebDriver instance to close
    """
    if driver:
        try:
            driver.quit()
            logger.info("WebDriver closed successfully")
        except Exception as e:
            logger.warning(f"Error closing WebDriver: {e}")