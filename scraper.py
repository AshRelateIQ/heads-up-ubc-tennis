import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Callable

import pandas as pd
from playwright.async_api import async_playwright, Page, TimeoutError as PlaywrightTimeoutError
import pytz
import requests
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG for detailed logging
    format="%(asctime)s [%(levelname)s] %(message)s",
)

DEFAULT_URL = "https://recreation.ubc.ca/tennis/court-booking/"
DATA_PATH = Path(__file__).parent / "court_data.json"


async def _click_and_wait(page: Page, locator_str: str, wait_for: Optional[str] = None, timeout: int = 10_000) -> None:
    """Click an element and optionally wait for a selector."""
    await page.click(locator_str, timeout=timeout)
    if wait_for:
        await page.wait_for_selector(wait_for, timeout=timeout)


async def _ensure_page_size_20(page: Page) -> bool:
    """Ensure the page size is set to 20 results. Returns True if successful."""
    try:
        # Check if page size is already 20 by looking at the select element
        select_elements = await page.query_selector_all("select")
        for select_el in select_elements:
            try:
                current_value = await select_el.evaluate("el => el.value")
                if current_value == "20":
                    logging.debug("Page size is already set to 20")
                    return True
            except Exception:
                continue
        
        # Page size is not 20, change it
        logging.info("Setting page size to 20 results...")
        select_elements = await page.query_selector_all("select, [role='combobox']")
        
        for idx, select_el in enumerate(select_elements):
            try:
                options = await select_el.query_selector_all("option")
                for opt in options:
                    opt_text = (await opt.inner_text()).strip()
                    opt_value = await opt.get_attribute("value") or ""
                    if opt_text == "20" or opt_value == "20":
                        tag_name = await select_el.evaluate("el => el.tagName.toLowerCase()")
                        if tag_name == "select":
                            try:
                                await select_el.evaluate("el => { el.value = '20'; el.dispatchEvent(new Event('change', { bubbles: true })); }")
                                await page.wait_for_timeout(2_000)
                                logging.info("✅ Set page size to 20 results")
                                return True
                            except Exception:
                                continue
            except Exception:
                continue
        
        logging.warning("Could not set page size to 20")
        return False
    except Exception as e:
        logging.warning("Error setting page size to 20: %s", e)
        return False


async def _wait_for_court_list_loaded(page: Page, expected_court_count: int = 10, max_wait: int = 30_000, check_interval: int = 1_000) -> bool:
    """Wait briefly for the court list page to load, then proceed.
    
    Just waits 2-3 seconds and does a quick check that we have some courts.
    Returns True to proceed regardless.
    """
    logging.info("Waiting briefly for court list to load (2-3 seconds)...")
    
    # Wait 2-3 seconds for page to settle
    await page.wait_for_timeout(2_500)
    
    # Quick check that we're on the right page and have some courts
    try:
        current_url = page.url
        if "perfectmind" in current_url.lower() and "facility" in current_url.lower():
            # We're on a facility/schedule page, not the list
            logging.debug("Still on facility page, but proceeding anyway...")
        else:
            # Check if we have at least some courts
            court_elements = await page.query_selector_all("text=/Court\\s+\\d+/i")
            court_count = len(court_elements)
            if court_count > 0:
                logging.info("✅ Found %d courts on page, proceeding...", court_count)
            else:
                logging.debug("No courts found yet, but proceeding anyway...")
    except Exception as exc:
        logging.debug("Error checking court list: %s, but proceeding anyway...", exc)
    
    # Always return True - we'll proceed and handle errors as we go
    return True


async def _verify_court_on_page(page: Page, court_name: str) -> bool:
    """Verify that the specified court name appears on the current page.
    
    Checks multiple locations: page title, headings, body text, etc.
    """
    try:
        # Normalize court name for matching (handle "Court 01" vs "Court 1" etc)
        court_num_match = re.search(r'Court\s+0?(\d+)', court_name, re.IGNORECASE)
        if court_num_match:
            court_num = int(court_num_match.group(1))
            # Try multiple formats
            court_patterns = [
                f"Court\\s+{court_num:02d}",  # Court 01
                f"Court\\s+{court_num}",      # Court 1
                f"Court\\s+0{court_num}",     # Court 01 (alternative)
            ]
        else:
            court_patterns = [re.escape(court_name)]
        
        # Check page title
        try:
            title = await page.title()
            for pattern in court_patterns:
                if re.search(pattern, title, re.IGNORECASE):
                    logging.debug("Found court name in page title: %s", title)
                    return True
        except Exception:
            pass
        
        # Check all headings (h1, h2, h3, etc.)
        try:
            headings = await page.query_selector_all("h1, h2, h3, h4, h5, h6")
            for heading in headings:
                try:
                    heading_text = await heading.inner_text()
                    for pattern in court_patterns:
                        if re.search(pattern, heading_text, re.IGNORECASE):
                            logging.debug("Found court name in heading: %s", heading_text)
                            return True
                except Exception:
                    continue
        except Exception:
            pass
        
        # Check page body text (look for court name near "Court" or facility-related text)
        try:
            body_text = await page.inner_text("body")
            for pattern in court_patterns:
                # Look for the pattern, ideally near words like "Court", "Facility", "Schedule"
                if re.search(pattern, body_text, re.IGNORECASE):
                    # Additional check: make sure it's not just in a list of all courts
                    # If we see many court numbers, we might still be on the list page
                    all_courts = re.findall(r'Court\s+\d+', body_text, re.IGNORECASE)
                    if len(all_courts) <= 2:  # Should only see 1-2 mentions of court names
                        logging.debug("Found court name in page body")
                        return True
        except Exception:
            pass
        
        # Check URL parameters (sometimes court info is in the URL)
        try:
            current_url = page.url
            for pattern in court_patterns:
                if re.search(pattern, current_url, re.IGNORECASE):
                    logging.debug("Found court name in URL: %s", current_url[:100])
                    return True
        except Exception:
            pass
        
        return False
    except Exception as exc:
        logging.debug("Error verifying court on page: %s", exc)
        return False


async def _wait_for_schedule_page(page: Page, court_name: str, max_wait: int = 30_000, check_interval: int = 1_000) -> bool:
    """Wait for the schedule page to load and VERIFY we're on the correct court's page.
    
    This function is very patient and verifies:
    1. We're on a schedule/booking page (not the court list)
    2. The page actually shows the correct court name
    3. The page has loaded calendar/schedule elements
    
    Returns True only if ALL verifications pass, False otherwise.
    """
    start_time = time.time() * 1000  # Convert to milliseconds
    elapsed = 0
    last_log_time = 0
    
    logging.info("Waiting for schedule page for %s (max wait: %d seconds)...", court_name, max_wait // 1000)
    
    while elapsed < max_wait:
        try:
            # Step 1: Check that we're NOT on the court list page
            court_list_indicators = await page.query_selector_all("text=/Court\\s+\\d+/i")
            if len(court_list_indicators) > 5:
                # Still on the list page, wait more
                if elapsed - last_log_time > 10_000:  # Log every 10 seconds
                    logging.debug("Still on court list page, waiting... (%d courts found)", len(court_list_indicators))
                    last_log_time = elapsed
                await page.wait_for_timeout(check_interval)
                elapsed = (time.time() * 1000) - start_time
                continue
            
            # Step 2: Check if we're on a schedule/booking page
            schedule_indicators = [
                "table",  # Calendar table
                ".calendar",
                "[class*='calendar']",
                "[class*='schedule']",
                "[class*='time-slot']",
                "td[onclick]",
                "div[onclick]",
                "[id*='calendar']",
                "[id*='schedule']",
            ]
            
            found_schedule = False
            for indicator in schedule_indicators:
                try:
                    elements = await page.query_selector_all(indicator)
                    if elements and len(elements) > 0:
                        found_schedule = True
                        break
                except Exception:
                    continue
            
            if not found_schedule:
                # Not on schedule page yet
                if elapsed - last_log_time > 10_000:
                    logging.debug("Schedule elements not found yet, waiting...")
                    last_log_time = elapsed
                await page.wait_for_timeout(check_interval)
                elapsed = (time.time() * 1000) - start_time
                continue
            
            # Step 3: CRITICAL - Verify we're on the CORRECT court's page
            court_verified = await _verify_court_on_page(page, court_name)
            
            if not court_verified:
                # We're on a schedule page, but not the right court!
                logging.warning("On a schedule page, but court verification failed for %s. Checking again...", court_name)
                if elapsed - last_log_time > 10_000:
                    # Try to get more info about what court we're actually on
                    try:
                        page_text = await page.inner_text("body")
                        found_courts = re.findall(r'Court\s+\d+', page_text, re.IGNORECASE)
                        if found_courts:
                            logging.warning("Found these courts on page: %s (expected: %s)", found_courts[:5], court_name)
                    except Exception:
                        pass
                    last_log_time = elapsed
                await page.wait_for_timeout(check_interval)
                elapsed = (time.time() * 1000) - start_time
                continue
            
            # Step 4: Additional confirmation - check URL
            current_url = page.url
            url_ok = "perfectmind" in current_url.lower() or "book" in current_url.lower() or "schedule" in current_url.lower() or "facility" in current_url.lower()
            
            if court_verified and found_schedule and url_ok:
                logging.info("✅ VERIFIED on correct schedule page for %s (URL: %s)", court_name, current_url[:100])
                return True
            
            # If we have schedule but court not verified, keep waiting
            if elapsed - last_log_time > 10_000:
                logging.debug("Schedule found but verification incomplete, continuing to wait...")
                last_log_time = elapsed
            
            await page.wait_for_timeout(check_interval)
            elapsed = (time.time() * 1000) - start_time
            
        except Exception as exc:
            logging.debug("Error checking schedule page: %s", exc)
            await page.wait_for_timeout(check_interval)
            elapsed = (time.time() * 1000) - start_time
    
    logging.warning("❌ Timeout waiting for schedule page for %s (waited %d seconds)", court_name, max_wait // 1000)
    return False


async def _scrape_court_schedule(page: Page, court_name: str) -> List[Dict]:
    """Scrape available slots from a court's weekly schedule view.
    
    IMPORTANT: This function assumes we're already on the correct court's schedule page.
    It will verify this before scraping.
    """
    entries: List[Dict] = []
    try:
        # CRITICAL: Verify we're on the correct court's page before scraping
        logging.info("Verifying we're on the correct page for %s before scraping...", court_name)
        court_verified = await _verify_court_on_page(page, court_name)
        if not court_verified:
            logging.error("❌ VERIFICATION FAILED: Not on %s's page! Attempting to identify which court we're actually on...", court_name)
            # Try to identify which court we're actually on
            try:
                page_text = await page.inner_text("body")
                found_courts = re.findall(r'Court\s+\d+', page_text, re.IGNORECASE)
                if found_courts:
                    unique_courts = list(set(found_courts))
                    logging.error("Found these courts on the page: %s (expected: %s)", unique_courts, court_name)
                else:
                    logging.error("No court names found on page")
            except Exception as exc:
                logging.error("Could not identify court on page: %s", exc)
            return entries  # Return empty - don't scrape wrong court data
        
        logging.info("✅ Verified on %s's page, proceeding with scraping...", court_name)
        
        # Wait for the schedule table to load - use #scheduler as the main container
        logging.info("Waiting for schedule table (#scheduler) to load...")
        try:
            await page.wait_for_selector("#scheduler", timeout=30_000)
            await page.wait_for_timeout(1_000)  # Give it a moment to fully render
        except Exception as exc:
            logging.error("No #scheduler container found on page: %s", exc)
            return entries
        
        # Use #scheduler as the main container (not just the table)
        scheduler_container = page.locator("#scheduler")
        
        # Get current time in Vancouver (Pacific timezone)
        vancouver_tz = pytz.timezone('America/Vancouver')
        now_vancouver = datetime.now(vancouver_tz)
        today = now_vancouver.date()
        
        # Calculate 72 hours from now
        max_datetime = now_vancouver + timedelta(hours=72)
        logging.debug("Current time in Vancouver: %s, Max time (72h): %s", now_vancouver, max_datetime)
        
        # Generate time slots from 8:00 AM to 10:00 PM (hourly)
        time_slots = []
        for hour in range(8, 23):  # 8 AM to 10 PM (22:00 = 10 PM)
            # Convert to 12-hour format
            start_period = "AM" if hour < 12 else "PM"
            start_hour_12 = hour if hour <= 12 else hour - 12
            if start_hour_12 == 0:
                start_hour_12 = 12
            
            time_slot_str = f"{start_hour_12}:00 {start_period}"
            time_slots.append(time_slot_str)
        
        logging.info("Checking slots for time ranges: %s", time_slots)
        
        # Locate ALL rows in the scheduler container
        rows = scheduler_container.locator("tbody tr")
        row_count = await rows.count()
        logging.info("Found %d rows in scheduler container", row_count)
        
        # Debug: Check for gridcells with "Bookable" text
        bookable_gridcells = scheduler_container.locator("[role='gridcell']:has-text('Bookable')")
        bookable_count = await bookable_gridcells.count()
        logging.info("Found %d gridcells with 'Bookable' text", bookable_count)
        
        if bookable_count > 0:
            logging.info("Debugging first 5 bookable gridcells:")
            for i in range(min(5, bookable_count)):
                try:
                    gc = bookable_gridcells.nth(i)
                    gc_text = await gc.inner_text()
                    gc_title = await gc.get_attribute("title") or ""
                    # Get the parent row to see structure
                    parent_row = await gc.evaluate_handle("el => el.closest('tr')")
                    row_text = ""
                    if parent_row:
                        try:
                            row_elem = parent_row.as_element() if hasattr(parent_row, 'as_element') else parent_row
                            if row_elem:
                                row_text = await row_elem.inner_text()
                        except Exception:
                            pass
                    logging.info("  Bookable gridcell %d: title='%s', text='%s', row text: %s", 
                               i, gc_title[:80], gc_text[:100], row_text[:150] if row_text else "empty")
                except Exception as exc:
                    logging.debug("  Bookable gridcell %d: error - %s", i, exc)
        
        # Also check all gridcells to see what titles they have
        all_gridcells = scheduler_container.locator("[role='gridcell']")
        gc_count = await all_gridcells.count()
        logging.info("Checking titles of first 20 gridcells:")
        titles_found = set()
        for i in range(min(20, gc_count)):
            try:
                gc = all_gridcells.nth(i)
                gc_title = await gc.get_attribute("title") or ""
                if gc_title:
                    titles_found.add(gc_title)
            except Exception:
                pass
        logging.info("  Unique titles found: %s", list(titles_found)[:10])
        
        # Also check what's in row 3 (the one with 92 cells)
        if row_count > 3:
            logging.info("Debugging row 3 (the one with many cells):")
            row3 = rows.nth(3)
            row3_cells = row3.locator("td, th")
            row3_cell_count = await row3_cells.count()
            logging.info("  Row 3 has %d cells", row3_cell_count)
            # Check first 10 cells
            for j in range(min(10, row3_cell_count)):
                try:
                    cell = row3_cells.nth(j)
                    cell_text = await cell.inner_text()
                    cell_html = await cell.evaluate("el => el.outerHTML.substring(0, 200)")
                    logging.info("    Cell %d: text='%s', HTML: %s", j, cell_text[:80], cell_html)
                except Exception as exc:
                    logging.debug("    Cell %d: error - %s", j, exc)
        
        # Debug: Print first few rows to see structure
        if row_count > 0:
            logging.info("Debugging first 10 rows:")
            for i in range(min(10, row_count)):
                try:
                    row = rows.nth(i)
                    row_text = await row.inner_text()
                    cells = row.locator("td, th")
                    cell_count = await cells.count()
                    cell_texts = []
                    for j in range(min(5, cell_count)):  # First 5 cells
                        try:
                            cell_text = await cells.nth(j).inner_text()
                            cell_texts.append(f"cell[{j}]='{cell_text[:50]}'")
                        except Exception:
                            cell_texts.append(f"cell[{j}]=error")
                    logging.info("  Row %d: %d cells, cells: %s", i, cell_count, ", ".join(cell_texts))
                except Exception as exc:
                    logging.debug("  Row %d: error - %s", i, exc)
        
        # Find all gridcells that contain a span with a title attribute
        # The span has title="03:00 PM-04:00 PM" and text "Bookable 24hrs in advance"
        all_gridcells = scheduler_container.locator("[role='gridcell']")
        gc_count = await all_gridcells.count()
        logging.info("Found %d gridcells in #scheduler", gc_count)
        
        for i in range(gc_count):
            try:
                gc = all_gridcells.nth(i)
                
                # Look for span elements with title attribute inside this gridcell
                title_spans_locator = gc.locator("span[title]")
                span_count = await title_spans_locator.count()
                
                # Debug: Log first few gridcells
                if i < 3 and span_count > 0:
                    logging.debug("Gridcell %d: found %d spans with title", i, span_count)
                
                for span_idx in range(span_count):
                    try:
                        span = title_spans_locator.nth(span_idx)
                        span_title = await span.get_attribute("title") or ""
                        span_text = (await span.inner_text()).strip().lower()
                        
                        # Check if this span has a time title and bookable text
                        if not span_title or ("AM" not in span_title and "PM" not in span_title):
                            continue
                        
                        # Check if it's bookable
                        is_bookable = False
                        status = "Open"
                        
                        if "bookable 24hrs in advance" in span_text or "bookable 24 hours in advance" in span_text:
                            is_bookable = True
                            status = "Bookable in 24h"
                        elif "book now" in span_text or ("book" in span_text and len(span_text) < 50):
                            is_bookable = True
                            status = "Open"
                        
                        if not is_bookable:
                            continue
                        
                        # Extract time from title (e.g., "03:00 PM-04:00 PM" -> "3:00 PM")
                        time_match = re.search(r'(\d{1,2}):00\s*(AM|PM)', span_title)
                        if not time_match:
                            continue
                        
                        time_slot_str = f"{time_match.group(1)}:00 {time_match.group(2)}"
                        logging.debug("Found bookable slot: time=%s, title='%s'", time_slot_str, span_title)
                        
                        # Determine day offset from the gridcell's left position
                        day_offset = 0
                        try:
                            left_pos = await gc.evaluate("el => parseFloat(window.getComputedStyle(el).left)")
                            # First column (today) is at ~2px, each day is ~208px
                            if left_pos <= 10:
                                day_offset = 0  # Today
                            else:
                                day_offset = int(round((left_pos - 2) / 208))
                            logging.debug("  Calculated day offset: %d (left position: %f px)", day_offset, left_pos)
                        except Exception as exc:
                            logging.warning("  Could not get left position: %s, defaulting to today (offset 0)", exc)
                            day_offset = 0
                        
                        slot_date = today + timedelta(days=day_offset)
                        date_str = slot_date.strftime("%Y-%m-%d")
                        
                        # Construct full datetime string
                        datetime_str = f"{date_str} {time_slot_str}"
                        
                        # Parse the datetime and check if it's within 72 hours
                        try:
                            # Parse time (e.g., "3:00 PM" -> hour=15, minute=0)
                            time_parts = time_slot_str.split()
                            time_hour_min = time_parts[0].split(':')
                            hour = int(time_hour_min[0])
                            minute = int(time_hour_min[1]) if len(time_hour_min) > 1 else 0
                            period = time_parts[1] if len(time_parts) > 1 else "AM"
                            
                            # Convert to 24-hour format
                            if period == "PM" and hour != 12:
                                hour += 12
                            elif period == "AM" and hour == 12:
                                hour = 0
                            
                            # Create datetime in Vancouver timezone
                            slot_datetime = vancouver_tz.localize(
                                datetime(slot_date.year, slot_date.month, slot_date.day, hour, minute)
                            )
                            
                            # Filter: only include slots within 72 hours
                            if slot_datetime > max_datetime:
                                logging.debug("  Skipping slot %s (beyond 72 hours)", datetime_str)
                                continue
                            
                            # Also skip slots in the past
                            if slot_datetime < now_vancouver:
                                logging.debug("  Skipping slot %s (in the past)", datetime_str)
                                continue
                                
                        except Exception as exc:
                            logging.warning("  Error parsing datetime for %s: %s, including anyway", datetime_str, exc)
                        
                        # Get link
                        href = page.url
                        try:
                            link_el = await gc.query_selector("a[href]")
                            if link_el:
                                href = await link_el.get_attribute("href")
                            else:
                                onclick = await gc.get_attribute("onclick")
                                if onclick:
                                    url_match = re.search(r'["\']([^"\']*perfectmind[^"\']*)["\']', onclick)
                                    if url_match:
                                        href = url_match.group(1)
                        except Exception:
                            pass
                        
                        entries.append({
                            "court": court_name,
                            "time": datetime_str,
                            "status": status,
                            "link": href,
                            "raw_text": await span.inner_text(),
                        })
                        
                        logging.debug("Found slot: %s on %s (day offset %d) - %s", 
                                    time_slot_str, date_str, day_offset, status)
                        
                    except Exception as exc:
                        logging.debug("Error processing span %d in gridcell %d: %s", span_idx, i, exc)
                        continue
                
            except Exception as exc:
                logging.debug("Error processing gridcell %d: %s", i, exc)
                continue
        
        logging.info("Found %d slots for %s", len(entries), court_name)
        
    except Exception as exc:
        logging.exception("Error scraping schedule for %s: %s", court_name, exc)
    
    return entries


async def scrape_courts(
    *,
    headless: bool = True,
    base_url: str = DEFAULT_URL,
    court_names: Optional[List[str]] = None,
    days: int = 7,
    progress_callback: Optional[Callable] = None,
) -> List[Dict]:
    """Scrape available slots for each court."""
    results: List[Dict] = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        page = await browser.new_page()
        try:
            # Step 1: Navigate to the booking page
            logging.info("Navigating to %s", base_url)
            await page.goto(base_url, wait_until="domcontentloaded", timeout=30_000)
            await page.wait_for_timeout(2000)
            
            # Step 2: Click "Book a Court" button
            logging.info("Clicking 'Book a Court' button")
            try:
                # Try multiple possible selectors for the button
                book_button_selectors = [
                    "text='Book a Court'",
                    "a:has-text('Book a Court')",
                    "button:has-text('Book a Court')",
                    "[href*='book']:has-text('Book')",
                ]
                
                clicked = False
                for selector in book_button_selectors:
                    try:
                        await page.click(selector, timeout=10_000)
                        clicked = True
                        logging.info("Clicked 'Book a Court' using selector: %s", selector)
                        break
                    except Exception:
                        continue
                
                if not clicked:
                    raise RuntimeError("Could not find 'Book a Court' button")
                
                # Wait for the court list to fully load with all courts and Choose buttons
                # Use flexible count - we'll discover the actual count (10 courts visible, Court 9 not available)
                court_list_loaded = await _wait_for_court_list_loaded(page, expected_court_count=10, max_wait=30_000)
                
                if not court_list_loaded:
                    logging.error("Failed to load court list properly, but continuing...")
                    # Still try to proceed, but log the issue
                    await page.wait_for_timeout(2_000)
                
            except Exception as exc:
                logging.error("Failed to click 'Book a Court': %s", exc)
                raise
            
            # Step 3: Change page size to show 20 results instead of paginating
            if not court_names:
                logging.info("Changing page size to show 20 results per page...")
                try:
                    # Wait for the page to be fully loaded first
                    await page.wait_for_timeout(2_000)
                    try:
                        await page.wait_for_load_state("networkidle", timeout=5_000)
                    except Exception:
                        pass
                    
                    # First, find the select/dropdown element that contains the page size options
                    # Look for select elements or comboboxes - try multiple times
                    select_elements = []
                    for attempt in range(3):
                        select_elements = await page.query_selector_all("select, [role='combobox'], [class*='select'], [class*='dropdown'], [aria-label*='per page'], [aria-label*='results']")
                        if len(select_elements) > 0:
                            break
                        await page.wait_for_timeout(1_000)
                    logging.info("Found %d potential select/dropdown elements", len(select_elements))
                    
                    page_size_changed = False
                    
                    # Try to find and click the "20" option
                    # Method 1: Direct role-based selector
                    try:
                        option_20 = page.get_by_role("option", name="20")
                        count = await option_20.count()
                        if count > 0:
                            # First, we might need to open the dropdown
                            # Look for the select element or button that opens it
                            select_parent = await option_20.first.evaluate_handle("el => el.closest('select, [role=\"combobox\"]')")
                            if select_parent:
                                # Click the parent to open dropdown
                                await select_parent.as_element().click()
                                await page.wait_for_timeout(500)
                            
                            await option_20.first.click()
                            logging.info("✅ Changed page size to 20 results (method 1)")
                            page_size_changed = True
                    except Exception as e1:
                        logging.debug("Method 1 failed: %s", e1)
                    
                    # Method 2: Find select element and look for option with text "20"
                    if not page_size_changed:
                        try:
                            for idx, select_el in enumerate(select_elements):
                                try:
                                    # Check if this select has a "20" option
                                    options = await select_el.query_selector_all("option")
                                    logging.info("Select element %d has %d options", idx, len(options))
                                    for opt_idx, opt in enumerate(options):
                                        opt_text = (await opt.inner_text()).strip()
                                        opt_value = await opt.get_attribute("value") or ""
                                        logging.info("  Option %d: text='%s', value='%s'", opt_idx, opt_text, opt_value)
                                        if opt_text == "20" or opt_value == "20":
                                            logging.info("Found '20' option in select element %d", idx)
                                            # Check if it's a real <select> element
                                            tag_name = await select_el.evaluate("el => el.tagName.toLowerCase()")
                                            logging.info("Select element %d tag: %s", idx, tag_name)
                                            
                                            if tag_name == "select":
                                                # Use locator's select_option method which is more reliable
                                                try:
                                                    select_locator = page.locator(f"select").nth(idx)
                                                    await select_locator.select_option(value="20", timeout=10_000)
                                                    logging.info("✅ Changed page size to 20 results (method 2 - select_option)")
                                                    page_size_changed = True
                                                    break
                                                except Exception as select_err:
                                                    logging.debug("select_option failed: %s", select_err)
                                                    # Fallback: try setting value directly via JavaScript
                                                    try:
                                                        await select_el.evaluate("el => { el.value = '20'; el.dispatchEvent(new Event('change', { bubbles: true })); }")
                                                        await page.wait_for_timeout(1_000)
                                                        logging.info("✅ Changed page size to 20 results (method 2 - JavaScript)")
                                                        page_size_changed = True
                                                        break
                                                    except Exception as js_err:
                                                        logging.debug("JavaScript method also failed: %s", js_err)
                                                        continue
                                            else:
                                                # Custom dropdown - use click method
                                                try:
                                                    await select_el.click(timeout=5_000)
                                                    await page.wait_for_timeout(500)
                                                    await opt.click(timeout=5_000)
                                                    logging.info("✅ Changed page size to 20 results (method 2 - custom dropdown)")
                                                    page_size_changed = True
                                                    break
                                                except Exception as click_err:
                                                    logging.debug("Custom dropdown click failed: %s", click_err)
                                                    continue
                                    if page_size_changed:
                                        break
                                except Exception as e:
                                    logging.debug("Error checking select element %d: %s", idx, e)
                                    continue
                        except Exception as e2:
                            logging.debug("Method 2 failed: %s", e2)
                    
                    # Method 3: Use locator to find option
                    if not page_size_changed:
                        try:
                            # Try finding by text content
                            all_options = await page.query_selector_all("option")
                            for opt in all_options:
                                opt_text = (await opt.inner_text()).strip()
                                if opt_text == "20":
                                    # Get parent select and click it first
                                    parent_select = await opt.evaluate_handle("el => el.parentElement")
                                    if parent_select:
                                        await parent_select.as_element().click()
                                        await page.wait_for_timeout(500)
                                    await opt.click()
                                    logging.info("✅ Changed page size to 20 results (method 3)")
                                    page_size_changed = True
                                    break
                        except Exception as e3:
                            logging.debug("Method 3 failed: %s", e3)
                    
                    if page_size_changed:
                        # Wait for page to reload with more results
                        await page.wait_for_timeout(3_000)
                        try:
                            await page.wait_for_load_state("networkidle", timeout=10_000)
                        except Exception:
                            pass
                        # Additional wait for courts to appear
                        await page.wait_for_timeout(3_000)
                        
                        # Verify we can see more courts now
                        court_check = await page.query_selector_all("text=/Court\\s+\\d+/i")
                        logging.info("After page size change: Found %d court elements", len(court_check))
                    else:
                        logging.warning("Could not change page size, continuing with default (10 results)...")
                except Exception as e:
                    logging.warning("Error changing page size: %s, continuing with default...", e)
                
                # Now discover all courts on the single page (should include all courts now)
                logging.info("Discovering all courts on page...")
                # Wait a bit more and try multiple times to find courts
                all_courts = []
                max_attempts = 3
                for attempt in range(max_attempts):
                    court_elements = await page.query_selector_all("text=/Court\\s+\\d+/i")
                    logging.info("Attempt %d/%d: Found %d court elements", attempt + 1, max_attempts, len(court_elements))
                    
                    if len(court_elements) > 0:
                        for el in court_elements:
                            try:
                                label = (await el.inner_text()).strip()
                                # Extract just "Court X" format (handle both "Court 1" and "Court 01")
                                match = re.search(r'Court\s+0?(\d+)', label, re.IGNORECASE)
                                if match:
                                    # Normalize to "Court X" format (remove leading zero)
                                    court_num = str(int(match.group(1)))  # Remove leading zeros
                                    court_label = f"Court {court_num:>02}"  # Format as "Court 01", "Court 02", etc.
                                    if court_label not in all_courts:
                                        all_courts.append(court_label)
                            except Exception:
                                continue
                        
                        if len(all_courts) >= 10:  # We should have at least 10 courts
                            break
                    
                    if attempt < max_attempts - 1:
                        await page.wait_for_timeout(2_000)
                
                court_names = sorted(all_courts, key=lambda x: int(re.search(r'\d+', x).group()))
                logging.info("Discovered %d total courts: %s", len(court_names), court_names)
            
            if not court_names:
                raise RuntimeError("No courts found on booking page")
            
            # Step 4: Find all court rows with their Choose buttons
            # Build a mapping of court names to their Choose buttons
            court_choose_map = {}
            
            # Find all elements containing "Court X" text
            all_court_elements = await page.query_selector_all("text=/Court\\s+\\d+/i")
            
            for court_el in all_court_elements:
                try:
                    court_text = (await court_el.inner_text()).strip()
                    match = re.search(r'Court\s+0?(\d+)', court_text, re.IGNORECASE)
                    if not match:
                        continue
                    
                    # Normalize court label format
                    court_num = str(int(match.group(1)))  # Remove leading zeros
                    court_label = f"Court {court_num:>02}"  # Format as "Court 01", "Court 02", etc.
                    
                    # Find the Choose button in the same row/container
                    try:
                        # Get the parent container (row, div, etc.)
                        parent_handle = await court_el.evaluate_handle("el => el.closest('tr, div.row, li, [class*=\"row\"], [class*=\"court\"]')")
                        if parent_handle:
                            parent_elem = parent_handle.as_element() if hasattr(parent_handle, 'as_element') else None
                            if parent_elem:
                                # Look for Choose button in the parent - try multiple selectors
                                choose_buttons = []
                                for selector in ["text=/Choose/i", "button:has-text('Choose')", "a:has-text('Choose')", "[class*='choose']", "[class*='select']"]:
                                    try:
                                        buttons = await parent_elem.query_selector_all(selector)
                                        if buttons:
                                            choose_buttons.extend(buttons)
                                    except Exception:
                                        continue
                                if choose_buttons:
                                    court_choose_map[court_label] = choose_buttons[0]
                                    continue
                    except Exception:
                        pass
                    
                    # If not found in parent, try to find nearby Choose button
                    # Look for Choose buttons and match by proximity/index
                    if court_label not in court_choose_map:
                        all_choose_buttons = []
                        for selector in ["text=/Choose/i", "button:has-text('Choose')", "a:has-text('Choose')"]:
                            try:
                                buttons = await page.query_selector_all(selector)
                                if buttons:
                                    all_choose_buttons.extend(buttons)
                            except Exception:
                                continue
                        # Match by index (assuming courts and buttons are in same order)
                        court_index = court_names.index(court_label) if court_label in court_names else -1
                        if court_index >= 0 and court_index < len(all_choose_buttons):
                            court_choose_map[court_label] = all_choose_buttons[court_index]
                    
                except Exception as exc:
                    logging.warning("Error mapping court to Choose button: %s", exc)
                    continue
            
            # Step 5: Loop through each court and scrape its schedule
            # Track which courts have been processed
            processed_courts = set()
            
            for court_name in court_names:
                try:
                    # Skip if already processed
                    if court_name in processed_courts:
                        logging.info("Skipping %s (already processed)", court_name)
                        continue
                    
                    current_progress = len(processed_courts) + 1
                    total_courts = len(court_names)
                    logging.info("Processing %s (%d/%d courts)", court_name, current_progress, total_courts)
                    
                    # Call progress callback if provided
                    if progress_callback:
                        try:
                            progress_callback(
                                current=current_progress,
                                total=total_courts,
                                message=f"Grabbing {court_name} timings...",
                                court_name=court_name
                            )
                        except Exception:
                            pass  # Don't fail scraping if callback fails
                    
                    # Ensure page size is 20 before processing Court 12 or 13
                    if court_name in ["Court 12", "Court 13"]:
                        logging.info("Ensuring page size is 20 for %s...", court_name)
                        await _ensure_page_size_20(page)
                        await page.wait_for_timeout(2_000)  # Wait for page to reload
                        try:
                            await page.wait_for_load_state("networkidle", timeout=5_000)
                        except Exception:
                            pass
                    
                    # Re-find the Choose button for this court (elements may be stale after navigation)
                    choose_button = None
                    try:
                        # Brief wait for page to be ready
                        await page.wait_for_timeout(500)
                        
                        # Check if we're on the court list page
                        court_list_check = await page.query_selector_all("text=/Court\\s+\\d+/i")
                        if not court_list_check or len(court_list_check) < 3:
                            logging.warning("Not on court list page, attempting to navigate back...")
                            # Try to get back to court list
                            try:
                                await page.goto(base_url, wait_until="domcontentloaded", timeout=30_000)
                                await page.wait_for_timeout(2_000)
                                await page.click("text='Book a Court'", timeout=10_000)
                                await page.wait_for_timeout(2_000)
                                
                                # Wait a bit for court list to load, but don't wait for all buttons
                                court_elements = await page.query_selector_all("text=/Court\\s+\\d+/i")
                                if len(court_elements) < 5:
                                    logging.error("Court list not loading after navigation back")
                                    continue
                            except Exception as nav_exc:
                                logging.error("Failed to navigate back to court list: %s", nav_exc)
                                continue
                        
                        # Use the exact pattern from Playwright codegen:
                        # page.get_by_role("listitem").filter(has_text="Choose Court 01 Read more").get_by_label("#: linkText + ' ' + Name #").click()
                        logging.debug("Looking for Choose button for %s using Playwright role-based selector...", court_name)
                        
                        # Extract court number from court_name (e.g., "Court 01" -> "01")
                        court_num_match = re.search(r'Court\s+0?(\d+)', court_name, re.IGNORECASE)
                        if not court_num_match:
                            logging.warning("Could not extract court number from %s", court_name)
                            processed_courts.add(court_name)
                            continue
                        
                        court_num = court_num_match.group(1)
                        # Format as "01", "02", etc. (with leading zero)
                        court_num_formatted = f"{int(court_num):02d}"
                        
                        # Find the Choose button using regular methods
                        if not choose_button:
                            # Try to find the listitem with the court text and then the button
                            max_button_wait = 10_000
                            button_check_interval = 500
                            button_start_time = time.time() * 1000
                            button_elapsed = 0
                            
                            while button_elapsed < max_button_wait and not choose_button:
                                try:
                                    # Method 1: Find listitem with role="listitem" containing the court text
                                    # Pattern: "Choose Court 01 Read more"
                                    listitems = await page.query_selector_all("li[role='listitem'], [role='listitem']")
                                    
                                    for li in listitems:
                                        try:
                                            li_text = await li.inner_text()
                                            # Look for listitem containing "Choose Court XX" or "Court XX Read more"
                                            if f"Court {court_num_formatted}" in li_text and ("Choose" in li_text or "Read more" in li_text):
                                                # Found the listitem for this court, now find the button inside it
                                                # Look for button with aria-label pattern or pm-confirm-button class
                                                button = await li.query_selector("a[aria-label*='linkText'], a[aria-label*='Name'], a.pm-confirm-button, a[onclick*='onChooseClick']")
                                                if button:
                                                    btn_text = (await button.inner_text()).strip().lower()
                                                    onclick_attr = await button.get_attribute("onclick") or ""
                                                    aria_label = await button.get_attribute("aria-label") or ""
                                                    
                                                    # Verify it's a Choose button
                                                    if "choose" in btn_text or "onChooseClick" in onclick_attr or "linkText" in aria_label:
                                                        choose_button = button
                                                        logging.info("✅ Found Choose button for %s using listitem method", court_name)
                                                        break
                                        except Exception:
                                            continue
                                    
                                    if choose_button:
                                        break
                                    
                                    # Method 2: Direct selector using text pattern
                                    try:
                                        # Find element containing "Choose Court XX" or "Court XX Read more"
                                        court_listitem = await page.query_selector(f"text=/Choose.*Court\\s+{court_num_formatted}|Court\\s+{court_num_formatted}.*Read more/i")
                                        if court_listitem:
                                            # Find the Choose button within this listitem or its parent
                                            button = await court_listitem.query_selector("a.pm-confirm-button, a[onclick*='onChooseClick'], a[aria-label*='linkText']")
                                            if not button:
                                                # Try parent
                                                parent = await court_listitem.evaluate_handle("el => el.closest('li, [role=\"listitem\"]')")
                                                if parent:
                                                    parent_elem = parent.as_element() if hasattr(parent, 'as_element') else None
                                                    if parent_elem:
                                                        button = await parent_elem.query_selector("a.pm-confirm-button, a[onclick*='onChooseClick'], a[aria-label*='linkText']")
                                            
                                            if button:
                                                choose_button = button
                                                logging.info("✅ Found Choose button for %s using direct text matching", court_name)
                                                break
                                    except Exception:
                                        pass
                                    
                                    # Method 3: Find by aria-label pattern and match to court
                                    try:
                                        # The aria-label is "#: linkText + ' ' + Name #"
                                        buttons = await page.query_selector_all("a[aria-label*='linkText'], a[aria-label*='Name'], a.pm-confirm-button")
                                        # Match by finding the one in the same listitem as the court
                                        for btn in buttons:
                                            try:
                                                # Get the listitem parent
                                                li_parent = await btn.evaluate_handle("el => el.closest('li, [role=\"listitem\"]')")
                                                if li_parent:
                                                    li_elem = li_parent.as_element() if hasattr(li_parent, 'as_element') else None
                                                    if li_elem:
                                                        li_text = await li_elem.inner_text()
                                                        # Check if this listitem contains the court number
                                                        if f"Court {court_num_formatted}" in li_text:
                                                            btn_text = (await btn.inner_text()).strip().lower()
                                                            onclick_attr = await btn.get_attribute("onclick") or ""
                                                            if "choose" in btn_text or "onChooseClick" in onclick_attr:
                                                                choose_button = btn
                                                                logging.info("✅ Found Choose button for %s using aria-label method", court_name)
                                                                break
                                            except Exception:
                                                continue
                                        if choose_button:
                                            break
                                    except Exception:
                                        pass
                                    
                                except Exception as exc:
                                    logging.debug("Error finding button: %s", exc)
                                
                                if not choose_button:
                                    await page.wait_for_timeout(button_check_interval)
                                    button_elapsed = (time.time() * 1000) - button_start_time
                            
                            if not choose_button:
                                logging.warning("Could not find Choose button for %s after waiting", court_name)
                                processed_courts.add(court_name)
                                continue
                    
                    except Exception as exc:
                        logging.warning("Error finding Choose button for %s: %s", court_name, exc)
                        processed_courts.add(court_name)
                        continue
                    
                    # Scroll the button into view and click it - with maximum patience
                    try:
                        # Scroll the button into view first
                        logging.debug("Scrolling to Choose button for %s...", court_name)
                        await choose_button.scroll_into_view_if_needed(timeout=5_000)
                        await page.wait_for_timeout(500)  # Wait after scrolling
                        
                        # Wait for button to be visible and enabled
                        logging.debug("Waiting for Choose button to be visible for %s...", court_name)
                        await choose_button.wait_for_element_state("visible", timeout=10_000)
                        await page.wait_for_timeout(500)
                        await choose_button.wait_for_element_state("stable", timeout=5_000)
                        await page.wait_for_timeout(500)
                        
                        # Verify we're still on the court list before clicking
                        court_list_check = await page.query_selector_all("text=/Court\\s+\\d+/i")
                        if len(court_list_check) < 3:
                            logging.warning("Court list not visible before clicking %s, waiting...", court_name)
                            await page.wait_for_timeout(2_000)
                            court_list_check = await page.query_selector_all("text=/Court\\s+\\d+/i")
                            if len(court_list_check) < 3:
                                logging.error("Not on court list page before clicking %s, skipping", court_name)
                                continue
                        
                        # Click the button
                        logging.info("Clicking 'Choose' button for %s...", court_name)
                        await choose_button.click(timeout=10_000)
                        logging.info("✅ Clicked 'Choose' for %s", court_name)
                        
                        # Brief wait for navigation to start
                        await page.wait_for_timeout(2_000)
                        
                    except Exception as exc:
                        logging.error("Failed to click Choose button for %s: %s", court_name, exc)
                        # Try alternative click method using JavaScript
                        try:
                            logging.info("Trying JavaScript click for %s...", court_name)
                            await choose_button.scroll_into_view_if_needed(timeout=5_000)
                            await page.wait_for_timeout(500)
                            await choose_button.evaluate("el => el.click()")
                            await page.wait_for_timeout(2_000)
                            logging.info("✅ Successfully clicked using JavaScript for %s", court_name)
                        except Exception as js_exc:
                            logging.error("JavaScript click also failed for %s: %s", court_name, js_exc)
                            continue
                    
                    # Wait for the schedule page to load and VERIFY we're on the correct court's page
                    logging.info("Waiting for schedule page to load for %s...", court_name)
                    schedule_loaded = await _wait_for_schedule_page(page, court_name, max_wait=30_000)
                    
                    if not schedule_loaded:
                        logging.error("❌ FAILED to verify schedule page for %s. Skipping this court to avoid wrong data.", court_name)
                        # Do NOT scrape if we can't verify - this prevents getting wrong court data
                        continue
                    
                    # Double-check one more time before scraping
                    final_verification = await _verify_court_on_page(page, court_name)
                    if not final_verification:
                        logging.error("❌ Final verification failed for %s. Page may have changed. Skipping.", court_name)
                        continue
                    
                    # Give it a bit more time for calendar to fully render
                    logging.info("✅ Verified on correct page for %s, waiting for calendar to render...", court_name)
                    await page.wait_for_timeout(2_000)
                    
                    # Scrape the schedule
                    court_entries = await _scrape_court_schedule(page, court_name)
                    results.extend(court_entries)
                    logging.info("Scraped %d slots from %s", len(court_entries), court_name)
                    
                    # Mark this court as processed
                    processed_courts.add(court_name)
                    
                    # Navigate back to the court list - try multiple methods
                    navigation_success = False
                    
                    # Method 1: Try going back
                    try:
                        logging.info("Navigating back from %s schedule page...", court_name)
                        
                        # Store current URL to check if we actually navigated
                        schedule_url = page.url
                        logging.debug("Current URL before go_back: %s", schedule_url[:100])
                        
                        # Try go_back with longer timeout and wait for navigation
                        try:
                            await page.go_back(timeout=15_000)
                            # Wait for navigation to complete
                            await page.wait_for_load_state("domcontentloaded", timeout=5_000)
                            await page.wait_for_timeout(1_000)  # Brief wait for page to settle
                        except Exception as nav_exc:
                            # Even if timeout, check if we navigated
                            logging.debug("go_back() had timeout/error, but checking if navigation occurred: %s", nav_exc)
                        
                        # Check if URL changed (indicates successful navigation)
                        new_url = page.url
                        logging.debug("URL after go_back: %s", new_url[:100])
                        
                        if new_url != schedule_url:
                            logging.info("✅ URL changed after go_back() - navigation detected")
                            
                            # Check if we're on the court list page by looking for court elements
                            try:
                                # Quick check: look for court list indicators
                                await page.wait_for_timeout(1_000)  # Give page time to render
                                court_elements = await page.query_selector_all("text=/Court\\s+\\d+/i")
                                logging.debug("Found %d court elements after go_back", len(court_elements))
                                
                                if len(court_elements) >= 3:
                                    # We're likely on the court list page
                                    logging.info("✅ Successfully navigated back (URL changed, %d courts visible)", len(court_elements))
                                    navigation_success = True
                                    
                                    # Wait a bit more for full load, but don't block if it's already working
                                    await page.wait_for_timeout(1_000)
                                    
                                    # Verify with a more thorough check (but don't wait too long)
                                    court_list_loaded = await _wait_for_court_list_loaded(page, expected_court_count=10, max_wait=5_000)
                                    if court_list_loaded:
                                        logging.info("✅ Court list fully loaded")
                                    else:
                                        logging.debug("Court list partially loaded, but proceeding (go_back worked)")
                                    
                                    # If next court is Court 12 or 13, ensure page size is 20
                                    remaining_courts = [c for c in court_names if c not in processed_courts]
                                    if remaining_courts and remaining_courts[0] in ["Court 12", "Court 13"]:
                                        logging.info("Next court is %s, ensuring page size is 20...", remaining_courts[0])
                                        await _ensure_page_size_20(page)
                                        await page.wait_for_timeout(2_000)
                                        try:
                                            await page.wait_for_load_state("networkidle", timeout=5_000)
                                        except Exception:
                                            pass
                                else:
                                    logging.warning("URL changed but not seeing court list (only %d courts found)", len(court_elements))
                            except Exception as check_exc:
                                logging.debug("Error checking court list after go_back: %s", check_exc)
                        else:
                            logging.debug("URL did not change after go_back() - may need fallback")
                            
                    except Exception as exc:
                        logging.warning("go_back() failed: %s", exc)
                    
                    # Method 2: If go_back failed or didn't work, navigate directly to the booking page
                    if not navigation_success:
                        try:
                            logging.info("go_back() failed, navigating directly to booking page")
                            await page.goto(base_url, wait_until="domcontentloaded", timeout=30_000)
                            await page.wait_for_timeout(2_000)
                            
                            # Click "Book a Court" again
                            book_button_selectors = [
                                "text='Book a Court'",
                                "a:has-text('Book a Court')",
                                "button:has-text('Book a Court')",
                            ]
                            clicked = False
                            for selector in book_button_selectors:
                                try:
                                    await page.click(selector, timeout=10_000)
                                    clicked = True
                                    break
                                except Exception:
                                    continue
                            
                            if clicked:
                                # Wait for court list to fully load
                                # Use flexible count
                                court_list_loaded = await _wait_for_court_list_loaded(page, expected_court_count=10, max_wait=30_000)
                                if court_list_loaded:
                                    navigation_success = True
                                    logging.info("✅ Successfully navigated back by re-clicking Book a Court (fully loaded)")
                                else:
                                    logging.warning("Court list loaded but may not be complete")
                                    # Still try to continue
                                    navigation_success = True
                        except Exception as exc:
                            logging.warning("Failed to navigate back from %s: %s", court_name, exc)
                    
                    if not navigation_success:
                        logging.error("Could not navigate back from %s, stopping scraper", court_name)
                        break
                        
                except Exception as exc:
                    logging.exception("Failed to scrape %s: %s", court_name, exc)
                    # Still mark as processed to avoid infinite retries
                    processed_courts.add(court_name)
                    continue
            
            logging.info("Scraping complete. Found %d total slots", len(results))
            
            # Final progress update
            if progress_callback:
                try:
                    progress_callback(
                        current=len(court_names),
                        total=len(court_names),
                        message="Scraping complete!",
                        court_name=None
                    )
                except Exception:
                    pass
            
        finally:
            await browser.close()
    return results


def get_supabase_client() -> Optional[Client]:
    """Create and return Supabase client if credentials are available."""
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    
    if not supabase_url or not supabase_key:
        return None
    
    try:
        return create_client(supabase_url, supabase_key)
    except Exception as e:
        logging.warning(f"Failed to create Supabase client: {e}")
        return None


def clean_and_upload_to_supabase(json_data: List[Dict]) -> bool:
    """Clean and upload JSON data to Supabase.
    
    Truncates the table and inserts all new data in one shot.
    Returns True if successful, False otherwise.
    """
    supabase = get_supabase_client()
    if not supabase:
        logging.warning("Supabase client not available, skipping upload")
        return False
    
    try:
        clean_rows = []
        
        for item in json_data:
            # Parse time string to datetime
            time_str = item.get('time', '')
            if not time_str:
                continue
            
            # Try different time formats
            dt_object = None
            time_formats = [
                "%Y-%m-%d %I:%M %p",  # "2025-12-18 08:00 AM"
                "%Y-%m-%d %H:%M",     # "2025-12-18 08:00"
            ]
            
            for fmt in time_formats:
                try:
                    dt_object = datetime.strptime(time_str, fmt)
                    break
                except ValueError:
                    continue
            
            if dt_object is None:
                logging.warning(f"Could not parse time: {time_str}")
                continue
            
            # Map JSON keys to SQL Schema columns
            row = {
                "court_name": item.get('court', ''),
                "start_time": dt_object.isoformat(),  # Supabase likes ISO strings
                "status": item.get('status', ''),
                "booking_link": item.get('link', ''),
                "raw_text": item.get('raw_text', ''),
                "updated_at": datetime.now(timezone.utc).isoformat()  # Mark when we last saw it
            }
            clean_rows.append(row)
        
        # Step 1: Truncate the table (delete all existing rows)
        try:
            # Delete all rows by using a condition that matches all rows
            # Using .gte() with a very old date that will match all rows
            old_date = datetime(1970, 1, 1).isoformat()
            delete_response = supabase.table('court_slots').delete().gte('start_time', old_date).execute()
            logging.info("Truncated court_slots table")
        except Exception as e:
            logging.warning(f"Failed to truncate table: {e}. Proceeding with insert anyway.")
        
        if not clean_rows:
            logging.warning("No rows to upload to Supabase")
            return False
        
        # Step 2: Insert all new data
        response = supabase.table('court_slots').insert(clean_rows).execute()
        
        logging.info(f"Inserted {len(clean_rows)} slots to Supabase (table was truncated first).")
        return True
        
    except Exception as e:
        logging.error(f"Failed to upload to Supabase: {e}")
        return False


def load_cache(path: Path = DATA_PATH) -> tuple[List[Dict], str]:
    """Load cache from Supabase first, fallback to JSON file.
    
    Returns:
        Tuple of (data, source) where source is 'supabase', 'json', or 'none'
    """
    # Try Supabase first
    supabase = get_supabase_client()
    if supabase:
        try:
            # Query all slots from Supabase
            response = supabase.table('court_slots').select("*").execute()
            
            if response.data and len(response.data) > 0:
                # Convert Supabase rows back to JSON format
                json_data = []
                for row in response.data:
                    # Parse ISO datetime back to the format we use
                    start_time_str = row['start_time']
                    # Handle different ISO formats
                    if start_time_str.endswith('Z'):
                        start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
                    else:
                        start_time = datetime.fromisoformat(start_time_str)
                    
                    time_str = start_time.strftime("%Y-%m-%d %I:%M %p")
                    
                    json_data.append({
                        'court': row['court_name'],
                        'time': time_str,
                        'status': row.get('status', ''),
                        'link': row.get('booking_link', ''),
                        'raw_text': row.get('raw_text', '')
                    })
                
                logging.info(f"Loaded {len(json_data)} slots from Supabase")
                return json_data, 'supabase'
            else:
                logging.info("No data found in Supabase, falling back to JSON")
        except Exception as e:
            logging.warning(f"Failed to load from Supabase: {e}, falling back to JSON")
            import traceback
            logging.debug(traceback.format_exc())
    
    # Fallback to JSON file
    if path.exists():
        try:
            data = json.loads(path.read_text())
            logging.info(f"Loaded {len(data)} slots from JSON file")
            return data, 'json'
        except json.JSONDecodeError:
            logging.warning("Cache file invalid JSON, ignoring.")
    
    return [], 'none'


def process_notifications(new_slots: List[Dict]) -> None:
    """Process notifications for matching subscriptions.
    
    Args:
        new_slots: List of dictionaries with scraped court slot data
    """
    supabase = get_supabase_client()
    if not supabase:
        logging.warning("Supabase client not available, skipping notifications")
        return
    
    bot_token = os.getenv("BOT_TOKEN")
    if not bot_token:
        logging.warning("BOT_TOKEN not set, skipping notifications")
        return
    
    try:
        # Fetch all subscriptions
        response = supabase.table('subscriptions').select("*").execute()
        subscriptions = response.data if response.data else []
        
        if not subscriptions:
            logging.debug("No subscriptions found, skipping notifications")
            return
        
        logging.info(f"Processing notifications for {len(subscriptions)} subscriptions and {len(new_slots)} slots")
        
        # Process each slot
        for slot in new_slots:
            time_str = slot.get('time', '')
            if not time_str:
                continue
            
            # Parse time string to datetime
            dt_object = None
            time_formats = [
                "%Y-%m-%d %I:%M %p",  # "2025-12-18 08:00 AM"
                "%Y-%m-%d %H:%M",     # "2025-12-18 08:00"
            ]
            
            for fmt in time_formats:
                try:
                    dt_object = datetime.strptime(time_str, fmt)
                    break
                except ValueError:
                    continue
            
            if dt_object is None:
                continue
            
            # Extract day name and hour (local time, no timezone conversion)
            day_name = dt_object.strftime("%A")  # Monday, Tuesday, etc.
            hour = dt_object.hour  # 0-23
            
            # Check against each subscription
            for sub in subscriptions:
                sub_day = sub.get('day_of_week', '')
                start_hour = sub.get('start_hour', 0)
                end_hour = sub.get('end_hour', 23)
                chat_id = sub.get('chat_id', '')
                
                # Check if day matches and hour is within range
                if day_name == sub_day and start_hour <= hour <= end_hour:
                    # Match found! Send Telegram notification
                    try:
                        message = f"🎾 *Match Found!*\n\n{slot.get('court', 'Court')} is available {day_name} at {time_str}.\n{slot.get('link', '#')}"
                        
                        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
                        payload = {
                            'chat_id': chat_id,
                            'text': message,
                            'parse_mode': 'Markdown'
                        }
                        
                        response = requests.post(url, json=payload, timeout=10)
                        response.raise_for_status()
                        
                        logging.info(f"Sent notification to chat_id {chat_id} for {slot.get('court')} at {time_str}")
                    except Exception as e:
                        logging.error(f"Failed to send notification to chat_id {chat_id}: {e}")
    
    except Exception as e:
        logging.error(f"Error processing notifications: {e}")


def save_cache(data: List[Dict], path: Path = DATA_PATH) -> None:
    """Save cache to both JSON file (backup) and Supabase."""
    # Always save to JSON file as backup
    path.write_text(json.dumps(data, indent=2))
    logging.info(f"Saved {len(data)} slots to JSON file")
    
    # Also save to Supabase
    clean_and_upload_to_supabase(data)
    
    # Process notifications after saving
    process_notifications(data)


async def main(court_names: Optional[List[str]] = None) -> None:
    data = await scrape_courts(court_names=court_names)
    save_cache(data)
    logging.info("Scraped %d slots", len(data))


if __name__ == "__main__":
    # Test with only Court 01
    asyncio.run(main(court_names=None))  # None = discover all courts

