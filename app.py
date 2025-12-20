import asyncio
import json
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable

import pytz
import requests
import streamlit as st
from dotenv import load_dotenv
from streamlit_calendar import calendar

from scraper import DATA_PATH, load_cache, save_cache, scrape_courts, get_supabase_client

load_dotenv()


def _format_readable_date(dt: datetime) -> str:
    """Format datetime to readable format like 'Wed 12th 2025'."""
    # Get day suffix (1st, 2nd, 3rd, 4th, etc.)
    day = dt.day
    if 10 <= day % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
    
    return dt.strftime(f"%a {day}{suffix} %Y")


def _format_time_range(start_dt: datetime, end_dt: datetime) -> str:
    """Format time range without repeating date if same day.
    Example: 'Wed 12th 2025 08:00PM - 10:00PM' or 'Wed 12th 2025 08:00PM - Thu 13th 2025 10:00PM'"""
    start_date_str = _format_readable_date(start_dt)
    start_time_str = start_dt.strftime("%I:%M%p")
    
    # Check if same day
    if start_dt.date() == end_dt.date():
        end_time_str = end_dt.strftime("%I:%M%p")
        return f"{start_date_str} {start_time_str} - {end_time_str}"
    else:
        end_date_str = _format_readable_date(end_dt)
        end_time_str = end_dt.strftime("%I:%M%p")
        return f"{start_date_str} {start_time_str} - {end_date_str} {end_time_str}"


def _format_single_time(dt: datetime) -> str:
    """Format single datetime to readable format.
    Example: 'Wed 12th 2025 08:00PM'"""
    date_str = _format_readable_date(dt)
    time_str = dt.strftime("%I:%M%p")
    return f"{date_str} {time_str}"


def _parse_time(value: str) -> Optional[datetime]:
    """Parse time string from various formats."""
    if not value:
        return None
    
    # Try common datetime patterns
    patterns = [
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %I:%M %p",
        "%m/%d/%Y %I:%M %p",
        "%m-%d-%Y %I:%M %p",
        "%d/%m/%Y %I:%M %p",
        "%d-%m-%Y %I:%M %p",
    ]
    
    for pattern in patterns:
        try:
            return datetime.strptime(value.strip(), pattern)
        except ValueError:
            continue
    
    # Try to extract date and time from text
    # Look for patterns like "10:00 AM", "2:00 PM"
    time_match = re.search(r'(\d{1,2}):(\d{2})\s*(AM|PM|am|pm)', value)
    if time_match:
        hour = int(time_match.group(1))
        minute = int(time_match.group(2))
        am_pm = time_match.group(3).upper()
        
        if am_pm == "PM" and hour != 12:
            hour += 12
        elif am_pm == "AM" and hour == 12:
            hour = 0
        
        # Use today's date as default
        today = datetime.now().replace(hour=hour, minute=minute, second=0, microsecond=0)
        return today
    
    return None


def group_by_day(data: List[Dict]) -> Dict[str, List[Dict]]:
    grouped: Dict[str, List[Dict]] = defaultdict(list)
    for entry in data:
        dt = _parse_time(entry.get("time", ""))
        label = dt.strftime("%A") if dt else "Unknown Day"
        grouped[label].append(entry)
    return grouped


def notify_ntfy(topic: str, message: str, base_url: str = "https://ntfy.sh") -> requests.Response:
    url = f"{base_url.rstrip('/')}/{topic}"
    return requests.post(url, data=message.encode("utf-8"), timeout=10)


def get_pacific_time() -> datetime:
    """Get current time in Pacific timezone (America/Vancouver)."""
    pacific = pytz.timezone('America/Vancouver')
    return datetime.now(pacific)








def run_sniper(force: bool = False, headless: bool = True) -> tuple[List[Dict], str]:
    """Run scraper and return data with source indicator."""
    data, source = load_cache()
    if force or not data:
        data = asyncio.run(scrape_courts(headless=headless))
        save_cache(data, DATA_PATH)
    
    # Update last run time in PST
    st.session_state['last_run_time'] = get_pacific_time().strftime('%Y-%m-%d %I:%M:%S %p %Z')
    # Determine data source
    data_source = 'supabase' if get_supabase_client() else 'json'
    return data, data_source


def render_hero(data: List[Dict]) -> None:
    """Render the hero section with next available slot."""
    parsed = [(item, _parse_time(item.get("time", ""))) for item in data]
    parsed = [item for item in parsed if item[1] is not None]
    parsed.sort(key=lambda x: x[1])
    if parsed:
        next_slot = parsed[0][0]
        st.markdown(
            f"### ✅ Next Slot Available: {next_slot['time']} ({next_slot['court']})"
        )
    else:
        st.markdown("### ❌ No Slots Available")


def find_one_hour_slots(data: List[Dict]) -> List[Dict]:
    """Find available one-hour slots (not part of a 2-hour booking)."""
    parsed = [(item, _parse_time(item.get("time", ""))) for item in data]
    parsed = [item for item in parsed if item[1] is not None]
    parsed.sort(key=lambda x: x[1])
    
    one_hour_slots = []
    seen_times = set()
    
    for item, dt in parsed:
        # Skip if this time is already part of a 2-hour slot
        time_key = (dt.date(), dt.hour)
        if time_key in seen_times:
            continue
        
        # Check if the next hour is also available (making it a 2-hour slot)
        next_hour = dt + timedelta(hours=1)
        
        is_two_hour = False
        for other_item, other_dt in parsed:
            if other_dt.date() == next_hour.date() and other_dt.hour == next_hour.hour:
                # Check if it's the same court
                if item['court'] == other_item['court']:
                    is_two_hour = True
                    seen_times.add((next_hour.date(), next_hour.hour))
                    break
        
        if not is_two_hour:
            # Format the time display
            formatted_time = _format_single_time(dt)
            one_hour_slots.append({
                **item,
                'formatted_time': formatted_time
            })
    
    return one_hour_slots[:3]  # Return top 3


def find_two_hour_slots(data: List[Dict]) -> List[Dict]:
    """Find available two-hour slots (back-to-back 1-hour slots)."""
    parsed = [(item, _parse_time(item.get("time", ""))) for item in data]
    parsed = [item for item in parsed if item[1] is not None]
    parsed.sort(key=lambda x: x[1])
    
    two_hour_slots = []
    seen_pairs = set()
    
    for i, (item, dt) in enumerate(parsed):
        # Check if the next hour is also available
        next_hour = dt + timedelta(hours=1)
        
        # Look for a slot in the next hour on the same court
        for j, (other_item, other_dt) in enumerate(parsed):
            if i != j and item['court'] == other_item['court']:
                if other_dt.date() == next_hour.date() and other_dt.hour == next_hour.hour:
                    # Avoid duplicates
                    pair_key = (dt, other_dt, item['court'])
                    if pair_key in seen_pairs:
                        continue
                    seen_pairs.add(pair_key)
                    
                    # Calculate end time (add 1 hour to the second slot's time)
                    end_time = other_dt + timedelta(hours=1)
                    
                    # Format the time range
                    formatted_time = _format_time_range(dt, end_time)
                    
                    # Found a 2-hour slot!
                    two_hour_slots.append({
                        'court': item['court'],
                        'start_time': item['time'],
                        'end_time': other_item['time'],
                        'formatted_time': formatted_time,
                        'status': 'Open (2 hours)',
                        'link': item.get('link', '#'),
                    })
                    break
        
        if len(two_hour_slots) >= 3:
            break
    
    return two_hour_slots[:3]  # Return top 3


def get_pastel_colors() -> Dict[str, str]:
    """Get pastel color mapping for courts."""
    # Pastel colors for individual courts (avoiding yellow for better white text contrast)
    pastel_colors = {
        "Court 01": "#FFB3BA",  # Pastel pink
        "Court 02": "#BAFFC9",  # Pastel green
        "Court 03": "#BAE1FF",  # Pastel blue
        "Court 04": "#B4C6E7",  # Pastel periwinkle (replaced yellow)
        "Court 05": "#FFDFBA",  # Pastel orange
        "Court 06": "#E0BBE4",  # Pastel purple
        "Court 07": "#FEC8C1",  # Pastel coral
        "Court 08": "#B5EAD7",  # Pastel mint
        "Court 09": "#C7CEEA",  # Pastel lavender
        "Court 10": "#FFD3A5",  # Pastel peach
        "Court 11": "#A8E6CF",  # Pastel seafoam
        "Court 12": "#FFAAA5",  # Pastel rose
        "Court 13": "#DDA0DD",  # Pastel plum
    }
    return pastel_colors


def group_slots_by_time_block(data: List[Dict]) -> List[Dict]:
    """Group slots by time blocks. If multiple courts are available at the same time, combine them.
    Also detect 2-hour slots (back-to-back 1-hour slots) and combine them."""
    parsed = [(item, _parse_time(item.get("time", ""))) for item in data]
    parsed = [item for item in parsed if item[1] is not None]
    parsed.sort(key=lambda x: x[1])
    
    # First, identify 2-hour slots (back-to-back 1-hour slots on same court)
    two_hour_slots_list = []
    processed_indices = set()
    
    for i, (item, dt) in enumerate(parsed):
        if i in processed_indices:
            continue
        
        # Check if next hour is also available on same court
        next_hour = dt + timedelta(hours=1)
        
        for j, (other_item, other_dt) in enumerate(parsed):
            if i != j and j not in processed_indices:
                if (item['court'] == other_item['court'] and 
                    other_dt.date() == next_hour.date() and 
                    other_dt.hour == next_hour.hour):
                    # Found a 2-hour slot!
                    two_hour_slots_list.append({
                        'start': dt,
                        'end': other_dt + timedelta(hours=1),
                        'court': item['court'],
                        'start_time_str': item['time'],
                        'end_time_str': other_item['time'],
                        'link': item.get('link', '#'),
                    })
                    processed_indices.add(i)
                    processed_indices.add(j)
                    break
    
    # Group 2-hour slots by time block (same start time, different courts)
    two_hour_time_blocks = defaultdict(list)
    for slot in two_hour_slots_list:
        block_key = (slot['start'].date(), slot['start'].hour)
        two_hour_time_blocks[block_key].append(slot)
    
    # Now group remaining slots by time (same time, different courts)
    time_blocks = defaultdict(list)
    
    for i, (item, dt) in enumerate(parsed):
        if i in processed_indices:
            continue
        
        # Group by date and hour
        block_key = (dt.date(), dt.hour)
        time_blocks[block_key].append(item)
    
    # Create calendar events
    events = []
    
    # Add grouped 2-hour slots
    for (date, hour), slots in two_hour_time_blocks.items():
        courts = sorted([slot['court'] for slot in slots], key=lambda x: int(re.search(r'\d+', x).group()))
        court_nums = [c.split()[-1] for c in courts]
        
        # Use first slot's times (all should have same start/end since they're grouped by time)
        first_slot = slots[0]
        start_str = first_slot['start'].strftime('%Y-%m-%dT%H:%M:%S')
        end_str = first_slot['end'].strftime('%Y-%m-%dT%H:%M:%S')
        
        # Create title
        if len(courts) == 1:
            title = f"Court {court_nums[0]}"
        else:
            title = f"Courts {', '.join(court_nums)}"
        
        events.append({
            'title': title,
            'start': start_str,
            'end': end_str,
            'resourceId': courts[0] if len(courts) == 1 else 'Multiple',
            'extendedProps': {
                'courts': courts,
                'isTwoHour': True,
                'link': first_slot['link'],
            }
        })
    
    # Add grouped 1-hour slots
    for (date, hour), items in time_blocks.items():
        courts = sorted([item['court'] for item in items], key=lambda x: int(re.search(r'\d+', x).group()))
        court_nums = [c.split()[-1] for c in courts]
        
        # Create title
        if len(courts) == 1:
            title = f"Court {court_nums[0]}"
        else:
            title = f"Courts {', '.join(court_nums)}"
        
        # Use first item's time for start
        start_dt = _parse_time(items[0]['time'])
        if start_dt is None:
            continue
        end_dt = start_dt + timedelta(hours=1)
        
        # Format dates for FullCalendar
        start_str = start_dt.strftime('%Y-%m-%dT%H:%M:%S')
        end_str = end_dt.strftime('%Y-%m-%dT%H:%M:%S')
        
        events.append({
            'title': title,
            'start': start_str,
            'end': end_str,
            'resourceId': courts[0] if len(courts) == 1 else 'Multiple',
            'extendedProps': {
                'courts': courts,
                'isTwoHour': False,
                'link': items[0].get('link', '#'),
            }
        })
    
    return events


def render_calendar_view(data: List[Dict]) -> None:
    """Render calendar view with grouped time blocks."""
    events = group_slots_by_time_block(data)
    
    if not events:
        st.info("No slots available to display")
        return
    
    # Get color mapping
    color_map = get_pastel_colors()
    multi_court_color = "#D3D3D3"  # Light gray for multiple courts (1-hour)
    two_hour_multi_color = "#FFA500"  # Orange for multiple 2-hour slots (1-hour)
    two_hour_multi_color = "#FFA500"  # Orange for multiple 2-hour slots
    
    # Configure calendar
    calendar_options = {
        "headerToolbar": {
            "left": "prev,next today",
            "center": "title",
            "right": "dayGridMonth,timeGridWeek,timeGridDay,listWeek",
        },
        "initialView": "timeGridWeek",
        "slotMinTime": "08:00:00",
        "slotMaxTime": "23:00:00",
        "height": "auto",
        "editable": False,
        "selectable": False,
    }
    
    # Add colors to events
    for event in events:
        courts = event['extendedProps']['courts']
        is_two_hour = event['extendedProps'].get('isTwoHour', False)
        
        if is_two_hour:
            # 2-hour slots: single court uses pastel color, multiple courts use special color
            if len(courts) == 1:
                # Single court 2-hour slots use the same pastel color as individual courts
                event['backgroundColor'] = color_map.get(courts[0], "#CCCCCC")
                event['borderColor'] = color_map.get(courts[0], "#CCCCCC")
            else:
                # Multiple court 2-hour slots use special color
                event['backgroundColor'] = two_hour_multi_color
                event['borderColor'] = two_hour_multi_color
        else:
            # 1-hour slots use regular colors
            if len(courts) == 1:
                event['backgroundColor'] = color_map.get(courts[0], "#CCCCCC")
                event['borderColor'] = color_map.get(courts[0], "#CCCCCC")
            else:
                event['backgroundColor'] = multi_court_color
                event['borderColor'] = multi_court_color
    
    # Render calendar
    calendar_events = calendar(events=events, options=calendar_options, key="tennis_calendar")
    
    # Add legend
    st.markdown("### Color Legend")
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**1-Hour Slots:**")
        st.markdown("Individual Courts:")
        for court, color in sorted(color_map.items(), key=lambda x: int(re.search(r'\d+', x[0]).group())):
            st.markdown(f'<div style="display: inline-block; width: 20px; height: 20px; background-color: {color}; border: 1px solid #ccc; margin-right: 5px; vertical-align: middle;"></div> {court}', unsafe_allow_html=True)
        st.markdown(f'<div style="display: inline-block; width: 20px; height: 20px; background-color: {multi_court_color}; border: 1px solid #ccc; margin-right: 5px; vertical-align: middle;"></div> Multiple Courts', unsafe_allow_html=True)
    
    with col2:
        st.markdown("**2-Hour Slots:**")
        st.markdown("Single court 2-hour slots use the same pastel colors as individual courts")
        st.markdown(f'<div style="display: inline-block; width: 20px; height: 20px; background-color: {two_hour_multi_color}; border: 1px solid #ccc; margin-right: 5px; vertical-align: middle;"></div> Multiple Courts (2 hours)', unsafe_allow_html=True)
        st.markdown("💡 **2-hour slots** are shown as longer blocks covering 2 consecutive hours")


def render_alerts_section() -> None:
    """Render the alerts/subscriptions section with form and subscription list."""
    supabase = get_supabase_client()
    if not supabase:
        st.warning("⚠️ Supabase not available. Alerts require Supabase connection.")
        return
    
    # Initialize chat_id in session state if not present
    if 'alert_chat_id' not in st.session_state:
        st.session_state['alert_chat_id'] = ""
    
    # Form to add new subscription
    with st.form("add_subscription_form"):
        st.markdown("### Add New Alert")
        chat_id = st.text_input("Chat ID", value=st.session_state['alert_chat_id'], help="Your Telegram chat ID")
        day_of_week = st.selectbox(
            "Day of Week",
            options=["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        )
        col1, col2 = st.columns(2)
        with col1:
            start_hour = st.number_input("Start Hour", min_value=0, max_value=23, value=8, step=1, help="Hour in 24-hour format (0-23)")
        with col2:
            end_hour = st.number_input("End Hour", min_value=0, max_value=23, value=22, step=1, help="Hour in 24-hour format (0-23)")
        
        submitted = st.form_submit_button("Add Alert")
        
        if submitted:
            if not chat_id:
                st.error("Please enter a Chat ID")
            elif start_hour > end_hour:
                st.error("Start hour must be less than or equal to end hour")
            else:
                try:
                    # Store chat_id in session state
                    st.session_state['alert_chat_id'] = chat_id
                    
                    # Insert subscription into Supabase
                    response = supabase.table('subscriptions').insert({
                        'chat_id': chat_id,
                        'day_of_week': day_of_week,
                        'start_hour': start_hour,
                        'end_hour': end_hour
                    }).execute()
                    
                    st.success(f"✅ Alert added successfully!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to add alert: {e}")
    
    # Display current subscriptions
    st.markdown("### My Alerts")
    
    # Search box for Chat ID
    col_search1, col_search2 = st.columns([3, 1])
    with col_search1:
        search_chat_id = st.text_input("Search by Chat ID", key="search_chat_id", placeholder="Enter your Telegram Chat ID", help="Enter your Chat ID to view and manage your alerts")
    with col_search2:
        search_button = st.button("Search", key="search_alerts_button", use_container_width=True)
    
    # Initialize search result in session state
    if 'searched_chat_id' not in st.session_state:
        st.session_state['searched_chat_id'] = None
    
    # Update searched_chat_id when search button is clicked
    if search_button:
        if search_chat_id:
            st.session_state['searched_chat_id'] = search_chat_id
        else:
            st.warning("Please enter a Chat ID to search")
            st.session_state['searched_chat_id'] = None
    
    # Get the chat_id to filter by (from search)
    chat_id_filter = st.session_state.get('searched_chat_id', None)
    
    if chat_id_filter:
        try:
            # Fetch subscriptions for this chat_id
            response = supabase.table('subscriptions').select("*").eq('chat_id', chat_id_filter).execute()
            
            if response.data and len(response.data) > 0:
                st.success(f"Found {len(response.data)} alert(s) for Chat ID: {chat_id_filter}")
                for sub in response.data:
                    col1, col2, col3 = st.columns([3, 1, 1])
                    with col1:
                        st.write(f"**{sub['day_of_week']}** {sub['start_hour']:02d}:00 - {sub['end_hour']:02d}:00")
                    with col2:
                        # Delete button
                        sub_id = sub.get('id') or f"{sub['chat_id']}_{sub['day_of_week']}_{sub['start_hour']}_{sub['end_hour']}"
                        if st.button("Delete", key=f"delete_{sub_id}"):
                            try:
                                # Try to delete by id first, then by all fields
                                if 'id' in sub:
                                    delete_response = supabase.table('subscriptions').delete().eq('id', sub['id']).execute()
                                else:
                                    # Delete by matching all fields
                                    delete_response = supabase.table('subscriptions').delete().eq('chat_id', sub['chat_id']).eq('day_of_week', sub['day_of_week']).eq('start_hour', sub['start_hour']).eq('end_hour', sub['end_hour']).execute()
                                st.success("✅ Alert deleted!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Failed to delete alert: {e}")
                    with col3:
                        st.write("")  # Spacer
            else:
                st.info(f"No alerts found for Chat ID: {chat_id_filter}")
        except Exception as e:
            st.error(f"Failed to load alerts: {e}")
    elif search_button and not search_chat_id:
        st.info("Please enter a Chat ID and click Search to view alerts.")
    elif not chat_id_filter:
        st.info("Enter a Chat ID above and click Search to view and manage your alerts.")


def render_feed(data: List[Dict], notifications_enabled: bool, topic: str, ntfy_url: str) -> None:
    grouped = group_by_day(data)
    for day, entries in grouped.items():
        st.subheader(day)
        for entry in entries:
            label = f"Book {entry['court']} @ {entry['time']}"
            link = entry.get("link") or "#"
            st.link_button(label, link, use_container_width=True)
            if notifications_enabled and topic:
                st.caption(f"Alert sent to ntfy topic '{topic}' when refreshed.")


def main() -> None:
    st.set_page_config(page_title="UBC Tennis Court Sniper", layout="wide")
    st.title("🎾 UBC Tennis Court Sniper")

    # Initialize session state
    if 'last_run_time' not in st.session_state:
        st.session_state['last_run_time'] = None

    # Load data initially
    data, data_source = run_sniper(force=False, headless=True)
    
    # Display last run time in PST
    if st.session_state.get('last_run_time'):
        st.markdown(f"**Last Scraped (PST):** {st.session_state['last_run_time']}")

    # Display data source indicator
    if data_source == 'supabase':
        st.success("✅ Connected to Supabase")
    elif data_source == 'json':
        st.warning("⚠️ Using JSON file (Supabase unavailable)")
    else:
        st.error("❌ No data available. Please run scraper.")

    render_hero(data)
    
    # Next 3 one-hour slots
    st.markdown("---")
    st.subheader("⏰ Next 3 One-Hour Slots")
    one_hour_slots = find_one_hour_slots(data)
    if one_hour_slots:
        for slot in one_hour_slots:
            # Use formatted_time if available, otherwise fall back to original time
            time_display = slot.get('formatted_time', slot.get('time', ''))
            label = f"Book {slot['court']} @ {time_display}"
            link = slot.get("link") or "#"
            st.link_button(label, link, use_container_width=True)
    else:
        st.info("No one-hour slots available")
    
    # Next 3 two-hour slots
    st.markdown("---")
    st.subheader("⏰⏰ Next 3 Two-Hour Slots")
    two_hour_slots = find_two_hour_slots(data)
    if two_hour_slots:
        for slot in two_hour_slots:
            # Use formatted_time if available, otherwise fall back to original time
            time_display = slot.get('formatted_time', slot.get('time', ''))
            label = f"Book {slot['court']} @ {time_display}"
            link = slot.get("link") or "#"
            st.link_button(label, link, use_container_width=True)
    else:
        st.info("No two-hour slots available")
    
    # Calendar view
    st.markdown("---")
    st.subheader("📅 Calendar View")
    render_calendar_view(data)
    
    # Full feed (collapsible)
    with st.expander("📋 List View (All Available Slots)"):
        render_feed(data, False, "", "")
    
    # Alerts section
    st.markdown("---")
    st.subheader("🔔 Alerts")
    render_alerts_section()


if __name__ == "__main__":
    main()

