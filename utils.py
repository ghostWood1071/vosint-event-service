import uuid
from datetime import datetime, timedelta

import pytz
import unidecode


def get_time_now_string(
    tz: str = "Asia/Ho_Chi_Minh", fmt: str = "%Y/%m/%d %H:%M:%S"
) -> str:
    return datetime.now(pytz.timezone(tz)).strftime(fmt)

def get_time_now_string_y_m_d(
    tz: str = "Asia/Ho_Chi_Minh", fmt: str = "%Y/%m/%d"
) -> str:
    return datetime.now(pytz.timezone(tz)).strftime(fmt)
def get_time_now_string_y_m_now(
    tz: str = "Asia/Ho_Chi_Minh", fmt: str = "%Y/%m/%d"
) -> str:
    return datetime.strptime(str(get_time_now_string_y_m_d()), "%Y/%m/%d")
    
def gen_id(length: int = 12) -> str:
    return str(uuid.uuid4().hex[-length:])


def norm_text(text: str) -> str:
    text_lower = text.lower()
    text_removed_accent = unidecode.unidecode(text_lower)
    return text_removed_accent

def get_day_in_week():
    current_date = datetime.now()
    start_of_week = current_date - timedelta(days=current_date.weekday())
    end_of_week = start_of_week + timedelta(days=6)
    formatted_start_of_week = start_of_week.strftime("%d-%m-%YT%H:%M:%S")
    formatted_end_of_week = end_of_week.strftime("%d-%m-%YT%H:%M:%S")
    return formatted_start_of_week, formatted_end_of_week
