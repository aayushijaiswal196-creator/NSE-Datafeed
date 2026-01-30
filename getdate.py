from datetime import datetime, timedelta, timezone

def get_current_date():
    # Define IST timezone offset (UTC + 5:30)
    ist_offset = timedelta(hours=5, minutes=30)
    
    # Get current UTC time and convert to IST
    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc + ist_offset
    
    # Check if current IST time is before 19:30
    cutoff_time = now_ist.replace(hour=19, minute=30, second=0, microsecond=0)
    
    if now_ist < cutoff_time:
        # Return yesterday's date
        target_date = now_ist - timedelta(days=1)
    else:
        # Return today's date
        target_date = now_ist
        
    return target_date.strftime("%d%m%Y")

#print(get_current_date())
