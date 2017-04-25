def timestamp_from_str(timestamp_str):
  """ Builds a timestamp tuple from a timestamp string of the format:
      "year-month-day_hour-minute"
  Returns: (<year>, <month>, <day>, <hour>, <minute>)
  """
  year_month_day, hour_minute = timestamp_str.split('_')
  year, month, day = year_month_day.split('-')
  hour, minute = hour_minute.split('-')
  return (int(year), int(month), int(day), int(hour), int(minute))
