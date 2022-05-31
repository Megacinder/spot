from pendulum import Date, DateTime, timezone, Time, duration

a = DateTime(2022, 5, 28, 10, 11, 13)
print(a.day_of_week)
print(a.weekday)
