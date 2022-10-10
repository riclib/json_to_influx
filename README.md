# json_to_openmetrics

Allows you to convert a json array to openmetrics.

Expected input format:
[
  {
    "time": "2022-01-14",
    "low": 3,
    "moderate": 0,
    "high": 0
  },
  {
    "time": "2022-01-14",
    "low": 3,
    "moderate": 0,
    "high": 0
  },
]

run with -h for flags to configure time field, format and input
