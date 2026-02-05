import requests

url = "https://api.ortex.com/api/v1/stock/us/AAPL/closing_prices?from_date=2023-01-01&to_date=2025-02-01&volume_from_all_exchanges=true"

headers = {
    "accept": "application/json",
    "Ortex-Api-Key": "TEST"
}

response = requests.get(url, headers=headers)

print(response.text)