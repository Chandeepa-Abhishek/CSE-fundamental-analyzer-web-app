"""
Test CSE API endpoints to discover available historical data
"""
import requests
import json
from datetime import datetime

base_url = 'https://www.cse.lk'

print('=' * 70)
print('TESTING CSE API ENDPOINTS FOR HISTORICAL DATA')
print('=' * 70)

# Test 1: Historical Data endpoint with long date range
print('\n1. Testing /api/historicalData (5 years)...')
try:
    r = requests.get(
        f'{base_url}/api/historicalData', 
        params={
            'symbol': 'JKH.N0000', 
            'startDate': '2020-01-01', 
            'endDate': '2026-01-06'
        }, 
        timeout=15
    )
    print(f'   Status: {r.status_code}')
    if r.status_code == 200:
        data = r.json()
        if isinstance(data, list):
            print(f'   Total records: {len(data)}')
            if len(data) > 0:
                print(f'   OLDEST record: {data[-1] if len(data) > 0 else "N/A"}')
                print(f'   NEWEST record: {data[0] if len(data) > 0 else "N/A"}')
                # Check date range
                dates = [d.get('date', d.get('Date', '')) for d in data if d.get('date') or d.get('Date')]
                if dates:
                    print(f'   Date range: {min(dates)} to {max(dates)}')
        elif isinstance(data, dict):
            print(f'   Keys: {list(data.keys())}')
            hist = data.get('data', data.get('historicalData', []))
            print(f'   Records in data: {len(hist) if isinstance(hist, list) else "N/A"}')
except Exception as e:
    print(f'   Error: {e}')

# Test 2: Try 10 year range
print('\n2. Testing /api/historicalData (10 years)...')
try:
    r = requests.get(
        f'{base_url}/api/historicalData', 
        params={
            'symbol': 'JKH.N0000', 
            'startDate': '2015-01-01', 
            'endDate': '2026-01-06'
        }, 
        timeout=15
    )
    print(f'   Status: {r.status_code}')
    if r.status_code == 200:
        data = r.json()
        if isinstance(data, list):
            print(f'   Total records: {len(data)}')
except Exception as e:
    print(f'   Error: {e}')

# Test 3: Chart Data endpoint
print('\n3. Testing /api/chartData...')
try:
    r = requests.get(f'{base_url}/api/chartData', params={'symbol': 'JKH.N0000'}, timeout=15)
    print(f'   Status: {r.status_code}')
    if r.status_code == 200:
        data = r.json()
        if isinstance(data, list):
            print(f'   Records: {len(data)}')
            if len(data) > 0:
                print(f'   First: {data[0]}')
                print(f'   Last: {data[-1]}')
        elif isinstance(data, dict):
            print(f'   Keys: {list(data.keys())}')
except Exception as e:
    print(f'   Error: {e}')

# Test 4: Company Info Summary
print('\n4. Testing /api/companyInfoSummery...')
try:
    r = requests.get(f'{base_url}/api/companyInfoSummery', params={'symbol': 'JKH.N0000'}, timeout=15)
    print(f'   Status: {r.status_code}')
    if r.status_code == 200:
        data = r.json()
        if isinstance(data, dict):
            print(f'   Keys: {list(data.keys())}')
            # Print some key financial fields
            for key in ['eps', 'pe', 'nav', 'marketCap', 'dividendYield', 'roe', 'debtEquity']:
                if key in data:
                    print(f'   {key}: {data[key]}')
except Exception as e:
    print(f'   Error: {e}')

# Test 5: Trade Summary
print('\n5. Testing /api/tradeSummary...')
try:
    r = requests.get(f'{base_url}/api/tradeSummary', timeout=15)
    print(f'   Status: {r.status_code}')
    if r.status_code == 200:
        data = r.json()
        if isinstance(data, dict) and 'reqTradeSummery' in data:
            items = data['reqTradeSummery']
            print(f'   Companies: {len(items)}')
            if len(items) > 0:
                print(f'   Sample keys: {list(items[0].keys())}')
except Exception as e:
    print(f'   Error: {e}')

# Test 6: Company Financials
print('\n6. Testing /api/companyFinancials...')
try:
    r = requests.get(f'{base_url}/api/companyFinancials', params={'symbol': 'JKH.N0000'}, timeout=15)
    print(f'   Status: {r.status_code}')
    if r.status_code == 200:
        data = r.json()
        print(f'   Type: {type(data).__name__}')
        if isinstance(data, dict):
            print(f'   Keys: {list(data.keys())}')
        elif isinstance(data, list):
            print(f'   Records: {len(data)}')
            if len(data) > 0:
                print(f'   First item keys: {list(data[0].keys()) if isinstance(data[0], dict) else data[0]}')
except Exception as e:
    print(f'   Error: {e}')

# Test 7: Annual Reports
print('\n7. Testing /api/annualReports...')
try:
    r = requests.get(f'{base_url}/api/annualReports', params={'symbol': 'JKH.N0000'}, timeout=15)
    print(f'   Status: {r.status_code}')
    if r.status_code == 200:
        data = r.json()
        if isinstance(data, list):
            print(f'   Reports available: {len(data)}')
            for i, report in enumerate(data[:5]):
                print(f'   {i+1}. {report}')
        elif isinstance(data, dict):
            print(f'   Keys: {list(data.keys())}')
except Exception as e:
    print(f'   Error: {e}')

# Test 8: Financial Statements
print('\n8. Testing /api/financialStatements...')
try:
    r = requests.get(f'{base_url}/api/financialStatements', params={'symbol': 'JKH.N0000'}, timeout=15)
    print(f'   Status: {r.status_code}')
    if r.status_code == 200:
        data = r.json()
        print(f'   Type: {type(data).__name__}')
        if isinstance(data, dict):
            print(f'   Keys: {list(data.keys())}')
        elif isinstance(data, list):
            print(f'   Records: {len(data)}')
except Exception as e:
    print(f'   Error: {e}')

# Test 9: Price Volume Data
print('\n9. Testing /api/priceVolumeData...')
try:
    r = requests.get(f'{base_url}/api/priceVolumeData', params={'symbol': 'JKH.N0000'}, timeout=15)
    print(f'   Status: {r.status_code}')
    if r.status_code == 200:
        data = r.json()
        print(f'   Type: {type(data).__name__}')
        if isinstance(data, dict):
            print(f'   Keys: {list(data.keys())}')
except Exception as e:
    print(f'   Error: {e}')

# Test 10: Index History
print('\n10. Testing /api/indexHistory (ASPI)...')
try:
    r = requests.get(
        f'{base_url}/api/indexHistory', 
        params={
            'index': 'ASPI', 
            'startDate': '2015-01-01', 
            'endDate': '2026-01-06'
        }, 
        timeout=15
    )
    print(f'   Status: {r.status_code}')
    if r.status_code == 200:
        data = r.json()
        if isinstance(data, list):
            print(f'   Records: {len(data)}')
            if len(data) > 0:
                print(f'   First: {data[0]}')
                print(f'   Last: {data[-1]}')
except Exception as e:
    print(f'   Error: {e}')

# Test 11: Dividend history
print('\n11. Testing /api/dividendHistory...')
try:
    r = requests.get(f'{base_url}/api/dividendHistory', params={'symbol': 'JKH.N0000'}, timeout=15)
    print(f'   Status: {r.status_code}')
    if r.status_code == 200:
        data = r.json()
        if isinstance(data, list):
            print(f'   Dividend records: {len(data)}')
            for i, div in enumerate(data[:3]):
                print(f'   {i+1}. {div}')
        elif isinstance(data, dict):
            print(f'   Keys: {list(data.keys())}')
except Exception as e:
    print(f'   Error: {e}')

print('\n' + '=' * 70)
print('TEST COMPLETE')
print('=' * 70)
