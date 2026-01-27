"""Debug script to check xG/xA name matching issues."""
from app.api.dashboard.service import normalize_name, _fetch_understat_squad_stats

# Get Understat data for 2024 season (current)
us_data = _fetch_understat_squad_stats(2024)
print(f"Total players in 2024 season: {len(us_data)}")

# Check for Mohamed Salah
print("\n--- Mohamed Salah lookup ---")
salah_variants = ['Mohamed Salah', 'M. Salah', 'Salah', 'Mohamed Salah Ghaly']
for name in salah_variants:
    norm = normalize_name(name)
    print(f'{name} -> "{norm}" -> found: {norm in us_data}')

# Show some sample Understat names
print('\nSample Understat names (first 20):')
for i, name in enumerate(list(us_data.keys())[:20]):
    print(f'  {i+1}. "{name}"')
    
# Try to find Salah with partial match
print('\nPartial matches for "salah":')
for name in us_data.keys():
    if 'salah' in name:
        print(f'  "{name}" -> xG={us_data[name]["xG"]:.2f}')

# Check some Aston Villa players
print('\nPartial matches for "morgan" (Rogers):')
for name in us_data.keys():
    if 'morgan' in name:
        print(f'  "{name}" -> xG={us_data[name]["xG"]:.2f}')

print('\nPartial matches for "watkins":')
for name in us_data.keys():
    if 'watkins' in name:
        print(f'  "{name}" -> xG={us_data[name]["xG"]:.2f}')
