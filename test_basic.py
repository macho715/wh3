import pandas as pd

# Basic test data
data = {
    'Date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'],
    'Incoming': [100, 50, 0, 75, 25],
    'Outgoing': [20, 30, 40, 15, 60]
}

df = pd.DataFrame(data)

# User provided logic
inv = 0  # initial_stock
inventory_list = []
for in_qty, out_qty in zip(df['Incoming'], df['Outgoing']):
    inv = inv + in_qty - out_qty   # Previous inv + inbound - outbound
    inventory_list.append(inv)

df['Inventory_loop'] = inventory_list

# Expected calculation
expected = [80, 100, 60, 120, 85]
df['Expected'] = expected

print("Test Results:")
print(df)

# Verify
match = (df['Inventory_loop'] == df['Expected']).all()
print(f"Assert test: {'PASS' if match else 'FAIL'}")

if match:
    print("SUCCESS: User logic works correctly!")
else:
    print("FAIL: Logic error found")

print(f"Final inventory: {inventory_list[-1]}") 