import pandas as pd

print("USER INVENTORY LOGIC TEST")
print("=" * 30)

# Test data
df = pd.DataFrame({
    'Incoming': [100, 50, 0, 75, 25],
    'Outgoing': [20, 30, 40, 15, 60]
})

print("Input data:")
print(df)

# User provided logic exactly as given
inv = 0  # initial_stock
inventory_list = []
for in_qty, out_qty in zip(df['Incoming'], df['Outgoing']):
    inv = inv + in_qty - out_qty   # Previous inv + inbound - outbound
    inventory_list.append(inv)

df['Inventory_loop'] = inventory_list

# Manual calculation for verification
df['Expected'] = [80, 100, 60, 120, 85]

print("\nResults:")
print(df)

# User's assert check
try:
    assert (df['Inventory_loop'] == df['Expected']).all()
    print("\nASSERT PASSED: User logic is correct!")
except AssertionError:
    print("\nASSERT FAILED: Logic error detected")

print(f"Final inventory: {inventory_list[-1]}")

# Save results
with open('demo_results.txt', 'w') as f:
    f.write("USER INVENTORY LOGIC TEST RESULTS\n")
    f.write("=" * 40 + "\n\n")
    f.write("Input data:\n")
    f.write(str(df[['Incoming', 'Outgoing']]) + "\n\n")
    f.write("Results:\n")
    f.write(str(df) + "\n\n")
    
    match = (df['Inventory_loop'] == df['Expected']).all()
    f.write(f"Assert test: {'PASS' if match else 'FAIL'}\n")
    f.write(f"Final inventory: {inventory_list[-1]}\n")
    
    if match:
        f.write("SUCCESS: User logic works correctly!\n")
    else:
        f.write("FAIL: Logic error found\n")

print("\nResults saved to demo_results.txt") 