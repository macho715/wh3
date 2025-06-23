import pandas as pd

def calculate_warehouse_inventory():
    """창고별 재고 계산 및 검증"""
    
    print("WAREHOUSE INVENTORY CALCULATION")
    print("=" * 50)
    
    try:
        # HVDC 일별 재고 데이터 읽기
        filename = 'HVDC_Comprehensive_Report_20250623_220958.xlsx'
        df = pd.read_excel(filename, sheet_name='📅_일별재고추적')
        
        print(f"Total data rows: {len(df)}")
        print(f"Columns: {list(df.columns)}")
        
        if 'Loc' in df.columns:
            warehouses = df['Loc'].unique()
            print(f"\nFound {len(warehouses)} warehouses:")
            for i, wh in enumerate(warehouses, 1):
                print(f"  {i}. {wh}")
            
            print("\n" + "="*50)
            print("WAREHOUSE INVENTORY CALCULATIONS")
            print("="*50)
            
            all_results = []
            
            # 각 창고별로 재고 계산
            for warehouse in warehouses:
                if pd.isna(warehouse) or warehouse == 'UNKNOWN':
                    continue
                    
                print(f"\n📦 WAREHOUSE: {warehouse}")
                print("-" * 40)
                
                # 해당 창고 데이터 필터링
                wh_data = df[df['Loc'] == warehouse].copy()
                wh_data = wh_data.sort_values('Date')
                
                print(f"Data rows for this warehouse: {len(wh_data)}")
                
                if len(wh_data) > 0 and 'Inbound' in wh_data.columns and 'Outbound' in wh_data.columns:
                    # 사용자 제공 재고 계산 로직 적용
                    initial_stock = 0
                    inv = initial_stock
                    inventory_list = []
                    
                    for _, row in wh_data.iterrows():
                        in_qty = row['Inbound']
                        out_qty = row['Outbound']
                        inv = inv + in_qty - out_qty   # 이전 inv에 입고-출고 반영
                        inventory_list.append(inv)
                    
                    wh_data['Inventory_loop'] = inventory_list
                    
                    # 통계 계산
                    total_inbound = wh_data['Inbound'].sum()
                    total_outbound = wh_data['Outbound'].sum()
                    final_inventory = inventory_list[-1] if inventory_list else 0
                    max_inventory = max(inventory_list) if inventory_list else 0
                    min_inventory = min(inventory_list) if inventory_list else 0
                    
                    print(f"Total Inbound: {total_inbound:,.0f}")
                    print(f"Total Outbound: {total_outbound:,.0f}")
                    print(f"Net Movement: {total_inbound - total_outbound:,.0f}")
                    print(f"Final Inventory: {final_inventory:,.0f}")
                    print(f"Max Inventory: {max_inventory:,.0f}")
                    print(f"Min Inventory: {min_inventory:,.0f}")
                    
                    # HVDC 기존 계산과 비교
                    if 'Closing' in wh_data.columns:
                        hvdc_final = wh_data['Closing'].iloc[-1] if len(wh_data) > 0 else 0
                        match = abs(final_inventory - hvdc_final) < 0.001
                        print(f"HVDC Final Inventory: {hvdc_final:,.0f}")
                        print(f"Calculation Match: {'✅ YES' if match else '❌ NO'}")
                        
                        if not match:
                            print(f"Difference: {final_inventory - hvdc_final:,.0f}")
                    
                    # 최근 5일 데이터 표시
                    if len(wh_data) >= 5:
                        print(f"\nRecent 5 days data:")
                        recent_data = wh_data.tail(5)[['Date', 'Inbound', 'Outbound', 'Inventory_loop']]
                        if 'Closing' in wh_data.columns:
                            recent_data = wh_data.tail(5)[['Date', 'Inbound', 'Outbound', 'Closing', 'Inventory_loop']]
                        print(recent_data.to_string(index=False))
                    
                    # 결과 저장
                    all_results.append({
                        'Warehouse': warehouse,
                        'Total_Inbound': total_inbound,
                        'Total_Outbound': total_outbound,
                        'Net_Movement': total_inbound - total_outbound,
                        'Final_Inventory': final_inventory,
                        'Max_Inventory': max_inventory,
                        'Min_Inventory': min_inventory,
                        'Data_Points': len(wh_data)
                    })
                    
                else:
                    print("No valid inventory data found")
            
            # 전체 요약
            if all_results:
                print(f"\n" + "="*50)
                print("WAREHOUSE INVENTORY SUMMARY")
                print("="*50)
                
                summary_df = pd.DataFrame(all_results)
                summary_df = summary_df.sort_values('Final_Inventory', ascending=False)
                
                print(summary_df.to_string(index=False))
                
                # 총계
                total_inbound = summary_df['Total_Inbound'].sum()
                total_outbound = summary_df['Total_Outbound'].sum()
                total_final_inventory = summary_df['Final_Inventory'].sum()
                
                print(f"\nGRAND TOTALS:")
                print(f"Total Inbound (All Warehouses): {total_inbound:,.0f}")
                print(f"Total Outbound (All Warehouses): {total_outbound:,.0f}")
                print(f"Total Final Inventory: {total_final_inventory:,.0f}")
                print(f"Net Movement: {total_inbound - total_outbound:,.0f}")
                
                # 결과를 파일로 저장
                with open('warehouse_inventory_results.txt', 'w', encoding='utf-8') as f:
                    f.write("WAREHOUSE INVENTORY CALCULATION RESULTS\n")
                    f.write("=" * 50 + "\n\n")
                    f.write("Summary by Warehouse:\n")
                    f.write(summary_df.to_string(index=False) + "\n\n")
                    f.write(f"GRAND TOTALS:\n")
                    f.write(f"Total Inbound: {total_inbound:,.0f}\n")
                    f.write(f"Total Outbound: {total_outbound:,.0f}\n")
                    f.write(f"Total Final Inventory: {total_final_inventory:,.0f}\n")
                    f.write(f"Net Movement: {total_inbound - total_outbound:,.0f}\n")
                
                print(f"\nResults saved to warehouse_inventory_results.txt")
                
                # 상위 5개 창고 하이라이트
                print(f"\nTOP 5 WAREHOUSES BY FINAL INVENTORY:")
                top5 = summary_df.head(5)
                for i, (_, row) in enumerate(top5.iterrows(), 1):
                    print(f"  {i}. {row['Warehouse']}: {row['Final_Inventory']:,.0f} units")
        
        else:
            print("No warehouse location data found")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    calculate_warehouse_inventory() 