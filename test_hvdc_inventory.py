# test_hvdc_inventory.py - HVDC 실제 데이터로 재고 검증
"""
HVDC 분석 결과의 재고 계산을 검증
사용자 제공 코드와 StockEngine 결과 비교
"""

import pandas as pd
import numpy as np
from inventory_validation import InventoryValidator

def test_with_hvdc_data():
    """HVDC 분석 실행 후 재고 검증"""
    
    print("🔍 HVDC 분석 시스템 재고 검증 시작")
    print("=" * 50)
    
    # 1. HVDC 분석 실행
    print("📊 HVDC 분석 실행 중...")
    
    try:
        # HVDC analysis.py 실행하여 결과 데이터 생성
        import subprocess
        result = subprocess.run(['python', 'HVDC analysis.py'], 
                              capture_output=True, text=True, timeout=120)
        
        if result.returncode == 0:
            print("✅ HVDC 분석 완료")
        else:
            print(f"⚠️ HVDC 분석 경고: {result.stderr}")
            
    except Exception as e:
        print(f"❌ HVDC 분석 실행 실패: {e}")
        return
    
    # 2. 생성된 리포트에서 재고 데이터 추출
    print("\n📋 생성된 리포트에서 재고 데이터 추출 중...")
    
    # 최신 리포트 파일 찾기
    import glob
    report_files = glob.glob("HVDC_Comprehensive_Report_*.xlsx")
    if not report_files:
        print("❌ 리포트 파일을 찾을 수 없습니다")
        return
    
    latest_report = max(report_files)
    print(f"📄 최신 리포트: {latest_report}")
    
    # 3. 창고별 월별 데이터 검증
    try:
        warehouse_df = pd.read_excel(latest_report, sheet_name='🏢 Warehouse Monthly')
        print(f"📊 창고별 월별 데이터: {len(warehouse_df)}행")
        print(f"컬럼: {list(warehouse_df.columns)}")
        
        # 재고 검증
        validator = InventoryValidator()
        validation_results = validator.validate_hvdc_stock_engine(warehouse_df)
        
        print(f"\n✅ 창고별 월별 데이터 검증 결과:")
        print(f"   상태: {validation_results.get('status', 'unknown')}")
        
        if validation_results.get('status') == 'completed':
            columns_used = validation_results.get('columns_used', {})
            print(f"   사용된 컬럼:")
            print(f"     입고: {columns_used.get('incoming', 'None')}")
            print(f"     출고: {columns_used.get('outgoing', 'None')}")
            print(f"     재고: {columns_used.get('inventory', 'None')}")
            
            print(f"   검증 결과:")
            print(f"     총 레코드: {validation_results.get('total_records', 0)}")
            print(f"     반복문 vs 벡터화: {'✅' if validation_results.get('loop_vs_vectorized_match') else '❌'}")
            print(f"     Assert 통과: {'✅' if validation_results.get('assert_passed') else '❌'}")
            
            mismatches = validation_results.get('mismatches', [])
            if mismatches:
                print(f"   ⚠️ 불일치 사항:")
                for mismatch in mismatches:
                    print(f"     - {mismatch}")
            else:
                print("   ✅ 모든 검증 통과!")
        
    except Exception as e:
        print(f"❌ 창고별 데이터 검증 실패: {e}")
    
    # 4. 일별 재고 데이터 검증
    try:
        daily_df = pd.read_excel(latest_report, sheet_name='📅 일별재고추적')
        print(f"\n📅 일별 재고 데이터: {len(daily_df)}행")
        print(f"컬럼: {list(daily_df.columns)}")
        
        # 사용자 제공 로직으로 재고 재계산
        if 'Inbound' in daily_df.columns and 'Outbound' in daily_df.columns:
            print(f"\n🔄 사용자 제공 로직으로 재고 재계산:")
            
            # 위치별로 분리하여 검증
            locations = daily_df['Loc'].unique() if 'Loc' in daily_df.columns else ['전체']
            
            for location in locations[:3]:  # 처음 3개 위치만 테스트
                if pd.isna(location):
                    continue
                    
                print(f"\n📍 {location} 창고:")
                
                if 'Loc' in daily_df.columns:
                    loc_data = daily_df[daily_df['Loc'] == location].copy()
                else:
                    loc_data = daily_df.copy()
                
                if len(loc_data) == 0:
                    continue
                
                loc_data = loc_data.sort_values('Date' if 'Date' in loc_data.columns else loc_data.columns[0])
                
                # 사용자 로직 적용
                initial_stock = 0
                inv = initial_stock
                inventory_list = []
                
                for _, row in loc_data.iterrows():
                    in_qty = row.get('Inbound', 0)
                    out_qty = row.get('Outbound', 0)
                    inv = inv + in_qty - out_qty   # 이전 inv에 입고-출고 반영
                    inventory_list.append(inv)
                
                loc_data['Inventory_loop'] = inventory_list
                
                # 기존 Closing과 비교
                if 'Closing' in loc_data.columns:
                    try:
                        # Inventory_loop 컬럼이 Closing 컬럼과 동일한지 확인
                        match = (loc_data['Inventory_loop'] == loc_data['Closing']).all()
                        print(f"   Inventory_loop == Closing: {'✅' if match else '❌'}")
                        
                        if not match:
                            diff_count = (loc_data['Inventory_loop'] != loc_data['Closing']).sum()
                            print(f"   불일치 건수: {diff_count}/{len(loc_data)}")
                            
                    except Exception as e:
                        print(f"   비교 오류: {e}")
                
                print(f"   최종 재고: {inventory_list[-1] if inventory_list else 0}")
                
    except Exception as e:
        print(f"❌ 일별 데이터 검증 실패: {e}")
    
    print(f"\n🎯 HVDC 재고 검증 완료!")

if __name__ == "__main__":
    test_with_hvdc_data() 