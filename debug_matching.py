import pandas as pd
import re

def analyze_matching_patterns():
    """인보이스와 창고 데이터 매칭 패턴 분석"""
    
    # 1. 인보이스 데이터 분석
    print("=== 인보이스 Shipment No 패턴 분석 ===")
    invoice_df = pd.read_excel('data/HVDC WAREHOUSE_INVOICE.xlsx')
    
    shipment_nos = invoice_df['Shipment No'].dropna().unique()[:20]  # 처음 20개만
    print(f"인보이스 Shipment No 샘플 ({len(shipment_nos)}개):")
    for i, shipment in enumerate(shipment_nos, 1):
        print(f"  {i:2d}. {shipment}")
    
    # HE- 패턴 추출
    he_patterns = []
    for shipment in invoice_df['Shipment No'].dropna():
        match = re.search(r'HE-\d+', str(shipment))
        if match:
            he_patterns.append(match.group())
    
    print(f"\n추출된 HE- 패턴 ({len(set(he_patterns))}개 고유값):")
    unique_he = list(set(he_patterns))[:15]  # 처음 15개만
    for i, pattern in enumerate(unique_he, 1):
        print(f"  {i:2d}. {pattern}")
    
    # 2. 창고 데이터 분석
    print(f"\n=== 창고 Case No 패턴 분석 ===")
    
    warehouse_files = [
        ('HITACHI(HE)', 'data/HVDC WAREHOUSE_HITACHI(HE).xlsx'),
        ('HITACHI(HE-0214,0252)', 'data/HVDC WAREHOUSE_HITACHI(HE-0214,0252)1.xlsx'),
        ('HITACHI(HE_LOCAL)', 'data/HVDC WAREHOUSE_HITACHI(HE_LOCAL).xlsx'),
        ('SIMENSE(SIM)', 'data/HVDC WAREHOUSE_SIMENSE(SIM).xlsx')
    ]
    
    all_warehouse_cases = []
    
    for name, file_path in warehouse_files:
        try:
            df = pd.read_excel(file_path, sheet_name=0)
            if 'Case No.' in df.columns:
                cases = df['Case No.'].dropna().unique()
                all_warehouse_cases.extend(cases)
                
                print(f"\n{name} - Case No 샘플 (총 {len(cases)}개):")
                sample_cases = [str(case) for case in cases[:10]]  # 처음 10개만
                for i, case in enumerate(sample_cases, 1):
                    print(f"  {i:2d}. {case}")
                
                # HE- 패턴이 있는지 확인
                he_cases = [str(case) for case in cases if 'HE-' in str(case)]
                if he_cases:
                    print(f"     HE- 패턴 포함: {len(he_cases)}개")
                    for i, case in enumerate(he_cases[:5], 1):
                        print(f"       {i}. {case}")
                else:
                    print(f"     HE- 패턴 없음")
        except Exception as e:
            print(f"  ❌ {name} 로드 실패: {e}")
    
    # 3. 매칭 가능성 분석
    print(f"\n=== 매칭 가능성 분석 ===")
    
    # 인보이스 HE- 패턴
    invoice_he_patterns = set()
    for shipment in invoice_df['Shipment No'].dropna():
        matches = re.findall(r'HE-\d+', str(shipment))
        invoice_he_patterns.update(matches)
    
    # 창고 HE- 패턴
    warehouse_he_patterns = set()
    for case in all_warehouse_cases:
        if 'HE-' in str(case):
            matches = re.findall(r'HE-\d+', str(case))
            warehouse_he_patterns.update(matches)
    
    print(f"인보이스 HE- 패턴: {len(invoice_he_patterns)}개")
    print(f"창고 HE- 패턴: {len(warehouse_he_patterns)}개")
    
    # 공통 패턴
    common_patterns = invoice_he_patterns.intersection(warehouse_he_patterns)
    print(f"공통 HE- 패턴: {len(common_patterns)}개")
    
    if common_patterns:
        print("공통 패턴 샘플:")
        for i, pattern in enumerate(list(common_patterns)[:10], 1):
            print(f"  {i:2d}. {pattern}")
    
    return {
        'invoice_he_patterns': invoice_he_patterns,
        'warehouse_he_patterns': warehouse_he_patterns,
        'common_patterns': common_patterns
    }

if __name__ == "__main__":
    analyze_matching_patterns() 