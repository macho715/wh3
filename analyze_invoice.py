import pandas as pd
import numpy as np

def analyze_invoice_file():
    """HVDC WAREHOUSE_INVOICE.xlsx 파일 분석"""
    
    try:
        # 인보이스 파일 로드
        invoice_df = pd.read_excel('data/HVDC WAREHOUSE_INVOICE.xlsx')
        
        print("=== HVDC WAREHOUSE_INVOICE.xlsx 분석 ===")
        print(f"📊 데이터 크기: {invoice_df.shape[0]}행 x {invoice_df.shape[1]}열")
        print(f"📋 컬럼 목록: {list(invoice_df.columns)}")
        print()
        
        # 컬럼별 데이터 타입
        print("=== 컬럼별 데이터 타입 ===")
        for col in invoice_df.columns:
            dtype = invoice_df[col].dtype
            non_null = invoice_df[col].notna().sum()
            print(f"  {col}: {dtype} (Non-null: {non_null})")
        print()
        
        # 첫 10행 출력
        print("=== 첫 10행 데이터 ===")
        print(invoice_df.head(10).to_string())
        print()
        
        # Case No. 관련 분석
        if 'Case No.' in invoice_df.columns:
            print("=== Case No. 분석 ===")
            case_count = invoice_df['Case No.'].nunique()
            print(f"  고유 Case No. 수: {case_count}")
            print(f"  Case No. 샘플: {invoice_df['Case No.'].dropna().head(5).tolist()}")
        
        # 날짜 관련 컬럼 분석
        date_columns = [col for col in invoice_df.columns if 'date' in col.lower() or 'Date' in col]
        if date_columns:
            print(f"=== 날짜 관련 컬럼 ({len(date_columns)}개) ===")
            for col in date_columns:
                print(f"  {col}: {invoice_df[col].dtype}")
                if invoice_df[col].notna().sum() > 0:
                    print(f"    샘플: {invoice_df[col].dropna().head(3).tolist()}")
        
        # 수량/금액 관련 컬럼 분석
        numeric_columns = invoice_df.select_dtypes(include=[np.number]).columns.tolist()
        if numeric_columns:
            print(f"=== 수치형 컬럼 ({len(numeric_columns)}개) ===")
            for col in numeric_columns:
                total = invoice_df[col].sum()
                count = invoice_df[col].notna().sum()
                print(f"  {col}: 총합={total}, 건수={count}")
        
        return invoice_df
        
    except Exception as e:
        print(f"❌ 에러 발생: {e}")
        return None

if __name__ == "__main__":
    df = analyze_invoice_file() 