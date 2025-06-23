# hvdc_korean_excel_report.py - HVDC 한국어 엑셀 리포트 생성기
"""
HVDC 창고 분석 결과를 한국어 컬럼명으로 명확한 엑셀 리포트 생성
- 월별_입고량, 월별_출고량, 월말_재고량 등 직관적인 용어 사용
- 창고별/현장별 상세 분석
- 완전한 검증 및 요약
"""

import pandas as pd
import numpy as np
from datetime import datetime
import glob
import os
import warnings
warnings.filterwarnings('ignore')

# hvdc_timeline_tracking 함수들 직접 로드
with open('hvdc_timeline_tracking.py', 'r', encoding='utf-8') as f:
    exec(f.read())

def create_korean_excel_report():
    """한국어 컬럼명 HVDC 엑셀 리포트 생성"""
    print("📊 HVDC 한국어 엑셀 리포트 생성 시작")
    print("=" * 50)
    
    # 1. 데이터 로드 및 처리 (기존 로직 사용)
    print("🔍 데이터 로드 및 처리 중...")
    
    # HVDC 파일 찾기
    hvdc_files = []
    data_dir = "data"
    if os.path.exists(data_dir):
        pattern = os.path.join(data_dir, "HVDC WAREHOUSE_*.xlsx")
        hvdc_files = [f for f in glob.glob(pattern) if 'invoice' not in f.lower()]
    
    if not hvdc_files:
        hvdc_files = [f for f in glob.glob("HVDC WAREHOUSE_*.xlsx") if 'invoice' not in f.lower()]
    
    if not hvdc_files:
        print("❌ HVDC 파일을 찾을 수 없습니다!")
        return
    
    # 케이스별 타임라인 추출
    all_case_timelines = []
    for filepath in hvdc_files:
        case_timelines = TimelineExtractor.extract_case_timeline(filepath)
        all_case_timelines.extend(case_timelines)
    
    # 케이스별 상태 결정
    case_statuses = []
    for case_timeline in all_case_timelines:
        case_status = StatusAnalyzer.determine_case_status(case_timeline)
        case_statuses.append(case_status)
    
    # 월별 분석
    warehouse_monthly = StockCalculator.calculate_monthly_warehouse_stock(case_statuses)
    site_monthly = StockCalculator.calculate_monthly_site_delivery(case_statuses)
    validation_result = Validator.validate_warehouse_calculations(warehouse_monthly)
    
    print(f"✅ 데이터 처리 완료: {len(case_statuses)}개 케이스")
    
    # 2. 한국어 컬럼명으로 변경
    print("🔤 한국어 컬럼명으로 변경 중...")
    
    # 창고별 월별 데이터 한국어 변경
    if not warehouse_monthly.empty:
        warehouse_korean = warehouse_monthly.rename(columns={
            'Warehouse': '창고명',
            'YearMonth': '년월',
            'Monthly_Inbound': '월별_입고량',
            'Monthly_Outbound': '월별_출고량', 
            'Cumulative_Stock': '월말_재고량',
            'Inbound_Count': '입고_건수',
            'Outbound_Count': '출고_건수'
        })
    else:
        warehouse_korean = pd.DataFrame()
    
    # 현장별 월별 데이터 한국어 변경
    if not site_monthly.empty:
        site_korean = site_monthly.rename(columns={
            'Site': '현장명',
            'YearMonth': '년월',
            'Monthly_Delivered': '월별_배송량',
            'Cumulative_Delivered': '누적_배송량',
            'Delivered_Count': '배송_건수'
        })
    else:
        site_korean = pd.DataFrame()
    
    # 케이스별 상세 데이터 준비
    case_details_korean = []
    for case in case_statuses:
        case_details_korean.append({
            '케이스번호': case['Case_No'],
            '수량': case['Qty'],
            '최종상태': case['Final_Status'],
            '현재위치': case['Current_Location'],
            '마지막창고': case['Last_Warehouse'],
            '최종현장': case['Final_Site'],
            '이동경로': ' → '.join(case['Timeline'][:3]) if case['Timeline'] else '이동없음',
            '총이벤트수': len(case.get('Events', [])),
            '원본파일': case['Source_File']
        })
    
    case_details_df = pd.DataFrame(case_details_korean)
    
    # 3. 엑셀 리포트 생성
    print("📄 한국어 엑셀 리포트 생성 중...")
    
    output_file = f"HVDC_한국어_리포트_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
        workbook = writer.book
        
        # 서식 정의
        title_format = workbook.add_format({
            'bold': True,
            'font_size': 18,
            'bg_color': '#1F4E79',
            'font_color': 'white',
            'align': 'center',
            'valign': 'vcenter',
            'border': 2
        })
        
        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'bg_color': '#D7E4BC',
            'border': 1,
            'align': 'center',
            'font_size': 11
        })
        
        number_format = workbook.add_format({
            'num_format': '#,##0',
            'align': 'right'
        })
        
        total_format = workbook.add_format({
            'bold': True,
            'bg_color': '#FFE699',
            'num_format': '#,##0',
            'border': 1,
            'align': 'right'
        })
        
        # 시트 1: 📊 종합 요약
        summary_data = [
            ['분석항목', '값', '단위'],
            ['분석일시', datetime.now().strftime('%Y년 %m월 %d일 %H시 %M분'), ''],
            ['총 케이스 수', len(case_statuses), '개'],
            ['총 수량', sum(case['Qty'] for case in case_statuses), '박스'],
            ['분석 파일 수', len(hvdc_files), '개'],
            ['운영 창고 수', len(warehouse_korean['창고명'].unique()) if not warehouse_korean.empty else 0, '개'],
            ['배송 현장 수', len(site_korean['현장명'].unique()) if not site_korean.empty else 0, '개'],
            ['검증 결과', '✅ 통과' if validation_result['validation_passed'] else '❌ 실패', ''],
            ['오류 건수', validation_result['total_errors'], '개']
        ]
        
        summary_df = pd.DataFrame(summary_data[1:], columns=summary_data[0])
        summary_df.to_excel(writer, sheet_name='📊_종합요약', index=False, startrow=2)
        
        worksheet = writer.sheets['📊_종합요약']
        worksheet.merge_range('A1:C2', 'HVDC 창고 분석 종합 요약', title_format)
        
        # 헤더 서식
        for col_num, value in enumerate(summary_df.columns):
            worksheet.write(2, col_num, value, header_format)
        
        # 컬럼 너비 조정
        worksheet.set_column('A:A', 25)
        worksheet.set_column('B:B', 30)
        worksheet.set_column('C:C', 12)
        
        # 시트 2: 🏢 창고별 월별 입출고 재고
        if not warehouse_korean.empty:
            warehouse_korean.to_excel(writer, sheet_name='🏢_창고별_월별_입출고재고', index=False, startrow=1)
            worksheet = writer.sheets['🏢_창고별_월별_입출고재고']
            
            # 제목
            worksheet.merge_range('A1:G1', '창고별 월별 입고·출고·재고 현황', title_format)
            
            # 헤더 서식
            for col_num, value in enumerate(warehouse_korean.columns):
                worksheet.write(1, col_num, value, header_format)
            
            # TOTAL 행 강조 및 숫자 서식
            for row_num in range(2, len(warehouse_korean) + 2):
                for col_num, col_name in enumerate(warehouse_korean.columns):
                    cell_value = warehouse_korean.iloc[row_num-2, col_num]
                    
                    if warehouse_korean.iloc[row_num-2]['년월'] == 'TOTAL':
                        # TOTAL 행
                        if col_name in ['월별_입고량', '월별_출고량', '월말_재고량', '입고_건수', '출고_건수']:
                            worksheet.write(row_num, col_num, cell_value, total_format)
                        else:
                            worksheet.write(row_num, col_num, cell_value, workbook.add_format({'bold': True, 'bg_color': '#FFE699', 'border': 1}))
                    else:
                        # 일반 행
                        if col_name in ['월별_입고량', '월별_출고량', '월말_재고량', '입고_건수', '출고_건수']:
                            worksheet.write(row_num, col_num, cell_value, number_format)
            
            # 컬럼 너비 조정
            worksheet.set_column('A:A', 18)  # 창고명
            worksheet.set_column('B:B', 12)  # 년월
            worksheet.set_column('C:G', 15)  # 숫자 컬럼들
        
        # 시트 3: 🏗️ 현장별 월별 배송
        if not site_korean.empty:
            site_korean.to_excel(writer, sheet_name='🏗️_현장별_월별_배송', index=False, startrow=1)
            worksheet = writer.sheets['🏗️_현장별_월별_배송']
            
            # 제목
            worksheet.merge_range('A1:E1', '현장별 월별 배송 현황', title_format)
            
            # 헤더 서식
            for col_num, value in enumerate(site_korean.columns):
                worksheet.write(1, col_num, value, header_format)
            
            # TOTAL 행 강조 및 숫자 서식
            for row_num in range(2, len(site_korean) + 2):
                for col_num, col_name in enumerate(site_korean.columns):
                    cell_value = site_korean.iloc[row_num-2, col_num]
                    
                    if site_korean.iloc[row_num-2]['년월'] == 'TOTAL':
                        # TOTAL 행
                        if col_name in ['월별_배송량', '누적_배송량', '배송_건수']:
                            worksheet.write(row_num, col_num, cell_value, total_format)
                        else:
                            worksheet.write(row_num, col_num, cell_value, workbook.add_format({'bold': True, 'bg_color': '#FFE699', 'border': 1}))
                    else:
                        # 일반 행
                        if col_name in ['월별_배송량', '누적_배송량', '배송_건수']:
                            worksheet.write(row_num, col_num, cell_value, number_format)
            
            # 컬럼 너비 조정
            worksheet.set_column('A:A', 15)  # 현장명
            worksheet.set_column('B:B', 12)  # 년월
            worksheet.set_column('C:E', 15)  # 숫자 컬럼들
        
        # 시트 4: 📦 케이스별 상세 추적
        if not case_details_df.empty:
            case_details_df.to_excel(writer, sheet_name='📦_케이스별_상세추적', index=False, startrow=1)
            worksheet = writer.sheets['📦_케이스별_상세추적']
            
            # 제목
            worksheet.merge_range('A1:I1', '케이스별 상세 추적 정보', title_format)
            
            # 헤더 서식
            for col_num, value in enumerate(case_details_df.columns):
                worksheet.write(1, col_num, value, header_format)
            
            # 컬럼 너비 조정
            worksheet.set_column('A:A', 20)  # 케이스번호
            worksheet.set_column('B:B', 10)  # 수량
            worksheet.set_column('C:C', 18)  # 최종상태
            worksheet.set_column('D:F', 16)  # 위치 컬럼들
            worksheet.set_column('G:G', 40)  # 이동경로
            worksheet.set_column('H:H', 12)  # 총이벤트수
            worksheet.set_column('I:I', 25)  # 원본파일
        
        # 시트 5: ✅ 검증 결과
        validation_data = [
            ['검증항목', '결과', '상세내용'],
            ['전체 검증 상태', '✅ 통과' if validation_result['validation_passed'] else '❌ 실패', ''],
            ['총 오류 건수', validation_result['total_errors'], '개'],
            ['검증 공식', '입고량 - 출고량 = 재고량', '모든 창고별 마지막 월 기준'],
            ['검증 일시', datetime.now().strftime('%Y년 %m월 %d일 %H시 %M분'), '']
        ]
        
        validation_df = pd.DataFrame(validation_data[1:], columns=validation_data[0])
        validation_df.to_excel(writer, sheet_name='✅_검증결과', index=False, startrow=1)
        
        worksheet = writer.sheets['✅_검증결과']
        worksheet.merge_range('A1:C1', '계산 검증 결과', title_format)
        
        # 헤더 서식
        for col_num, value in enumerate(validation_df.columns):
            worksheet.write(1, col_num, value, header_format)
        
        # 컬럼 너비 조정
        worksheet.set_column('A:A', 25)
        worksheet.set_column('B:B', 20)
        worksheet.set_column('C:C', 30)
    
    print(f"✅ 한국어 엑셀 리포트 생성 완료: {output_file}")
    
    # 생성된 시트 요약
    print(f"\n📋 생성된 시트 목록:")
    print(f"   📊 종합요약 - 전체 분석 요약")
    print(f"   🏢 창고별_월별_입출고재고 - 월별_입고량, 월별_출고량, 월말_재고량")
    print(f"   🏗️ 현장별_월별_배송 - 월별_배송량, 누적_배송량")
    print(f"   📦 케이스별_상세추적 - 케이스별 이동 경로")
    print(f"   ✅ 검증결과 - 입고-출고=재고 검증")
    
    return output_file

if __name__ == "__main__":
    output_file = create_korean_excel_report()
    print(f"\n🎉 HVDC 한국어 엑셀 리포트 생성 완료!")
    print(f"📁 파일: {output_file}")
    print(f"\n📊 컬럼명 설명:")
    print(f"   월별_입고량: 해당 월에 창고로 들어온 수량")
    print(f"   월별_출고량: 해당 월에 창고에서 현장으로 나간 수량") 
    print(f"   월말_재고량: 해당 월 말 기준 창고에 남아있는 수량") 