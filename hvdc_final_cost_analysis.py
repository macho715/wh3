#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HVDC 최종 비용 분석 시스템
=========================

hvdc_ontology_pipeline.py를 활용하여 월별, 창고별, 사이트별 운영 비용을 계산하고
종합 리포트를 생성합니다.

작성자: HVDC Analysis Team  
최종 수정: 2025-06-22
기반: hvdc_ontology_pipeline.py + 비용 분석
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# hvdc_ontology_pipeline.py에서 기본 클래스들 가져오기
from hvdc_ontology_pipeline import (
    OntologyMapper, 
    EnhancedDataLoader, 
    EnhancedTransactionEngine, 
    EnhancedAnalysisEngine
)

def main():
    """HVDC 최종 비용 분석 메인 함수"""
    print("🚀 HVDC 최종 비용 분석 시스템 시작")
    print("=" * 60)
    
    try:
        # 1. 온톨로지 매퍼 초기화
        print("🔧 온톨로지 시스템 초기화 중...")
        mapper = OntologyMapper("mapping_rules_v2.4.json")
        print(f"✅ 온톨로지 매핑 룰 로드 완료")
        
        # 2. 인보이스 비용 데이터 로드
        print("\n💰 인보이스 비용 데이터 로드 중...")
        invoice_df = pd.read_excel('data/HVDC WAREHOUSE_INVOICE.xlsx')
        print(f"✅ 인보이스 데이터 로드: {len(invoice_df)}건")
        
        # 인보이스 데이터 전처리
        invoice_df['packages_qty'] = pd.to_numeric(invoice_df["pkgs q'ty"], errors='coerce').fillna(0)
        invoice_df['total_cost'] = pd.to_numeric(invoice_df['TOTAL'], errors='coerce').fillna(0)
        invoice_df['operation_month'] = pd.to_datetime(invoice_df['Operation Month'], errors='coerce')
        invoice_df['year_month'] = invoice_df['operation_month'].dt.strftime('%Y-%m')
        
        # 카테고리별 창고 매핑
        category_warehouse_map = {
            'Indoor(M44)': 'DSV Indoor',
            'Outdoor': 'DSV Outdoor', 
            'Al Markaz': 'DSV Al Markaz'
        }
        invoice_df['warehouse'] = invoice_df['Category'].map(category_warehouse_map).fillna('기타')
        
        # 평균 비용 계산
        total_packages = invoice_df['packages_qty'].sum()
        total_cost = invoice_df['total_cost'].sum()
        avg_cost_per_package = total_cost / total_packages if total_packages > 0 else 0
        
        print(f"💰 총 인보이스 비용: ${total_cost:,.2f}")
        print(f"📦 총 패키지 수량: {total_packages:,.0f}개")
        print(f"💸 평균 패키지당 비용: ${avg_cost_per_package:.2f}")
        
        # 3. 창고 데이터 로드 및 분석 (온톨로지 파이프라인 활용)
        print("\n📂 창고 데이터 로드 및 분석 중...")
        
        data_loader = EnhancedDataLoader(mapper)
        raw_events = data_loader.load_and_process_files("data")
        
        if raw_events.empty:
            print("❌ 창고 데이터가 없습니다.")
            return False
        
        # 4. 트랜잭션 분석 (온톨로지 엔진 활용)
        print("\n🔄 트랜잭션 분석 중...")
        tx_engine = EnhancedTransactionEngine(mapper)
        transaction_log = tx_engine.create_transaction_log(raw_events)
        
        print(f"✅ 총 트랜잭션: {len(transaction_log):,}건")
        
        # 5. 재고 분석 (온톨로지 엔진 활용)
        print("\n📊 재고 분석 중...")
        analysis_engine = EnhancedAnalysisEngine(mapper)
        daily_stock = analysis_engine.calculate_daily_stock(transaction_log)
        monthly_summary = analysis_engine.create_monthly_summary(transaction_log, daily_stock)
        
        print(f"✅ 일별 재고 포인트: {len(daily_stock):,}개")
        print(f"✅ 월별 요약: {len(monthly_summary):,}개")
        
        # 6. 비용 분석 수행
        print("\n💰 비용 분석 수행 중...")
        
        # 6-1. 창고별 월별 운영 비용 계산
        print("🏢 창고별 월별 운영 비용 계산 중...")
        
        transaction_log['year_month'] = pd.to_datetime(transaction_log['Date']).dt.strftime('%Y-%m')
        
        # 월별 창고별 트랜잭션 집계
        monthly_warehouse_ops = transaction_log.groupby(['Location', 'year_month', 'TxType_Refined']).agg({
            'Qty': 'sum',
            'Case_No': 'nunique'
        }).reset_index()
        
        # 창고별 운영 비용 계산
        warehouse_cost_results = []
        
        for _, row in monthly_warehouse_ops.iterrows():
            warehouse = row['Location']
            year_month = row['year_month'] 
            tx_type = row['TxType_Refined']
            qty = row['Qty']
            
            # 트랜잭션 타입별 비용 비율 적용
            if tx_type == 'IN':
                cost_rate = avg_cost_per_package * 0.3  # 입고 처리비 30%
                cost_type = '입고처리비'
            elif tx_type in ['TRANSFER_OUT', 'FINAL_OUT']:
                cost_rate = avg_cost_per_package * 0.2  # 출고 처리비 20%
                cost_type = '출고처리비'
            else:
                cost_rate = avg_cost_per_package * 0.1  # 기타 10%
                cost_type = '기타운영비'
            
            total_cost = qty * cost_rate
            
            warehouse_cost_results.append({
                'Warehouse': warehouse,
                'YearMonth': year_month,
                'TxType': tx_type,
                'CostType': cost_type,
                'Qty': qty,
                'CostPerUnit': cost_rate,
                'TotalCost': total_cost
            })
        
        warehouse_costs_df = pd.DataFrame(warehouse_cost_results)
        print(f"   ✅ 창고별 월별 비용 계산 완료: {len(warehouse_costs_df)}건")
        
        # 6-2. 사이트별 월별 배송 비용 계산
        print("🏗️ 사이트별 월별 배송 비용 계산 중...")
        
        # 사이트 배송 데이터 필터링
        site_deliveries = transaction_log[
            (transaction_log['TxType_Refined'] == 'FINAL_OUT') & 
            (transaction_log['Site'] != 'UNK')
        ].copy()
        
        if not site_deliveries.empty:
            site_deliveries['year_month'] = pd.to_datetime(site_deliveries['Date']).dt.strftime('%Y-%m')
            
            # 월별 사이트별 배송량 집계
            monthly_site_deliveries = site_deliveries.groupby(['Site', 'year_month']).agg({
                'Qty': 'sum',
                'Case_No': 'nunique'
            }).reset_index()
            
            # 사이트별 배송 비용 계산
            site_cost_results = []
            
            for _, row in monthly_site_deliveries.iterrows():
                site = row['Site']
                year_month = row['year_month']
                qty = row['Qty']
                
                # 배송 비용 = 운송비(30%) + 현장 하역비(15%)
                transportation_rate = avg_cost_per_package * 0.3
                site_handling_rate = avg_cost_per_package * 0.15
                total_delivery_cost = qty * (transportation_rate + site_handling_rate)
                
                site_cost_results.append({
                    'Site': site,
                    'YearMonth': year_month,
                    'DeliveredQty': qty,
                    'TransportationCost': qty * transportation_rate,
                    'SiteHandlingCost': qty * site_handling_rate,
                    'TotalDeliveryCost': total_delivery_cost
                })
            
            site_costs_df = pd.DataFrame(site_cost_results)
            print(f"   ✅ 사이트별 월별 비용 계산 완료: {len(site_costs_df)}건")
        else:
            site_costs_df = pd.DataFrame()
            print("   ⚠️ 사이트 배송 데이터 없음")
        
        # 7. 비용 요약 및 분석
        print("\n📊 비용 요약 및 분석 중...")
        
        # 창고별 총 비용
        warehouse_total_costs = warehouse_costs_df.groupby('Warehouse')['TotalCost'].sum().reset_index()
        warehouse_total_costs = warehouse_total_costs.sort_values('TotalCost', ascending=False)
        
        # 사이트별 총 비용
        if not site_costs_df.empty:
            site_total_costs = site_costs_df.groupby('Site')['TotalDeliveryCost'].sum().reset_index()
            site_total_costs = site_total_costs.sort_values('TotalDeliveryCost', ascending=False)
        else:
            site_total_costs = pd.DataFrame()
        
        # 전체 비용 계산
        total_warehouse_cost = warehouse_costs_df['TotalCost'].sum()
        total_site_cost = site_costs_df['TotalDeliveryCost'].sum() if not site_costs_df.empty else 0
        grand_total_cost = total_warehouse_cost + total_site_cost
        
        print(f"💰 비용 분석 결과:")
        print(f"   🏢 총 창고 운영비: ${total_warehouse_cost:,.2f}")
        print(f"   🏗️ 총 사이트 배송비: ${total_site_cost:,.2f}")
        print(f"   💸 총 운영비용: ${grand_total_cost:,.2f}")
        print(f"   📊 창고 비용 비율: {total_warehouse_cost/grand_total_cost*100:.1f}%")
        print(f"   📊 배송 비용 비율: {total_site_cost/grand_total_cost*100:.1f}%")
        
        # 8. 종합 리포트 생성
        print("\n📄 종합 리포트 생성 중...")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f'HVDC_최종_운영비용_분석리포트_{timestamp}.xlsx'
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            
            # 8-1. 종합요약 시트
            summary_data = []
            
            # 기본 정보
            summary_data.extend([
                ['📊 HVDC 운영 비용 분석 요약', ''],
                ['분석 일시', datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                ['', ''],
                ['📦 기본 현황', ''],
                ['총 트랜잭션', f"{len(transaction_log):,}건"],
                ['총 케이스', f"{transaction_log['Case_No'].nunique():,}개"],
                ['총 수량', f"{transaction_log['Qty'].sum():,}박스"],
                ['분석 기간', f"{transaction_log['Date'].min().strftime('%Y-%m-%d')} ~ {transaction_log['Date'].max().strftime('%Y-%m-%d')}"],
                ['', ''],
            ])
            
            # 비용 정보
            summary_data.extend([
                ['💰 비용 분석 결과', ''],
                ['총 운영비용', f"${grand_total_cost:,.2f}"],
                ['창고 운영비', f"${total_warehouse_cost:,.2f}"],
                ['사이트 배송비', f"${total_site_cost:,.2f}"],
                ['창고 비용 비율', f"{total_warehouse_cost/grand_total_cost*100:.1f}%"],
                ['배송 비용 비율', f"{total_site_cost/grand_total_cost*100:.1f}%"],
                ['평균 패키지당 비용', f"${avg_cost_per_package:.2f}"],
                ['', ''],
            ])
            
            # 창고별 비용
            summary_data.append(['🏢 창고별 총 운영비용', ''])
            for _, row in warehouse_total_costs.iterrows():
                summary_data.append([row['Warehouse'], f"${row['TotalCost']:,.2f}"])
            summary_data.append(['', ''])
            
            # 사이트별 비용
            if not site_total_costs.empty:
                summary_data.append(['🏗️ 사이트별 총 배송비용', ''])
                for _, row in site_total_costs.iterrows():
                    summary_data.append([row['Site'], f"${row['TotalDeliveryCost']:,.2f}"])
            
            summary_df = pd.DataFrame(summary_data, columns=['항목', '값'])
            summary_df.to_excel(writer, sheet_name='📊종합요약', index=False)
            
            # 8-2. 창고별 월별 운영비용 시트 (피벗 테이블)
            if not warehouse_costs_df.empty:
                warehouse_pivot = warehouse_costs_df.pivot_table(
                    index='Warehouse',
                    columns='YearMonth', 
                    values='TotalCost',
                    aggfunc='sum',
                    fill_value=0
                ).reset_index()
                
                # 총계 컬럼 추가
                warehouse_pivot['총계'] = warehouse_pivot.iloc[:, 1:].sum(axis=1)
                
                warehouse_pivot.to_excel(writer, sheet_name='🏢창고별_월별_운영비용', index=False)
            
            # 8-3. 사이트별 월별 배송비용 시트 (피벗 테이블)
            if not site_costs_df.empty:
                site_pivot = site_costs_df.pivot_table(
                    index='Site',
                    columns='YearMonth',
                    values='TotalDeliveryCost', 
                    aggfunc='sum',
                    fill_value=0
                ).reset_index()
                
                # 총계 컬럼 추가
                site_pivot['총계'] = site_pivot.iloc[:, 1:].sum(axis=1)
                
                site_pivot.to_excel(writer, sheet_name='🏗️사이트별_월별_배송비용', index=False)
            
            # 8-4. 창고별 상세 비용 내역
            if not warehouse_costs_df.empty:
                warehouse_costs_df.to_excel(writer, sheet_name='🏢창고별_상세비용', index=False)
            
            # 8-5. 사이트별 상세 비용 내역
            if not site_costs_df.empty:
                site_costs_df.to_excel(writer, sheet_name='🏗️사이트별_상세비용', index=False)
            
            # 8-6. 기존 온톨로지 분석 결과 추가
            if not daily_stock.empty:
                daily_stock.to_excel(writer, sheet_name='📊일별_재고_분석', index=False)
            
            if not monthly_summary.empty:
                monthly_summary.to_excel(writer, sheet_name='📅월별_요약_분석', index=False)
        
        print(f"✅ 종합 리포트 저장 완료: {output_file}")
        
        # 9. 최종 결과 출력
        print(f"\n🎉 HVDC 최종 비용 분석 완료!")
        print(f"📁 생성된 파일: {output_file}")
        
        print(f"\n📋 최종 분석 결과:")
        print(f"   📦 총 처리량: {transaction_log['Qty'].sum():,}박스")
        print(f"   💰 총 운영비용: ${grand_total_cost:,.2f}")
        print(f"   🏢 창고 운영비: ${total_warehouse_cost:,.2f} ({total_warehouse_cost/grand_total_cost*100:.1f}%)")
        print(f"   🏗️ 사이트 배송비: ${total_site_cost:,.2f} ({total_site_cost/grand_total_cost*100:.1f}%)")
        print(f"   💸 박스당 평균 비용: ${grand_total_cost/transaction_log['Qty'].sum():.2f}")
        
        print(f"\n🏆 주요 창고별 운영비용:")
        for _, row in warehouse_total_costs.head(5).iterrows():
            print(f"     {row['Warehouse']}: ${row['TotalCost']:,.2f}")
        
        if not site_total_costs.empty:
            print(f"\n🎯 주요 사이트별 배송비용:")
            for _, row in site_total_costs.head(5).iterrows():
                print(f"     {row['Site']}: ${row['TotalDeliveryCost']:,.2f}")
        
        return True
        
    except Exception as e:
        print(f"❌ 분석 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    if success:
        print(f"\n✅ HVDC 최종 비용 분석 시스템 실행 완료!")
    else:
        print(f"\n❌ HVDC 최종 비용 분석 시스템 실행 실패!") 