# -*- coding: utf-8 -*-
"""
Compustat 매칭 - 단계4B: 검토 결과 적용 (최적화 버전)
================================================
기능: 수동 검토된 검증 파일을 읽고 Compustat ID를 final_outcome에 병합

개선점:
1. 상세한 데이터 검증
2. ID의 선행 0 보존 (dtype=str 사용)
3. 상세한 로그
"""

import pandas as pd
import logging
from datetime import datetime

# ==========================================
# 로그 설정
# ==========================================
# logs 폴더가 존재하는지 확인
import os
if not os.path.exists('logs'):
    os.makedirs('logs')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/compustat_merge_4B_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==========================================
# 1. 경로 설정
# ==========================================
PATH_MAIN = "/Users/lidachuan/Desktop/Patent Data/final_outcome_1993_1997_COMPLETE.xlsx"
PATH_COMPUSTAT = "/Users/lidachuan/Desktop/Patent Data/compustat_19802025.csv"
PATH_VERIFIED = "/Users/lidachuan/Desktop/Patent Data/company_match_verification.xlsx"
PATH_OUTPUT = "/Users/lidachuan/Desktop/Patent Data/final_outcome.xlsx"

# ==========================================
# 2. 메인 처리 함수
# ==========================================

def main():
    start_time = datetime.now()
    
    logger.info("=" * 60)
    logger.info("Compustat 매칭 - 단계4B: 검토 결과 적용 (최적화 버전)")
    logger.info("=" * 60)
    
    # ========== 단계1: 데이터 읽기 ==========
    logger.info("\n단계 1/4: 데이터 읽기...")
    
    # 주 테이블 읽기
    logger.info("   주 테이블 로드 중...")
    try:
        df_main = pd.read_excel(PATH_MAIN)
        logger.info(f"   ✅ 주 테이블 로드 완료: {len(df_main):,} 행")
    except Exception as e:
        logger.error(f"   ❌ 읽기 실패: {e}")
        return False
    
    # 수동 검증 테이블 읽기 (검토 완료)
    logger.info("   수동 검증 테이블 로드 중...")
    try:
        df_verify = pd.read_excel(
            PATH_VERIFIED, 
            usecols=['Acquiror_Original', 'Matched_Compustat_Original']
        )
        # 중복 제거
        df_verify = df_verify.drop_duplicates(subset=['Acquiror_Original'])
        logger.info(f"   ✅ 검증 테이블 로드 완료: {len(df_verify):,} 개 유효 매칭 쌍")
    except Exception as e:
        logger.error(f"   ❌ 읽기 실패: {e}")
        logger.error("   단계4A를 완료하고 검증 파일을 수동 검토했는지 확인하세요")
        return False
    
    # Compustat 데이터 읽기 (선행 0 보존)
    logger.info("   Compustat 데이터 로드 중...")
    try:
        cols_to_load = ['conm', 'gvkey', 'cusip', 'cik']
        df_comp = pd.read_csv(
            PATH_COMPUSTAT, 
            usecols=cols_to_load, 
            dtype=str,  # 선행 0 보존
            low_memory=False
        )
        logger.info(f"   ✅ Compustat 데이터 로드 완료: {len(df_comp):,} 행")
    except ValueError:
        # 컬럼명이 일치하지 않으면 전체 읽기 시도
        logger.warning("   컬럼명이 일치하지 않을 수 있음, 전체 읽기 시도...")
        df_comp = pd.read_csv(PATH_COMPUSTAT, dtype=str, low_memory=False)
        logger.info(f"   ✅ Compustat 데이터 로드 완료 (전체): {len(df_comp):,} 행")
    except Exception as e:
        logger.error(f"   ❌ 읽기 실패: {e}")
        return False
    
    # ========== 단계2: Compustat 데이터 처리 ==========
    logger.info("\n단계 2/4: Compustat 사전 구축 중...")
    
    # conm이 비어있는 행 제거
    df_comp_clean = df_comp[df_comp['conm'].notna()].copy()
    
    # conm으로 중복 제거 (첫 번째 레코드 유지)
    df_comp_unique = df_comp_clean.drop_duplicates(subset=['conm'])
    
    logger.info(f"   ✅ Compustat 고유 회사: {len(df_comp_unique):,}")
    
    # ========== 단계3: 데이터 병합 ==========
    logger.info("\n단계 3/4: 데이터 병합 중...")
    
    # 3.1 검증 테이블과 Compustat ID 병합
    logger.info("   단계 3.1: Compustat ID 가져오기...")
    df_verify_with_ids = pd.merge(
        df_verify,
        df_comp_unique[['conm', 'gvkey', 'cusip', 'cik']],
        left_on='Matched_Compustat_Original',
        right_on='conm',
        how='left'
    )
    
    # 매칭 성공률 통계
    id_matched = df_verify_with_ids['gvkey'].notna().sum()
    logger.info(f"   ✅ ID 매칭 성공: {id_matched} / {len(df_verify)} ({id_matched/len(df_verify)*100:.1f}%)")
    
    # 3.2 주 테이블과 병합 (기존 gvkey/cusip/cik 컬럼 채우기)
    logger.info("   단계 3.2: 주 테이블의 ID 컬럼 채우기...")
    
    # 매핑 사전 생성
    acquiror_to_ids = {}
    for _, row in df_verify_with_ids.iterrows():
        acquiror_name = row['Acquiror_Original']
        acquiror_to_ids[acquiror_name] = {
            'gvkey': row.get('gvkey', None),
            'cusip': row.get('cusip', None),
            'cik': row.get('cik', None),
            'compustat_name': row.get('Matched_Compustat_Original', None)
        }
    
    # 기존 컬럼 채우기 (기존 값 유지, 빈 값만 채우기)
    df_final = df_main.copy()
    
    # 컬럼이 존재하는지 확인
    for col in ['gvkey', 'cusip', 'cik', 'compustat_name']:
        if col not in df_final.columns:
            df_final[col] = None
    
    # 행별로 채우기
    for idx, row in df_final.iterrows():
        acquiror_name = row['acquiror_name']
        if acquiror_name in acquiror_to_ids:
            ids = acquiror_to_ids[acquiror_name]
            # 빈 값만 채우기
            if pd.isna(df_final.at[idx, 'gvkey']):
                df_final.at[idx, 'gvkey'] = ids['gvkey']
            if pd.isna(df_final.at[idx, 'cusip']):
                df_final.at[idx, 'cusip'] = ids['cusip']
            if pd.isna(df_final.at[idx, 'cik']):
                df_final.at[idx, 'cik'] = ids['cik']
            if pd.isna(df_final.at[idx, 'compustat_name']):
                df_final.at[idx, 'compustat_name'] = ids['compustat_name']
    
    logger.info(f"   ✅ 채우기 완료, 최종 행 수: {len(df_final):,}")
    
    # ========== 단계4: 결과 저장 ==========
    logger.info("\n단계 4/4: 결과 저장 중...")
    
    try:
        df_final.to_excel(PATH_OUTPUT, index=False)
        logger.info(f"   ✅ 파일 저장 완료: {PATH_OUTPUT}")
    except Exception as e:
        logger.error(f"   ❌ 저장 실패: {e}")
        return False
    
    # ========== 완료 요약 ==========
    duration = (datetime.now() - start_time).total_seconds()
    
    # 통계
    total_rows = len(df_final)
    matched_count = df_final['compustat_name'].notna().sum()
    match_rate = matched_count / total_rows * 100
    
    has_gvkey = df_final['gvkey'].notna().sum()
    has_cusip = df_final['cusip'].notna().sum()
    has_cik = df_final['cik'].notna().sum()
    
    logger.info("\n" + "=" * 60)
    logger.info("단계4B 완료!")
    logger.info("=" * 60)
    logger.info(f"⏱  총 소요시간: {duration:.2f} 초")
    logger.info(f"\n📊 결과 통계:")
    logger.info(f"   - 총 행 수: {total_rows:,}")
    logger.info(f"   - Compustat 매칭: {matched_count:,} ({match_rate:.1f}%)")
    logger.info(f"   - gvkey 있음: {has_gvkey:,}")
    logger.info(f"   - cusip 있음: {has_cusip:,}")
    logger.info(f"   - cik 있음: {has_cik:,}")
    logger.info(f"\n📁 출력 파일:")
    logger.info(f"   {PATH_OUTPUT}")
    logger.info(f"\n✅ 전체 데이터 처리 프로세스 완료! 🎉")
    logger.info("=" * 60)
    
    return True


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
