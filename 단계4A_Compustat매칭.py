# -*- coding: utf-8 -*-
"""
Compustat 매칭 - 단계4A: 검증 파일 생성 (최적화 버전)
================================================
기능: final_outcome을 Compustat와 매칭하여 수동 검증 파일 생성

개선점:
1. thefuzz를 rapidfuzz로 대체 (더 빠름)
2. 상세한 로그 및 진행 상황 표시
3. 개선된 정리 함수 (단계1과 일치)
"""

import pandas as pd
import re
from rapidfuzz import process, fuzz
from tqdm import tqdm
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
        logging.FileHandler(f'logs/compustat_match_4A_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==========================================
# 1. 경로 및 설정
# ==========================================
PATH_MA = "/Users/lidachuan/Desktop/Patent Data/final_outcome_1993_1997_COMPLETE.xlsx"
PATH_COMPUSTAT = "/Users/lidachuan/Desktop/Patent Data/compustat_19802025.csv"
OUTPUT_VERIFICATION = "/Users/lidachuan/Desktop/Patent Data/company_match_verification.xlsx"

FUZZY_THRESHOLD = 90  # 퍼지 매칭 임계값

# ==========================================
# 2. 정리 함수 (단계1과 일치 유지)
# ==========================================

def clean_company_name(name):
    """최적화된 표준화 정리 함수"""
    if pd.isna(name) or not isinstance(name, str):
        return ""
    
    name = str(name).upper().strip()
    
    # 1. 일반적인 기호 처리
    name = name.replace('&', ' AND ')
    name = name.replace('-', ' ')
    name = name.replace("'", '')
    
    # 2. 일반적인 약어 확장
    abbreviations = {
        r'\bINTL\b': 'INTERNATIONAL',
        r'\bNATL\b': 'NATIONAL',
        r'\bCORP\b': 'CORPORATION',
        r'\bINC\b': 'INCORPORATED',
        r'\bMFG\b': 'MANUFACTURING',
        r'\bTECH\b': 'TECHNOLOGY',
        r'\bSYS\b': 'SYSTEMS',
    }
    for abbr, full in abbreviations.items():
        name = re.sub(abbr, full, name)
    
    # 3. 접미사 제거
    suffixes_priority = [
        r'\bINCORPORATED\b', r'\bCORPORATION\b', r'\bCOMPANY\b',
        r'\bLIMITED\b', r'\bGROUP\b',
        r'\bCORP\.?\b', r'\bINC\.?\b', r'\bLTD\.?\b', 
        r'\bCO\.?\b', r'\bL\.L\.C\.?\b', r'\bPLC\.?\b',
        r'\bLLC\b', r'\bS\.A\.\b', r'\bNV\b', r'\bGMBH\b',
        r'\bSA\b', r'\bAG\b', r'\bKK\b'
    ]
    
    for suffix in suffixes_priority:
        name = re.sub(suffix, '', name, flags=re.IGNORECASE)
    
    # 4. 구두점 제거
    name = re.sub(r'[^A-Z0-9\s]', ' ', name)
    
    # 5. 공백 병합
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name


# ==========================================
# 3. 메인 처리 함수
# ==========================================

def main():
    start_time = datetime.now()
    
    logger.info("=" * 60)
    logger.info("Compustat 매칭 - 단계4A: 검증 파일 생성 (최적화 버전)")
    logger.info("=" * 60)
    
    # ========== 단계1: 데이터 로드 ==========
    logger.info("\n단계 1/4: 데이터 로드 중...")
    
    # M&A 데이터 읽기
    logger.info("   M&A 데이터 로드 중...")
    try:
        df_ma = pd.read_excel(PATH_MA)
        logger.info(f"   ✅ M&A 데이터 로드 성공: {len(df_ma):,} 행")
    except Exception as e:
        logger.error(f"   ❌ 읽기 실패: {e}")
        return False
    
    # 필터링: patent_name이 있는 행만 처리
    df_ma_target = df_ma[df_ma['patent_name'].notna()].copy()
    logger.info(f"   patent_name 비어있지 않음으로 필터링: {len(df_ma_target):,} 행 매칭 대기")
    
    # Compustat 데이터 읽기 (메모리 절약을 위해 conm 컬럼만)
    logger.info("   Compustat 데이터 로드 중 (회사명 컬럼만)...")
    try:
        # 전략: 대용량 파일의 경우 필요한 컬럼만 읽기 (conm)
        df_comp = pd.read_csv(PATH_COMPUSTAT, usecols=['conm'], low_memory=False)
        logger.info(f"   ✅ Compustat 데이터 로드 성공: {len(df_comp):,} 행")
    except ValueError:
        # 컬럼명이 일치하지 않으면 전체 읽기 시도 (느릴 수 있음)
        logger.warning("   'conm' 컬럼을 찾을 수 없음, 전체 읽기 시도...")
        df_comp = pd.read_csv(PATH_COMPUSTAT, low_memory=False)
        logger.info(f"   ✅ Compustat 데이터 로드 성공 (전체): {len(df_comp):,} 행")
    except Exception as e:
        logger.error(f"   ❌ 읽기 실패: {e}")
        return False
    
    # ========== 단계2: 데이터 정리 ==========
    logger.info("\n단계 2/4: 회사명 정리 중...")
    
    # M&A의 acquiror_name 정리
    df_ma_target['clean_acquiror'] = df_ma_target['acquiror_name'].apply(clean_company_name)
    
    # Compustat의 conm 정리
    df_comp['clean_conm'] = df_comp['conm'].apply(clean_company_name)
    
    # Compustat 조회 세트 생성
    compustat_unique = df_comp[df_comp['clean_conm'] != ""][['conm', 'clean_conm']].drop_duplicates(subset=['clean_conm'])
    compustat_clean_set = set(compustat_unique['clean_conm'])
    compustat_clean_list = list(compustat_unique['clean_conm'])
    
    logger.info(f"   ✅ Compustat 고유 회사명: {len(compustat_clean_list):,}")
    
    # ========== 단계3: 매칭 수행 ==========
    logger.info("\n단계 3/4: 매칭 수행 중...")
    
    strict_res = []
    fuzzy_res = []
    unmatched_rows = []
    
    # 3.1 정확 매칭
    logger.info("   단계 3.1: 정확 매칭...")
    for idx, row in df_ma_target.iterrows():
        acquiror_orig = row['acquiror_name']
        acquiror_clean = row['clean_acquiror']
        
        if not acquiror_clean:
            continue
        
        if acquiror_clean in compustat_clean_set:
            strict_res.append({
                'Acquiror_Original': acquiror_orig,
                'Acquiror_Clean': acquiror_clean,
                'Matched_Compustat_Clean': acquiror_clean,
                'Match_Type': 'Strict',
                'Score': 100
            })
        else:
            unmatched_rows.append(row)
    
    logger.info(f"   ✅ 정확 매칭: {len(strict_res)} 건")
    logger.info(f"   퍼지 매칭 대기: {len(unmatched_rows)} 건")
    
    # 3.2 퍼지 매칭
    if len(unmatched_rows) > 0:
        logger.info(f"   단계 3.2: 퍼지 매칭 (임계값 {FUZZY_THRESHOLD})...")
        
        for row in tqdm(unmatched_rows, desc="   매칭 진행"):
            acquiror_orig = row['acquiror_name']
            acquiror_clean = row['clean_acquiror']
            
            match_result = process.extractOne(
                acquiror_clean, 
                compustat_clean_list, 
                scorer=fuzz.token_set_ratio,
                score_cutoff=FUZZY_THRESHOLD
            )
            
            if match_result:
                match_name, score, _ = match_result
                fuzzy_res.append({
                    'Acquiror_Original': acquiror_orig,
                    'Acquiror_Clean': acquiror_clean,
                    'Matched_Compustat_Clean': match_name,
                    'Match_Type': 'Fuzzy',
                    'Score': score
                })
        
        logger.info(f"   ✅ 퍼지 매칭: {len(fuzzy_res)} 건")
    
    # ========== 단계4: 검증 파일 생성 ==========
    logger.info("\n단계 4/4: 수동 검증 파일 생성 중...")
    
    # 결과 병합
    df_strict = pd.DataFrame(strict_res)
    df_fuzzy = pd.DataFrame(fuzzy_res)
    df_all_matches = pd.concat([df_strict, df_fuzzy], ignore_index=True)
    
    if df_all_matches.empty:
        logger.warning("   ⚠️  일치하는 결과 없음")
        return False
    
    # Compustat 원본 이름 찾기
    clean_to_original_map = dict(zip(compustat_unique['clean_conm'], compustat_unique['conm']))
    df_all_matches['Matched_Compustat_Original'] = df_all_matches['Matched_Compustat_Clean'].map(clean_to_original_map)
    
    # 출력 컬럼 선택
    output_columns = [
        'Acquiror_Original',
        'Matched_Compustat_Original',
        'Match_Type',
        'Score',
        'Acquiror_Clean',
        'Matched_Compustat_Clean'
    ]
    
    df_verify = df_all_matches[output_columns].copy()
    
    # 정렬: Fuzzy가 먼저, 점수가 낮은 것 우선 검토
    df_verify.sort_values(by=['Match_Type', 'Score'], ascending=[True, True], inplace=True)
    
    # 내보내기
    df_verify.to_excel(OUTPUT_VERIFICATION, index=False)
    
    # ========== 완료 요약 ==========
    duration = (datetime.now() - start_time).total_seconds()
    
    logger.info("\n" + "=" * 60)
    logger.info("단계4A 완료!")
    logger.info("=" * 60)
    logger.info(f"⏱  총 소요시간: {duration:.2f} 초")
    logger.info(f"📊 매칭 결과:")
    logger.info(f"   - 정확 매칭: {len(strict_res)}")
    logger.info(f"   - 퍼지 매칭: {len(fuzzy_res)}")
    logger.info(f"   - 총계: {len(df_verify):,} 쌍")
    logger.info(f"\n📁 출력 파일:")
    logger.info(f"   {OUTPUT_VERIFICATION}")
    logger.info(f"\n⚠️  다음 단계 (중요):")
    logger.info(f"   1. {OUTPUT_VERIFICATION} 열기")
    logger.info(f"   2. 수동 검토, 잘못된 매칭 행 삭제")
    logger.info(f"   3. 파일 저장 (파일명 유지)")
    logger.info(f"   4. 단계4B 실행 (Compustat匹配_단계4B_최적화버전.py)")
    logger.info("=" * 60)
    
    return True


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
