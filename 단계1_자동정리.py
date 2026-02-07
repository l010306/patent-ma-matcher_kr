# -*- coding: utf-8 -*-
"""
특허 데이터 매칭 최적화 버전
==================
주요 개선사항:
1. fuzzywuzzy를 rapidfuzz로 대체 (성능 10-100배 향상)
2. 개선된 이름 정리 함수
3. 병렬 처리 지원
4. 품질 관리 검사
5. 더 나은 로그 및 진행 상황 추적
6. 개선된 발명자 통계 로직
"""

import pandas as pd
import numpy as np
from rapidfuzz import fuzz, process
from tqdm import tqdm
import re
import logging
from datetime import datetime
from multiprocessing import Pool, cpu_count
import warnings
warnings.filterwarnings('ignore')

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
        logging.FileHandler(f'logs/matching_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==========================================
# 1. 개선된 정리 함수
# ==========================================

def clean_company_name(name):
    """
    최적화된 표준화 정리 함수
    개선점:
    - 더 정확한 접미사 처리
    - 일반적인 약어 유지
    - 특수 문자의 컨텍스트 처리
    """
    if pd.isna(name) or not isinstance(name, str):
        return ""
    
    name = str(name).upper().strip()
    
    # 1. 일반적인 기호 처리
    name = name.replace('&', ' AND ')
    name = name.replace('-', ' ')
    name = name.replace("'", '')
    
    # 2. 일반적인 약어 확장 (매칭률 향상)
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
    
    # 3. 단계별 접미사 제거 (우선순위별)
    # 먼저 전체 형식을 제거하여 부분 매칭 문제 방지
    suffixes_priority = [
        # 전체 형식 우선
        r'\bINCORPORATED\b', r'\bCORPORATION\b', r'\bCOMPANY\b',
        r'\bLIMITED\b', r'\bGROUP\b',
        # 점이 포함된 약어
        r'\bCORP\.?\b', r'\bINC\.?\b', r'\bLTD\.?\b', 
        r'\bCO\.?\b', r'\bL\.L\.C\.?\b', r'\bPLC\.?\b',
        # 기타 형식
        r'\bLLC\b', r'\bS\.A\.\b', r'\bNV\b', r'\bGMBH\b',
        r'\bSA\b', r'\bAG\b', r'\bKK\b'
    ]
    
    for suffix in suffixes_priority:
        name = re.sub(suffix, '', name, flags=re.IGNORECASE)
    
    # 4. 구두점 제거 (문자, 숫자, 공백만 유지)
    name = re.sub(r'[^A-Z0-9\s]', ' ', name)
    
    # 5. 여러 공백 병합
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name


# ==========================================
# 2. 발명자 통계 함수 (최대값 사용)
# ==========================================

def calculate_inventor_count_vectorized(df, inventor_cols):
    """
    한국어 요구사항에 따라: inventors 컬럼과 이름 컬럼 개수 중 더 큰 값 사용
    벡터화된 연산으로 성능 향상
    """
    # 1. inventors 컬럼에서 숫자 가져오기
    num_from_column = pd.to_numeric(df['inventors'], errors='coerce').fillna(0)
    
    # 2. 이름 컬럼에서 개수 세기 (벡터화된 연산)
    num_from_names = df[inventor_cols].notna().sum(axis=1)
    
    # 3. 두 값 중 더 큰 값 사용 (한국어 요구사항: 둘 중 큰 값을 기준으로)
    return np.maximum(num_from_column, num_from_names)


# ==========================================
# 3. 배치 퍼지 매칭 함수 (병렬 처리 지원)
# ==========================================

def fuzzy_match_batch(args):
    """배치 퍼지 매칭 처리 (병렬 처리용)"""
    unmatched_chunk, acquiror_list, threshold, tier_name = args
    
    results = []
    for _, row in unmatched_chunk.iterrows():
        assignee = row['assignee']
        clean = row['clean_name']
        
        # rapidfuzz의 extractOne 사용, score_cutoff으로 사전 필터링
        match_result = process.extractOne(
            clean, 
            acquiror_list, 
            scorer=fuzz.token_set_ratio,
            score_cutoff=threshold
        )
        
        if match_result:
            match_name, score, _ = match_result
            results.append({
                'Assignee_Original': assignee,
                'Assignee_Clean': clean,
                'Matched_Acquiror_Clean': match_name,
                'Match_Type': f'Fuzzy (≥{threshold})',
                'Similarity': score,
                'Tier': tier_name
            })
    
    return results


# ==========================================
# 4. 품질 관리 검사
# ==========================================

def validate_matches(df_matches):
    """
    품질 관리 검사
    문제 목록과 통계 정보 반환
    """
    issues = []
    stats = {}
    
    if df_matches.empty:
        return issues, stats
    
    # 1. 일대다 매칭 감지 (하나의 특허 회사가 여러 인수 회사에 매칭)
    duplicates = df_matches.groupby('Assignee_Original')['Matched_Acquiror_Clean'].nunique()
    one_to_many = duplicates[duplicates > 1]
    if len(one_to_many) > 0:
        issues.append(f"⚠️  경고: {len(one_to_many)} 개의 특허 회사가 여러 인수 회사에 매칭됨 (수동 확인 필요)")
        stats['one_to_many'] = len(one_to_many)
    
    # 2. 점수 분포 확인
    score_dist = df_matches['Similarity'].describe()
    stats['score_distribution'] = score_dist.to_dict()
    
    low_score = df_matches[df_matches['Similarity'] < 95]
    if len(low_score) > 0:
        issues.append(f"ℹ️  정보: {len(low_score)} 개의 매칭 점수 < 95, 중점 검토 권장")
        stats['low_score_count'] = len(low_score)
    
    # 3. 너무 짧은 이름 감지 (오매칭 가능성)
    short_names = df_matches[df_matches['Assignee_Clean'].str.len() < 3]
    if len(short_names) > 0:
        issues.append(f"⚠️  경고: {len(short_names)} 개의 회사명이 너무 짧음 (예: '3M'), 수동 확인 필요")
        stats['short_names'] = len(short_names)
    
    # 4. 매칭 유형 통계
    match_type_dist = df_matches['Match_Type'].value_counts()
    stats['match_types'] = match_type_dist.to_dict()
    
    return issues, stats


# ==========================================
# 5. 메인 처리 프로세스
# ==========================================

def main():
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("특허 데이터 매칭 프로세스 시작 (최적화 버전)")
    logger.info("=" * 60)
    
    # ========== 데이터 로딩 ==========
    logger.info("데이터 로딩 중...")
    
    # 실제 경로에 맞게 수정하세요
    df_main = pd.read_excel('/Users/lidachuan/Desktop/Patent Data/final_outcome.xlsx')
    df_main.drop_duplicates(subset=['acquiror_name'], keep='first', inplace=True)
    logger.info(f"✅ 메인 데이터베이스 로딩 완료: {len(df_main)} 개 회사")
    
    df_patent = pd.read_csv('/Users/lidachuan/Desktop/Patent Data/1993-1997/patent_database.csv')
    df_patent.dropna(subset=['assignee'], inplace=True)
    logger.info(f"✅ 특허 데이터베이스 로딩 완료: {len(df_patent)} 건 레코드")
    
    # ========== 이름 정리 ==========
    logger.info("\n회사명 정리 중...")
    df_main['clean_name'] = df_main['acquiror_name'].apply(clean_company_name)
    df_patent['clean_name'] = df_patent['assignee'].apply(clean_company_name)
    
    # 정리 후 빈 행 제거
    df_patent = df_patent[df_patent['clean_name'] != ""].copy()
    logger.info(f"✅ 정리 완료, {len(df_patent)} 건의 유효한 특허 레코드 유지")
    
    # ========== 발명자 통계 ==========
    logger.info("\n발명자 수 계산 중...")
    inventor_name_cols = [f'inventor_name{i}' for i in range(1, 11)]
    
    # 컬럼이 존재하는지 확인
    for col in inventor_name_cols:
        if col not in df_patent.columns:
            df_patent[col] = np.nan
    
    # 벡터화된 함수로 발명자 수 계산 (최대값 사용, 한국어 요구사항에 부합)
    df_patent['final_inventor_count'] = calculate_inventor_count_vectorized(
        df_patent, 
        inventor_name_cols
    )
    logger.info(f"✅ 발명자 통계 완료, 특허 당 평균 {df_patent['final_inventor_count'].mean():.2f} 명")
    
    # ========== 요약 테이블 생성 ==========
    logger.info("\n회사 요약 테이블 생성 중...")
    df_summary = df_patent.groupby(['assignee', 'clean_name']).agg({
        'application_year': 'count',
        'final_inventor_count': 'sum'
    }).reset_index()
    
    df_summary.rename(columns={
        'application_year': 'patent_count', 
        'final_inventor_count': 'inventor_sum'
    }, inplace=True)
    
    df_summary = df_summary.sort_values(by='patent_count', ascending=False).reset_index(drop=True)
    logger.info(f"✅ 요약 완료, 총 {len(df_summary)} 개 특허 보유 회사")
    
    # 중간 파일 저장
    if not os.path.exists('temp'):
        os.makedirs('temp')
    df_summary.to_pickle("temp/temp_summary_optimized.pkl")
    
    # ========== 데이터 계층화 ==========
    logger.info("\n데이터 계층화 중...")
    total_count = len(df_summary)
    top_5_idx = int(total_count * 0.05)
    
    df_tier1 = df_summary.iloc[:top_5_idx].copy()
    df_remaining = df_summary.iloc[top_5_idx:].copy()
    df_tier2 = df_remaining[df_remaining['patent_count'] > 5].copy()
    df_tier3 = df_remaining[df_remaining['patent_count'] <= 5].copy()
    
    logger.info(f"✅ 계층화 완료:")
    logger.info(f"   - Tier 1 (상위 5%): {len(df_tier1)} 개 회사")
    logger.info(f"   - Tier 2 (>5개 특허): {len(df_tier2)} 개 회사")
    logger.info(f"   - Tier 3 (나머지): {len(df_tier3)} 개 회사")
    
    # ========== 매칭 준비 ==========
    acquiror_clean_set = set(df_main['clean_name'].dropna().unique())
    acquiror_clean_list = list(acquiror_clean_set)
    logger.info(f"\n인수 데이터베이스에 {len(acquiror_clean_list)} 개의 고유 회사명 포함")
    
    matches_for_review = []
    matches_auto = []
    
    # ========== 매칭 실행 ==========
    def perform_matching(df_target, tier_name, fuzzy_threshold=None, use_parallel=False):
        """범용 매칭 함수"""
        strict_res = []
        fuzzy_res = []
        
        logger.info(f"\n--- {tier_name} 처리 중 ({len(df_target)} 건 레코드) ---")
        
        # 1. 정확 매칭
        logger.info("  단계 1/2: 정확 매칭 수행 중...")
        unmatched_list = []
        
        for idx, row in df_target.iterrows():
            assignee = row['assignee']
            clean = row['clean_name']
            
            if clean in acquiror_clean_set:
                strict_res.append({
                    'Assignee_Original': assignee,
                    'Assignee_Clean': clean,
                    'Matched_Acquiror_Clean': clean,
                    'Match_Type': 'Strict',
                    'Similarity': 100,
                    'Tier': tier_name
                })
            else:
                unmatched_list.append(row)
        
        logger.info(f"  ✅ 정확 매칭: {len(strict_res)} 건 일치")
        
        # 2. 퍼지 매칭
        if fuzzy_threshold is not None and len(unmatched_list) > 0:
            logger.info(f"  단계 2/2: 퍼지 매칭 수행 중 (임계값={fuzzy_threshold})...")
            
            df_unmatched = pd.DataFrame(unmatched_list)
            
            if use_parallel and len(df_unmatched) > 100:
                # 병렬 처리 (데이터가 많을 때 사용)
                n_cores = min(cpu_count() - 1, 4)  # 최대 4개 코어 사용
                chunks = np.array_split(df_unmatched, n_cores)
                
                logger.info(f"  {n_cores} 개 코어로 병렬 처리 중...")
                with Pool(n_cores) as pool:
                    args_list = [(chunk, acquiror_clean_list, fuzzy_threshold, tier_name) 
                                for chunk in chunks]
                    results = pool.map(fuzzy_match_batch, args_list)
                
                # 결과 병합
                for batch_result in results:
                    fuzzy_res.extend(batch_result)
            else:
                # 순차 처리 (데이터가 적을 때 더 빠름)
                for _, row in tqdm(df_unmatched.iterrows(), total=len(df_unmatched), desc="  퍼지 매칭"):
                    assignee = row['assignee']
                    clean = row['clean_name']
                    
                    match_result = process.extractOne(
                        clean, 
                        acquiror_clean_list, 
                        scorer=fuzz.token_set_ratio,
                        score_cutoff=fuzzy_threshold
                    )
                    
                    if match_result:
                        match_name, score, _ = match_result
                        fuzzy_res.append({
                            'Assignee_Original': assignee,
                            'Assignee_Clean': clean,
                            'Matched_Acquiror_Clean': match_name,
                            'Match_Type': f'Fuzzy (≥{fuzzy_threshold})',
                            'Similarity': score,
                            'Tier': tier_name
                        })
            
            logger.info(f"  ✅ 퍼지 매칭: {len(fuzzy_res)} 건 일치")
        
        return strict_res, fuzzy_res
    
    # Tier 1: 정확 + 퍼지(90) → 전체 수동 검토
    t1_strict, t1_fuzzy = perform_matching(df_tier1, "Tier 1", fuzzy_threshold=90, use_parallel=True)
    matches_for_review.extend(t1_strict)
    matches_for_review.extend(t1_fuzzy)
    
    # Tier 2: 정확(자동) + 퍼지100(수동)
    t2_strict, t2_fuzzy = perform_matching(df_tier2, "Tier 2", fuzzy_threshold=100, use_parallel=True)
    matches_auto.extend(t2_strict)
    matches_for_review.extend(t2_fuzzy)
    
    # Tier 3: 정확만(자동)
    t3_strict, t3_fuzzy = perform_matching(df_tier3, "Tier 3", fuzzy_threshold=None)
    matches_auto.extend(t3_strict)
    
    # ========== 품질 검사 ==========
    logger.info("\n" + "=" * 60)
    logger.info("품질 관리 검사 수행 중...")
    logger.info("=" * 60)
    
    df_all_matches = pd.DataFrame(matches_for_review + matches_auto)
    issues, stats = validate_matches(df_all_matches)
    
    for issue in issues:
        logger.info(issue)
    
    # ========== 결과 내보내기 ==========
    logger.info("\n결과 파일 내보내기 중...")
    
    def get_orig_acquiror(clean_val):
        try:
            return df_main[df_main['clean_name'] == clean_val]['acquiror_name'].iloc[0]
        except:
            return ""
    
    # 1. 수동 검토 파일
    if matches_for_review:
        df_review = pd.DataFrame(matches_for_review)
        df_review['Original_Acquiror_Name'] = df_review['Matched_Acquiror_Clean'].apply(get_orig_acquiror)
        df_review.sort_values(by=['Match_Type', 'Similarity'], ascending=[True, True], inplace=True)
        
        output_review = "Step1_Manual_Review.xlsx"
        df_review.to_excel(output_review, index=False)
        logger.info(f"✅ 수동 검토 파일: {output_review} ({len(df_review)} 건)")
    
    # 2. 자동 승인 파일
    if matches_auto:
        df_auto = pd.DataFrame(matches_auto)
        df_auto['Original_Acquiror_Name'] = df_auto['Matched_Acquiror_Clean'].apply(get_orig_acquiror)
        
        output_auto = "Step1_Auto_Results.xlsx"
        df_auto.to_excel(output_auto, index=False)
        logger.info(f"✅ 자동 승인 파일: {output_auto} ({len(df_auto)} 건)")
    
    # ========== 통계 요약 ==========
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.info("\n" + "=" * 60)
    logger.info("처리 완료! 통계 요약:")
    logger.info("=" * 60)
    logger.info(f"총 소요시간: {duration:.2f} 초")
    logger.info(f"처리 속도: {len(df_patent) / duration:.0f} 건/초")
    logger.info(f"\n매칭 결과:")
    logger.info(f"  - 총 매칭 수: {len(df_all_matches)}")
    logger.info(f"  - 수동 검토 필요: {len(matches_for_review)}")
    logger.info(f"  - 자동 승인: {len(matches_auto)}")
    logger.info(f"  - 매칭률: {len(df_all_matches) / len(df_summary) * 100:.2f}%")
    
    if 'match_types' in stats:
        logger.info(f"\n매칭 유형 분포:")
        for match_type, count in stats['match_types'].items():
            logger.info(f"  - {match_type}: {count}")
    
    logger.info("\n로그 파일 저장 완료, 자세한 내용을 확인하세요.")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
