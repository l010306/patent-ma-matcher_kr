# -*- coding: utf-8 -*-
"""
최종 데이터 집계 최적화 버전 (단계3)
============================
기능: 슈퍼 사전을 사용하여 특허 데이터를 final_outcome 파일에 집계

개선점:
1. 상세한 로그 기록
2. 진행 상황 표시
3. 데이터 검증
4. 벡터화된 발명자 통계 (한국어 요구사항)
5. 누락된 컬럼 자동 처리
"""

import pandas as pd
import numpy as np
import pickle
import logging
from datetime import datetime
from tqdm import tqdm

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
        logging.FileHandler(f'logs/aggregation_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==========================================
# 1. 경로 설정
# ==========================================
# 슈퍼 사전 경로
DICT_PATH = '/Users/lidachuan/Desktop/Patent Data/Master_Company_Dictionary.pkl'

# 메인 데이터베이스 템플릿 경로
FINAL_OUTCOME_PATH = '/Users/lidachuan/Desktop/Patent Data/final_outcome.xlsx'

# 특허 데이터베이스 경로 (단일 CSV 또는 디렉토리)
PATENT_DB_PATH = '/Users/lidachuan/Desktop/Patent Data/1993-1997/patent_database.csv'

# 출력 파일 경로
OUTPUT_PATH = '/Users/lidachuan/Desktop/Patent Data/final_outcome_1993_1997_COMPLETE.xlsx'

# ==========================================
# 2. 발명자 통계 함수 (한국어 요구사항 준수)
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
    
    # 3. 두 값 중 더 큰 값 사용 (한국어 요구사항 준수)
    return np.maximum(num_from_column, num_from_names)

# ==========================================
# 3. 메인 처리 함수
# ==========================================

def load_master_dictionary():
    """슈퍼 사전 로드"""
    logger.info("단계 1/6: 슈퍼 사전 로드 중...")
    try:
        with open(DICT_PATH, 'rb') as f:
            master_dict = pickle.load(f)
        logger.info(f"   ✅ 사전 로드 성공, {len(master_dict):,} 개 매핑 관계 포함")
        return master_dict
    except FileNotFoundError:
        logger.error(f"   ❌ 오류: 사전 파일을 찾을 수 없음 {DICT_PATH}")
        logger.error("   먼저 단계2(슈퍼 사전 구축)를 실행하세요")
        raise


def load_main_database():
    """메인 데이터베이스 템플릿 로드"""
    logger.info("\n단계 2/6: 메인 데이터베이스 템플릿 로드 중...")
    try:
        df_main = pd.read_excel(FINAL_OUTCOME_PATH)
        df_main.drop_duplicates(subset=['acquiror_name'], keep='first', inplace=True)
        logger.info(f"   ✅ 템플릿 로드 성공, 총 {len(df_main):,} 개 회사")
        return df_main
    except FileNotFoundError:
        logger.error(f"   ❌ 오류: 템플릿 파일을 찾을 수 없음 {FINAL_OUTCOME_PATH}")
        raise


def load_patent_database():
    """특허 데이터베이스 로드"""
    logger.info("\n단계 3/6: 특허 데이터베이스 로드 중...")
    try:
        df_patent = pd.read_csv(PATENT_DB_PATH, low_memory=False)
        original_count = len(df_patent)
        
        # assignee가 비어있는 행 제거
        df_patent.dropna(subset=['assignee'], inplace=True)
        logger.info(f"   ✅ 특허 데이터 로드 완료: {len(df_patent):,} 건 유효 레코드 (원본 {original_count:,})")
        return df_patent
    except FileNotFoundError:
        logger.error(f"   ❌ 오류: 특허 데이터 파일을 찾을 수 없음 {PATENT_DB_PATH}")
        raise


def process_patent_data(df_patent, master_dict):
    """특허 데이터 처리: 사전 매핑 적용 및 발명자 통계"""
    logger.info("\n단계 4/6: 특허 데이터 처리 중...")
    
    # 매핑 적용
    logger.info("   사전 매핑 적용 중...")
    df_patent['assignee_stripped'] = df_patent['assignee'].astype(str).str.strip()
    df_patent['Matched_Acquiror'] = df_patent['assignee_stripped'].map(master_dict)
    
    # 매칭률 통계
    matched_count = df_patent['Matched_Acquiror'].notna().sum()
    match_rate = matched_count / len(df_patent) * 100
    logger.info(f"   ✅ 매핑 완료: {matched_count:,} / {len(df_patent):,} ({match_rate:.2f}%)")
    
    # 매칭 성공한 것만 유지
    df_matched = df_patent.dropna(subset=['Matched_Acquiror']).copy()
    
    # 연도 정리
    logger.info("   연도 데이터 정리 중...")
    df_matched['application_year'] = pd.to_numeric(df_matched['application_year'], errors='coerce')
    df_matched = df_matched.dropna(subset=['application_year'])
    df_matched['application_year'] = df_matched['application_year'].astype(int)
    
    # 발명자 수 통계 (한국어 요구사항 준수)
    logger.info("   발명자 수 계산 중...")
    inventor_name_cols = [f'inventor_name{i}' for i in range(1, 11)]
    
    # 컬럼이 존재하는지 확인
    for col in inventor_name_cols:
        if col not in df_matched.columns:
            df_matched[col] = np.nan
    
    df_matched['final_inventor_count'] = calculate_inventor_count_vectorized(
        df_matched, 
        inventor_name_cols
    )
    
    logger.info(f"   ✅ 처리 완료, 특허 당 평균 {df_matched['final_inventor_count'].mean():.2f} 명 발명자")
    
    return df_matched


def aggregate_data(df_matched):
    """데이터 집계: 회사 및 연도별 통계"""
    logger.info("\n단계 5/6: 데이터 집계 중...")
    
    # 회사 및 연도별 그룹화
    logger.info("   회사 및 연도별 그룹 통계 중...")
    df_grouped = df_matched.groupby(['Matched_Acquiror', 'application_year']).agg({
        'assignee': 'count',  # 특허 수
        'final_inventor_count': 'sum'  # 발명자 총수
    }).reset_index()
    
    # 피벗 테이블: 특허 수
    logger.info("   특허 수 피벗 테이블 생성 중...")
    pivot_patent = df_grouped.pivot(
        index='Matched_Acquiror', 
        columns='application_year', 
        values='assignee'
    )
    pivot_patent.columns = [f'patent_{int(col)}' for col in pivot_patent.columns]
    
    # 피벗 테이블: 발명자 수
    logger.info("   발명자 수 피벗 테이블 생성 중...")
    pivot_inventor = df_grouped.pivot(
        index='Matched_Acquiror', 
        columns='application_year', 
        values='final_inventor_count'
    )
    pivot_inventor.columns = [f'patent_inventor_{int(col)}' for col in pivot_inventor.columns]
    
    # 피벗 테이블 병합
    df_stats = pd.concat([pivot_patent, pivot_inventor], axis=1).reset_index()
    df_stats.rename(columns={'Matched_Acquiror': 'acquiror_name'}, inplace=True)
    
    logger.info(f"   ✅ 집계 완료, {len(df_stats)} 개 회사 포함")
    logger.info(f"   연도 범위: {pivot_patent.columns.tolist()[:3]}...{pivot_patent.columns.tolist()[-3:]}")
    
    # 회사 별칭 수집
    logger.info("   회사 별칭 수집 중...")
    df_names = df_matched.groupby('Matched_Acquiror')['assignee'].apply(
        lambda x: list(set(x))
    ).reset_index()
    
    # 별칭 목록 확장
    max_len = df_names['assignee'].apply(len).max() if not df_names.empty else 0
    name_cols = ['patent_name'] + [f'patent_name_{i}' for i in range(1, max_len)]
    names_expanded = pd.DataFrame(df_names['assignee'].tolist(), index=df_names.index)
    names_expanded = names_expanded.iloc[:, :len(name_cols)]
    names_expanded.columns = name_cols[:names_expanded.shape[1]]
    
    df_names = pd.concat([df_names[['Matched_Acquiror']], names_expanded], axis=1)
    df_names.rename(columns={'Matched_Acquiror': 'acquiror_name'}, inplace=True)
    
    return df_stats, df_names


def merge_to_final_outcome(df_main, df_stats, df_names):
    """최종 파일에 병합"""
    logger.info("\n단계 6/6: 최종 파일 병합 중...")
    
    # 기존의 오래된 컬럼 정리
    logger.info("   기존 통계 컬럼 정리 중...")
    cols_to_remove = [c for c in df_main.columns 
                     if c.startswith('patent_') or c.startswith('patent_inventor_')]
    if cols_to_remove:
        df_main = df_main.drop(columns=cols_to_remove, errors='ignore')
        logger.info(f"   {len(cols_to_remove)} 개 기존 컬럼 제거함")
    
    # 통계 데이터 병합
    logger.info("   통계 데이터 병합 중...")
    df_final = pd.merge(df_main, df_stats, on='acquiror_name', how='left')
    
    # 별칭 데이터 병합
    logger.info("   별칭 데이터 병합 중...")
    df_final = pd.merge(df_final, df_names, on='acquiror_name', how='left')
    
    # NaN을 0으로 채우기 (숫자 컬럼만)
    stat_cols = [c for c in df_final.columns 
                if (c.startswith('patent_') or c.startswith('patent_inventor_')) 
                and 'name' not in c]
    df_final[stat_cols] = df_final[stat_cols].fillna(0).astype(int)
    
    logger.info(f"   ✅ 병합 완료, 최종 파일 총 {len(df_final)} 행")
    
    # 데이터가 있는 회사 통계
    companies_with_patents = (df_final[stat_cols].sum(axis=1) > 0).sum()
    logger.info(f"   그 중 {companies_with_patents} 개 회사에 특허 데이터 있음")
    
    return df_final


def save_output(df_final):
    """출력 파일 저장"""
    logger.info("\n결과 저장 중...")
    df_final.to_excel(OUTPUT_PATH, index=False)
    logger.info(f"✅ 결과 저장 완료: {OUTPUT_PATH}")


# ==========================================
# 4. 메인 실행 프로세스
# ==========================================

def main():
    start_time = datetime.now()
    
    logger.info("=" * 60)
    logger.info("최종 데이터 집계 프로세스 시작 (최적화 버전)")
    logger.info("=" * 60)
    
    try:
        # 단계1: 사전 로드
        master_dict = load_master_dictionary()
        
        # 단계2: 메인 데이터베이스 로드
        df_main = load_main_database()
        
        # 단계3: 특허 데이터 로드
        df_patent = load_patent_database()
        
        # 단계4: 특허 데이터 처리
        df_matched = process_patent_data(df_patent, master_dict)
        
        # 단계5: 데이터 집계
        df_stats, df_names = aggregate_data(df_matched)
        
        # 단계6: 최종 파일에 병합
        df_final = merge_to_final_outcome(df_main, df_stats, df_names)
        
        # 결과 저장
        save_output(df_final)
        
        # 완료 요약
        duration = (datetime.now() - start_time).total_seconds()
        
        logger.info("\n" + "=" * 60)
        logger.info("처리 완료!")
        logger.info("=" * 60)
        logger.info(f"⏱  총 소요시간: {duration:.2f} 초")
        logger.info(f"📊 처리 속도: {len(df_patent) / duration:.0f} 건/초")
        logger.info(f"\n✅ 다음 단계: 단계4(Compustat 매칭) 실행")
        
        return True
        
    except Exception as e:
        logger.error(f"\n❌ 처리 실패: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
