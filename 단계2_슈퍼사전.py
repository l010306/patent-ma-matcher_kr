# -*- coding: utf-8 -*-
"""
슈퍼 사전 구축 최적화 버전 (단계2)
============================
기능: 자동 매칭과 수동 검토 결과를 병합하여 마스터 회사 사전 구축

개선점:
1. 더 나은 오류 처리 및 검증
2. 충돌 감지 및 보고
3. 상세한 로그 기록
4. 통계 정보 출력
"""

import pandas as pd
import os
import pickle
import logging
from datetime import datetime

# ==========================================
# 로그 설정
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'dict_building_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==========================================
# 1. 설정: 입력 파일 목록
# ==========================================
# 여러 연도 파일을 포함할 수 있으며, 필요에 따라 추가
FILES_TO_PROCESS = [
    # --- 1993-1997년 파일 ---
    'Step1_Manual_Review.xlsx',      # 수동 검토 후 파일 (오류 매칭 제거)
    'Step1_Auto_Results.xlsx',       # 자동 매칭 결과
    
    # --- 다른 연도가 있으면 여기에 추가 ---
    # '1998_2000_Manual_Review.xlsx',
    # '1998_2000_Auto_Results.xlsx',

]

# 출력 파일 설정
OUTPUT_DICT_FILE = 'Master_Company_Dictionary.pkl'       # 코드 로딩용 (Pickle 형식)
OUTPUT_EXCEL_FILE = 'Master_Company_Dictionary_VIEW.xlsx' # 수동 확인용 (Excel 형식)

# ==========================================
# 2. 메인 처리 함수
# ==========================================

def build_master_dictionary(files_list):
    """
    슈퍼 사전 구축
    반환: master_dict, statistics
    """
    logger.info("=" * 60)
    logger.info("슈퍼 사전 구축 시작 (마스터 회사 사전)")
    logger.info("=" * 60)
    
    master_dict = {}  # 구조: { 'Assignee_Original': 'Original_Acquiror_Name' }
    source_stats = []
    conflicts = []  # 충돌 기록
    
    for file_path in files_list:
        if not os.path.exists(file_path):
            logger.warning(f"⚠️  건너뛰기: 파일을 찾을 수 없음 {file_path}")
            continue
        
        logger.info(f"\n처리 중: {file_path}")
        
        try:
            df = pd.read_excel(file_path)
            
            # 필수 컬럼 확인
            required_cols = ['Assignee_Original', 'Original_Acquiror_Name']
            if not all(col in df.columns for col in required_cols):
                logger.error(f"   ❌ 오류: 필수 컬럼 {required_cols} 누락, 이 파일 건너뛰기")
                continue
            
            # 유효하지 않은 행 필터링
            df_valid = df.dropna(subset=required_cols)
            df_valid = df_valid[
                (df_valid['Assignee_Original'].astype(str).str.strip() != "") &
                (df_valid['Original_Acquiror_Name'].astype(str).str.strip() != "")
            ]
            
            logger.info(f"   유효 행 수: {len(df_valid)}")
            
            # 통계 정보
            count_new = 0
            count_duplicate = 0
            count_conflict = 0
            
            for idx, row in df_valid.iterrows():
                assignee_raw = str(row['Assignee_Original']).strip()
                acquiror_std = str(row['Original_Acquiror_Name']).strip()
                
                if assignee_raw not in master_dict:
                    # 새 매핑
                    master_dict[assignee_raw] = acquiror_std
                    count_new += 1
                else:
                    # 이미 존재하는 매핑
                    existing = master_dict[assignee_raw]
                    if existing == acquiror_std:
                        # 중복이지만 일치
                        count_duplicate += 1
                    else:
                        # 충돌!
                        count_conflict += 1
                        conflicts.append({
                            'Assignee': assignee_raw,
                            'Existing_Acquiror': existing,
                            'New_Acquiror': acquiror_std,
                            'Source_File': file_path
                        })
                        # 전략: 첫 번째 매핑 유지, 충돌 기록
                        logger.warning(f"   ⚠️  충돌: '{assignee_raw}'은 이미 '{existing}'로 매핑됨, 새 값 '{acquiror_std}' 무시됨")
            
            logger.info(f"   ✅ 처리 완료: 신규 {count_new}, 중복 {count_duplicate}, 충돌 {count_conflict}")
            
            source_stats.append({
                'File': os.path.basename(file_path),
                'Valid_Rows': len(df_valid),
                'New_Mappings': count_new,
                'Duplicates': count_duplicate,
                'Conflicts': count_conflict
            })
            
        except Exception as e:
            logger.error(f"   ❌ 읽기 실패: {e}")
    
    return master_dict, source_stats, conflicts


def save_dictionary(master_dict, source_stats, conflicts):
    """사전 및 통계 정보 저장"""
    logger.info("\n" + "=" * 60)
    logger.info("결과 저장")
    logger.info("=" * 60)
    
    if not master_dict:
        logger.error("❌ 오류: 사전이 비어있습니다! 매핑 관계를 추출하지 못했습니다.")
        return False
    
    # 1. Pickle로 저장 (후속 코드 로딩용)
    with open(OUTPUT_DICT_FILE, 'wb') as f:
        pickle.dump(master_dict, f)
    logger.info(f"✅ Pickle 파일 저장 완료: {OUTPUT_DICT_FILE}")
    
    # 2. Excel로 저장 (수동 확인용)
    df_out = pd.DataFrame(
        list(master_dict.items()), 
        columns=['Assignee_Original_Name', 'Mapped_Acquiror_Name']
    )
    df_out = df_out.sort_values('Mapped_Acquiror_Name').reset_index(drop=True)
    df_out.to_excel(OUTPUT_EXCEL_FILE, index=False)
    logger.info(f"✅ Excel 파일 저장 완료: {OUTPUT_EXCEL_FILE}")
    
    # 3. 통계 정보 저장
    if source_stats:
        df_stats = pd.DataFrame(source_stats)
        stats_file = 'Dictionary_Build_Statistics.xlsx'
        df_stats.to_excel(stats_file, index=False)
        logger.info(f"✅ 통계 정보 저장 완료: {stats_file}")
    
    # 4. 충돌이 있으면 충돌 보고서 저장
    if conflicts:
        df_conflicts = pd.DataFrame(conflicts)
        conflict_file = 'Dictionary_Conflicts.xlsx'
        df_conflicts.to_excel(conflict_file, index=False)
        logger.warning(f"⚠️  충돌 보고서 저장 완료: {conflict_file} ({len(conflicts)} 건 충돌)")
    
    return True


def print_summary(master_dict, source_stats, conflicts):
    """요약 정보 출력"""
    logger.info("\n" + "=" * 60)
    logger.info("구축 완료 요약")
    logger.info("=" * 60)
    
    logger.info(f"\n📊 전체 통계:")
    logger.info(f"   - 총 매핑 관계 수: {len(master_dict):,}")
    logger.info(f"   - 처리한 파일 수: {len(source_stats)}")
    logger.info(f"   - 감지된 충돌: {len(conflicts)}")
    
    if source_stats:
        logger.info(f"\n📁 각 파일 기여도:")
        for stat in source_stats:
            logger.info(f"   {stat['File']}")
            logger.info(f"      신규: {stat['New_Mappings']}, 중복: {stat['Duplicates']}, 충돌: {stat['Conflicts']}")
    
    # 동일 회사에 매핑된 변형 수 통계
    from collections import Counter
    acquiror_counts = Counter(master_dict.values())
    top_companies = acquiror_counts.most_common(10)
    
    logger.info(f"\n🏢 가장 많은 변형을 가진 회사 (상위 10):")
    for company, count in top_companies:
        logger.info(f"   {company}: {count} 개 변형")
    
    if conflicts:
        logger.info(f"\n⚠️  경고: {len(conflicts)} 개 충돌 발견, Dictionary_Conflicts.xlsx 확인 필요")
        logger.info("   충돌 처리 전략: 첫 매핑 유지")


# ==========================================
# 3. 메인 실행 프로세스
# ==========================================

def main():
    start_time = datetime.now()
    
    # 사전 구축
    master_dict, source_stats, conflicts = build_master_dictionary(FILES_TO_PROCESS)
    
    # 결과 저장
    success = save_dictionary(master_dict, source_stats, conflicts)
    
    if success:
        # 요약 출력
        print_summary(master_dict, source_stats, conflicts)
        
        # 소요 시간 계산
        duration = (datetime.now() - start_time).total_seconds()
        
        logger.info(f"\n⏱  총 소요시간: {duration:.2f} 초")
        logger.info("\n" + "=" * 60)
        logger.info("🎉 슈퍼 사전 구축 성공!")
        logger.info("=" * 60)
        logger.info(f"\n다음 단계:")
        logger.info(f"   1. {OUTPUT_EXCEL_FILE} 확인하여 매핑 관계 검증")
        logger.info(f"   2. 충돌이 있으면 Dictionary_Conflicts.xlsx 검토")
        logger.info(f"   3. 단계3(최종 집계) 실행하여 이 사전 사용")
        
        return True
    else:
        logger.error("\n❌ 사전 구축 실패")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
