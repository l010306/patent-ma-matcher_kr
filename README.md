# patent-ma-matcher_kr
# 특허 데이터 처리 시스템 - 운영 가이드

## 📋 시스템 개요

본 시스템은 특허 데이터와 인수합병 데이터의 매칭, 집계 및 분석에 사용됩니다. 전체 프로세스는 4개의 주요 단계로 나뉘며, 각 단계마다 전용 스크립트와 명확한 입출력이 있습니다.

---

## 🚀 빠른 시작

### 환경 준비

```bash
# 1. 의존성 설치
pip install rapidfuzz pandas numpy openpyxl tqdm

# 2. 작업 디렉토리로 이동
cd "/Users/lidachuan/Desktop/Patent Data"
```

### 데이터 파일 준비

다음 파일이 존재하는지 확인하세요:
- `final_outcome.xlsx` - 인수합병 데이터베이스 템플릿 (acquiror_name, gvkey, cusip, cik 등 컬럼 포함)
- `patent_database.csv` - 특허 데이터 (또는 연도별 여러 CSV 파일)
- `compustat_19802025.csv` - Compustat 회사 데이터베이스

---

## 📝 전체 운영 프로세스

### 단계1️⃣: 자동 정리 및 매칭

**목적**: 회사명 정리 및 계층별 매칭

```bash
python 단계1_자동정리.py
```

**출력 파일**:
- `Step1_Manual_Review.xlsx` ⚠️ **수동 검토 필요**
- `Step1_Auto_Results.xlsx` ✅ 자동 승인 결과
- `logs/matching_log_*.log` - 상세 로그

**⚠️ 수동 검토 작업 (중요)**:
1. `Step1_Manual_Review.xlsx` 열기
2. 각 행의 매칭이 올바른지 확인
3. **잘못된 매칭 행 삭제**
4. 파일 저장 (파일명 유지)

**예상 소요시간**: 
- 1만 건 레코드: ~10초
- 10만 건 레코드: ~1-2분
- 50만 건 레코드: ~5-10분

---

### 단계2️⃣: 슈퍼 사전 구축

**목적**: 자동 결과와 수동 검토 결과를 병합하여 통합 회사명 매핑 사전 생성

```bash
python 단계2_슈퍼사전.py
```

**⚙️ 설정 (선택사항)**:
여러 연도 데이터가 있는 경우, `단계2_슈퍼사전.py`의 36-45행 편집:
```python
FILES_TO_PROCESS = [
    'Step1_Manual_Review.xlsx',   # 1993-1997년 (검토 완료)
    'Step1_Auto_Results.xlsx',
    # 다른 연도 추가...
]
```

**출력 파일**:
- `Master_Company_Dictionary.pkl` ✅ **핵심 사전** (단계3에서 사용)
- `Master_Company_Dictionary_VIEW.xlsx` - 수동 확인용
- `Dictionary_Build_Statistics.xlsx` - 통계 정보
- `Dictionary_Conflicts.xlsx` - 충돌 보고서 (있는 경우)
- `logs/dict_building_*.log` - 로그

**예상 소요시간**: < 10초

---

### 단계3️⃣: 최종 데이터 집계

**목적**: 슈퍼 사전을 사용하여 특허 데이터를 연도별로 final_outcome에 집계

```bash
python 단계3_최종집계.py
```

**출력 파일**:
- `final_outcome_1993_1997_COMPLETE.xlsx` ✅ **특허 통계가 포함된 완전한 파일**
  - 새 컬럼: `patent_1993`, `patent_1994`, ..., `patent_1997`
  - 새 컬럼: `patent_inventor_1993`, ..., `patent_inventor_1997`
  - 새 컬럼: `patent_name`, `patent_name_1`, ...(회사 별칭)
- `logs/aggregation_*.log` - 로그

**예상 소요시간**:
- 소규모 데이터셋: < 30초
- 중규모 데이터셋: 1-3분
- 대규모 데이터셋: 5-15분

---

### 단계4️⃣A: Compustat 매칭 - 검증 파일 생성

**목적**: 회사를 Compustat 데이터베이스와 매칭하여 검증 파일 생성

```bash
python 단계4A_Compustat매칭.py
```

**출력 파일**:
- `company_match_verification.xlsx` ⚠️ **수동 검토 필요**
- `logs/compustat_match_4A_*.log` - 로그

**⚠️ 수동 검토 작업 (중요)**:
1. `company_match_verification.xlsx` 열기
2. 매칭이 올바른지 확인 (Fuzzy 매칭 중점 검토)
3. **잘못된 매칭 행 삭제**
4. 파일 저장 (파일명 유지)

**예상 소요시간**: 1-5분

---

### 단계4️⃣B: Compustat 매칭 - 검토 결과 적용

**목적**: 검토된 Compustat ID를 final_outcome에 입력

```bash
python 단계4B_Compustat매칭.py
```

**출력 파일**:
- `final_outcome.xlsx` ✅ **최종 완전한 결과**
  - `gvkey`, `cusip`, `cik`, `compustat_name` 컬럼 채워짐
- `logs/compustat_merge_4B_*.log` - 로그

**예상 소요시간**: < 1분

---

## ✅ 완료!

전체 프로세스 실행 완료 후, `final_outcome.xlsx`에 포함되는 내용:
- ✅ 인수합병 회사 정보
- ✅ 연도별 특허 수 통계
- ✅ 연도별 발명자 수 통계
- ✅ 특허 회사 별칭 목록
- ✅ Compustat 회사 ID (gvkey, cusip, cik)

---

## 📂 파일 구성

```
Patent Data/
├── 단계1_자동정리.py              # 핵심 스크립트
├── 단계2_슈퍼사전.py
├── 단계3_최종집계.py
├── 단계4A_Compustat매칭.py
├── 단계4B_Compustat매칭.py
│
├── logs/                          # 로그 폴더 (자동 생성)
├── temp/                          # 임시 폴더 (자동 생성)
│
├── final_outcome.xlsx             # 최종 결과 ⭐
├── Master_Company_Dictionary.pkl  # 핵심 사전
│
├── Step1_Manual_Review.xlsx       # 단계1 검토 필요
├── Step1_Auto_Results.xlsx
├── company_match_verification.xlsx # 단계4A 검토 필요
│
└── 전체_프로세스_사용_가이드_한국어.md  # 상세 문서
```

---

## ⚙️ 일반적인 조정

### 퍼지 매칭 임계값 수정

**단계1** - `단계1_자동정리.py` 364행 편집:
```python
# 90을 85(더 관대함) 또는 95(더 엄격함)로 변경
t1_strict, t1_fuzzy = perform_matching(df_tier1, "Tier 1", fuzzy_threshold=90)
```

**단계4A** - `단계4A_Compustat매칭.py` 40행 편집:
```python
FUZZY_THRESHOLD = 90  # 다른 값으로 변경
```

### 병렬 처리 코어 수 조정

`단계1_자동정리.py` 323행 편집:
```python
n_cores = min(cpu_count() - 1, 4)  # 2 또는 8로 변경
```

### 병렬 처리 비활성화 (메모리 문제 발생 시)

`단계1_자동정리.py` 364행 편집:
```python
# use_parallel=True를 False로 변경
t1_strict, t1_fuzzy = perform_matching(df_tier1, "Tier 1", fuzzy_threshold=90, use_parallel=False)
```

---

## 🔍 문제 해결

### Q: NumPy 버전 경고
```bash
pip install --upgrade pandas pyarrow numexpr bottleneck
```

### Q: rapidfuzz 설치 실패 (M1/M2 Mac)
```bash
pip install --upgrade pip
pip install rapidfuzz --no-binary :all:
```

### Q: Compustat 데이터 로드가 매우 느림
- 단계4A 스크립트 사용 확인 (conm 컬럼만 읽음)
- 데이터 파일 크기 줄이기 또는 연도별 배치 처리 시도

### Q: 메모리 부족
- 병렬 처리 비활성화
- CPU 코어 수 감소
- 연도별 데이터 분할

### Q: 매칭률이 너무 낮음
- 퍼지 매칭 임계값 낮추기 (85 또는 80)
- 회사명 차이가 너무 큰지 확인
- 로그 파일에서 세부 정보 확인

---

## 📊 성능 최적화

본 시스템은 다음 기술을 사용하여 고성능을 구현합니다:

| 최적화 기술 | 향상 효과 |
|---------|------------|
| rapidfuzz (C++) | 10-100배 |
| 병렬 처리 | 3-4배 |
| 벡터화 연산 | 10-50배 |
| **종합 향상** | **4-10배** |

---

## 💡 중요 안내

1. ⚠️ **단계1과 단계4A의 수동 검토는 필수이며**, 건너뛸 수 없습니다
2. ✅ 모든 스크립트는 `logs/`와 `temp/` 폴더를 자동으로 생성합니다
3. 📝 로그 파일에 상세 정보가 포함되어 있으므로 문제 발생 시 우선적으로 확인하세요
4. 🔄 특정 단계를 다시 실행해야 하는 경우 해당 스크립트를 다시 실행하면 됩니다
5. 💾 중요한 파일은 자동으로 백업되며, 이전 파일명에 타임스탬프가 포함됩니다

---

## 📞 기술 지원

- 상세 문서: `전체_프로세스_사용_가이드_한국어.md`
- 파일 설명: `파일_구성_설명_한국어.md`
- 로그 위치: `logs/` 폴더

**버전**: v1.0  
**업데이트 날짜**: 2026-02-07
