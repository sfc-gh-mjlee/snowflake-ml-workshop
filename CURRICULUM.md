# Snowflake End-to-End ML 교육 커리큘럼 상세 문서

**버전**: 2.0  
**대상 환경**: krdemo (wf00579.ap-northeast-2.aws)  
**데모 시나리오**: TPC-H 고객 생애 가치 예측 (LTV Prediction)  
**총 교육 시간**: 약 6시간

---

## 교육 개요

### 비즈니스 문제
TPC-H 데이터베이스의 고객·주문 데이터를 활용해 **"이 고객이 향후 12개월간 얼마를 구매할 것인가? (FUTURE_12M_REVENUE)"** 를 예측하는 회귀(Regression) 모델을 구축합니다. 과거 구매 패턴을 분석하여 고객별 LTV(생애 가치)를 예측하고, 마케팅 예산 배분·VIP 등급 자동 부여·개인화 캠페인 등에 활용합니다. 데이터 준비부터 모델 운영까지 Snowflake ML의 전체 라이프사이클을 단일 플랫폼에서 경험합니다.

### 사용 데이터
| 테이블 | 위치 | 설명 |
|--------|------|------|
| CUSTOMER | SNOWFLAKE_SAMPLE_DATA.TPCH_SF1 | 고객 정보 (150,000건) |
| ORDERS | SNOWFLAKE_SAMPLE_DATA.TPCH_SF1 | 주문 내역 (1,500,000건) |
| LINEITEM | SNOWFLAKE_SAMPLE_DATA.TPCH_SF1 | 주문 상세 (6,000,000건) |
| NATION | SNOWFLAKE_SAMPLE_DATA.TPCH_SF1 | 국가 정보 |
| CUSTOMER_FEATURES | DEMO.ML_DEMO | 전처리된 피처 테이블 (99,996건) |

### 학습 목표
1. Snowflake ML의 전체 아키텍처와 각 컴포넌트 역할 이해
2. 데이터 이동 없이 Snowflake 내에서 완전한 ML 파이프라인 구축 경험
3. Feature Store, Model Registry, SPCS 등 핵심 기능 실습
4. 시계열 데이터에서 Data Leakage 방지를 위한 시간 분리(Temporal Split) 설계
5. 운영 환경에서의 모니터링 및 자동화 방법 습득

---

## Module 1: Snowflake ML 개요

**유형**: 이론 강의  
**소요 시간**: 30분  
**형식**: 프레젠테이션 + Q&A

### 전달 내용

#### 1-1. 왜 Snowflake에서 ML을 하는가?

기존 ML 워크플로우의 문제점을 먼저 짚어줍니다.

- **데이터 이동 문제**: 데이터 웨어하우스에서 외부 ML 플랫폼으로 데이터를 복사해야 하는 비효율
  - 예: Snowflake → S3 → SageMaker → 예측 결과 → Snowflake 재적재
  - 결과: 보안 위험, 비용 증가, 데이터 일관성 문제
- **거버넌스 단절**: 학습 데이터·모델·예측 결과가 서로 다른 플랫폼에 분산
- **인프라 관리 부담**: GPU 서버, 컨테이너, 의존성 관리 등 ML 인프라 운영 복잡성

Snowflake ML이 해결하는 방식:
- 데이터가 있는 곳에서 바로 모델 학습 및 추론 수행
- 기존 Snowflake 거버넌스(RBAC, 마스킹 정책, 태그)가 ML 자산에 자동 적용
- 인프라 관리 없이 CPU/GPU 컴퓨팅 자동 할당

#### 1-2. Snowflake ML 아키텍처

전체 컴포넌트를 레이어별로 설명합니다.

```
┌─────────────────────────────────────────────────────────┐
│                      데이터 레이어                          │
│    Snowflake Tables / External Data / Streaming           │
├─────────────────────────────────────────────────────────┤
│                  피처 엔지니어링 레이어                       │
│    Feature Store  │  Snowpark DataFrames  │  Datasets     │
├─────────────────────────────────────────────────────────┤
│                    모델 개발 레이어                          │
│    Notebooks (Container Runtime)  │  ML Jobs              │
│    Experiment Tracking  │  Distributed Training           │
├─────────────────────────────────────────────────────────┤
│                    모델 관리 레이어                          │
│    Model Registry  │  버전 관리  │  거버넌스                  │
├─────────────────────────────────────────────────────────┤
│                      추론 레이어                            │
│    Batch Inference (mv.run)  │  SPCS Real-time            │
│    Dynamic Table Inference                                │
├─────────────────────────────────────────────────────────┤
│                      운영 레이어                            │
│    ML Observability  │  ML Lineage  │  Task Graph          │
└─────────────────────────────────────────────────────────┘
```

#### 1-3. 핵심 컴포넌트 대응표

| 컴포넌트 | 역할 | AWS 서비스 비교 |
|----------|------|----------------|
| Notebooks (Container Runtime) | 브라우저 기반 Jupyter, CPU/GPU 학습 | SageMaker Studio |
| Feature Store | 피처 정의·저장·재사용·실시간 서빙 | SageMaker Feature Store |
| ML Jobs | 외부 IDE에서 Snowflake 컴퓨팅으로 코드 제출 | SageMaker Training Jobs |
| Experiment Tracking | 실험 메트릭·파라미터 로깅·비교 | MLflow, W&B |
| Model Registry | 모델 버전 관리·거버넌스·배포 | MLflow Registry |
| Batch Inference | SQL 기반 대량 예측 | SageMaker Batch Transform |
| SPCS Real-time | 컨테이너 기반 REST 추론 서비스 | SageMaker Endpoints |
| ML Observability | 모델 드리프트·성능 모니터링 | SageMaker Model Monitor |
| ML Lineage | 데이터~모델 계보 추적 | SageMaker Lineage |
| Task Graph | ML 파이프라인 자동화·스케줄링 | Step Functions, Airflow |

#### 1-4. 데모 시나리오 소개

**시나리오: 고객 LTV 예측 (Churn Prediction)**

- **비즈니스 목표**: 고객별 향후 12개월 예상 구매금액(LTV)을 예측하여 마케팅 예산 최적화 및 VIP 등급 자동 부여
- **예측 대상**: `FUTURE_12M_REVENUE` (1 = 이탈, 0 = 유지)
- **데이터 기간**: TPC-H ORDERS 테이블 (1992-01-01 ~ 1998-08-02, 6.5년)

**시간 분리 전략 (Temporal Split)**

LTV 예측은 **시간적 순서**가 중요한 ML 문제입니다. 과거 데이터로 학습하여 미래를 예측해야 하므로, 랜덤 분할이 아닌 **시간 기준 분할**이 필수입니다.

```
┌────────────────────────────────────────────────────────────┐
│  관찰 기간 (Observation Period)                              │
│  1992-01-01 ~ 1997-06-30  (5.5년)                          │
│  → 이 기간의 주문 패턴으로 피처 생성                             │
│  → TOTAL_ORDERS, AVG_ORDER_VALUE, RECENCY 등                │
└────────────────────────────────────────────────────────────┘
                    ↓
┌────────────────────────────────────────────────────────────┐
│  예측 윈도우 (Prediction Window)                             │
│  1997-07-01 ~ 1998-06-30  (12개월)                         │
│  → 이 기간의 총 구매금액 = FUTURE_12M_REVENUE (타겟)                │
│  → ⚠️ 이 기간 데이터는 LTV 레이블 계산에만 사용! 피처에 사용 금지   │
└────────────────────────────────────────────────────────────┘
                    ↓
┌────────────────────────────────────────────────────────────┐
│  Train/Test 분할                                            │
│  고객을 80:20으로 랜덤 분할 (시간 분할 아님)                   │
│  → 같은 관찰기간/레이블 구간을 공유하되 고객 단위로 분할         │
└────────────────────────────────────────────────────────────┘
```

**Data Leakage 방지**

- ❌ 잘못된 방법: 전체 기간 데이터로 피처 생성 후 랜덤 분할 → **미래 정보 유출**
- ✅ 올바른 방법: 관찰 기간 데이터만으로 피처 생성 → 예측 윈도우는 레이블링에만 사용

**클래스 분포**
- LTV 고객 (FUTURE_12M_REVENUE=1): 약 34% (34,054명)
- 유지 고객 (FUTURE_12M_REVENUE=0): 약 66% (65,927명)
- → 비교적 균형잡힌 데이터셋, 별도 샘플링 불필요
## Module 2: 데이터 탐색 및 전처리

**유형**: 실습  
**소요 시간**: 45분  
**노트북**: `02_data_exploration.ipynb`

### 학습 목표
- Snowpark DataFrame API로 대규모 데이터를 Snowflake 내에서 처리하는 방법 이해
- ML 타겟 변수 설계 및 피처 엔지니어링 방법 습득
- Snowflake ML 전처리 파이프라인 구성 방법 이해

### 전달 내용

#### 2-1. Snowpark DataFrame 소개

Snowpark의 핵심 동작 방식을 pandas와 비교해 설명합니다.

- **Lazy Evaluation**: `.show()`, `.collect()`, `.count()` 호출 시 SQL로 변환되어 Warehouse에서 실행
- **Pushdown 최적화**: Python 코드가 Snowflake SQL로 자동 변환 — 중간 데이터가 클라이언트로 전송되지 않음

```python
# Snowpark 방식 — 데이터가 Snowflake에 머무름
df = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER")
df.filter(col("C_ACCTBAL") > 5000).count()  # SQL로 변환되어 Warehouse에서 실행

# pandas 방식 — 전체 데이터가 클라이언트로 이동 후 처리
import pandas as pd
df = pd.read_sql("SELECT * FROM CUSTOMER", conn)  # 150,000건 전체 전송
df[df["C_ACCTBAL"] > 5000].count()              # 로컬 메모리에서 실행
```

`df.explain()`으로 생성된 SQL 실행 계획을 직접 확인해 Pushdown이 실제로 동작하는지 보여주면 효과적입니다.

#### 2-2. 탐색적 데이터 분석 (EDA)

확인해야 할 항목과 그 이유를 함께 설명합니다.

| 확인 항목 | 방법 | 이유 |
|-----------|------|------|
| 스키마·로우 수 | `df.schema`, `df.count()` | 데이터 규모 파악 |
| 결측값 비율 | `df.select([count(when(isnan(c), c)).alias(c)])` | 전처리 전략 수립 |
| 수치형 분포 | `df.describe()`, plotly histogram | 이상치·스케일 차이 파악 |
| C_MKTSEGMENT 분포 | `df.group_by().count()` | 카테고리 인코딩 방법 결정 |
| 테이블 JOIN 관계 | 키 중복 여부 확인 | 데이터 중복(Fanout) 방지 |

#### 2-3. 타겟 변수 설계

비즈니스 관점에서 타겟 변수 정의 방식을 설명합니다.

```sql
-- 타겟: 고객의 재구매 여부이 전체 중앙값 초과 → FUTURE_12M_REVENUE = 1
FUTURE_12M_REVENUE = IFF(AVG_ORDER_VALUE > MEDIAN(AVG_ORDER_VALUE), 1, 0)
```

- 중앙값 기준을 쓰는 이유: 평균은 이상치에 민감, 중앙값이 더 견고
- 결과: 50:50 클래스 밸런스 → 데모에서 불균형 처리 불필요
- 실제 비즈니스에서는 도메인 지식 반영 필요 (상위 20%, 구매 금액 절대값 기준 등)

**생성 피처 목록**

| 피처명 | 설명 | 타입 |
|--------|------|------|
| C_ACCTBAL | 계정 잔액 | Numeric |
| C_MKTSEGMENT | 시장 세그먼트 (5개) | Categorical |
| TOTAL_ORDERS | 총 주문 횟수 | Numeric |
| AVG_ORDER_VALUE | 평균 주문 금액 | Numeric |
| TOTAL_REVENUE | 총 매출액 | Numeric |
| AVG_DISCOUNT | 평균 할인율 | Numeric |
| NET_REVENUE | 순 매출 (할인 적용) | Numeric |
| DISTINCT_PARTS | 구매한 고유 제품 수 | Numeric |
| CUSTOMER_TENURE_DAYS | 고객 활동 기간(일) | Numeric |
| NATION | 국가명 | Categorical |

#### 2-4. Snowflake ML 전처리 파이프라인

sklearn 호환 API를 쓰지만 실행은 Snowflake에서 일어납니다.

```python
from snowflake.ml.modeling.preprocessing import StandardScaler, OrdinalEncoder
from snowflake.ml.modeling.pipeline import Pipeline

pipeline = Pipeline(steps=[
    ("encoder", OrdinalEncoder(
        input_cols=["C_MKTSEGMENT", "NATION"],
        output_cols=["C_MKTSEGMENT_ENC", "NATION_ENC"]
    )),
    ("scaler", StandardScaler(
        input_cols=NUMERIC_COLS,
        output_cols=[f"{c}_SCALED" for c in NUMERIC_COLS]
    ))
])

pipeline.fit(train_df)          # Warehouse에서 통계 계산
transformed_df = pipeline.transform(train_df)  # Warehouse에서 변환
```

핵심 포인트:
- sklearn과 동일한 API → 기존 코드 재사용 가능
- 파이프라인 객체를 Model Registry에 함께 저장 → 추론 시 동일 전처리 자동 적용, 학습·서빙 불일치(Training-Serving Skew) 방지

### 실습 포인트
- `df.explain()`으로 생성된 SQL 확인
- 피처 분포 시각화 후 어떤 전처리가 필요한지 토론
- 타겟 변수 정의 기준을 다른 방식으로 바꿨을 때 클래스 비율 변화 확인

---

## Module 3: Feature Store

**유형**: 실습  
**소요 시간**: 45분  
**노트북**: `03_feature_store.ipynb`

### 학습 목표
- Feature Store의 필요성과 핵심 개념(Entity, Feature View) 이해
- Managed Feature View를 통한 자동 피처 갱신 방법 습득
- 포인트-인-타임 정합성이 중요한 이유 이해

### 전달 내용

#### 3-1. Feature Store가 없을 때 발생하는 문제

- **피처 중복 개발**: 팀 A와 팀 B가 "고객 평균 주문 금액"을 각자 다르게 계산
- **학습-서빙 불일치(Training-Serving Skew)**: 학습 시와 추론 시 피처 계산 로직이 달라 예측 품질 저하
- **포인트-인-타임 오염(Data Leakage)**: 미래 데이터가 학습에 포함되어 실제 배포 시 성능 급락
- **재사용 불가**: 한 모델에서 만든 피처를 다른 모델이 재사용하기 어려움

#### 3-2. 핵심 개념

```
Entity (엔티티)
└── 피처의 기준이 되는 비즈니스 객체
    예) Customer (기준키: C_CUSTKEY)
        Product (기준키: PRODUCT_ID)

Feature View (피처 뷰)
└── Entity에 대한 피처 집합
    ├── Static Feature View  : 정적 뷰 (스냅샷)
    └── Managed Feature View : Dynamic Table 기반 자동 갱신

Feature Store
└── Entity와 Feature View를 관리하는 중앙 저장소
```

```python
from snowflake.ml.feature_store import FeatureStore, FeatureView, Entity
from snowflake.ml.feature_store import CreationMode

# Feature Store 초기화
fs = FeatureStore(
    session=session,
    database="DEMO",
    name="ML_DEMO",
    default_warehouse="COMPUTE_WH",
    creation_mode=CreationMode.CREATE_IF_NOT_EXIST
)

# Entity 정의 및 등록
customer_entity = Entity(name="CUSTOMER", join_keys=["C_CUSTKEY"])
fs.register_entity(customer_entity)
```

#### 3-3. Managed Feature View — 자동 갱신

Dynamic Table을 활용한 피처 자동 갱신을 실습합니다.

```python
feature_df = session.sql("""
    SELECT
        O_CUSTKEY,
        COUNT(O_ORDERKEY)   AS ORDER_COUNT_30D,
        AVG(O_TOTALPRICE)   AS AVG_ORDER_VALUE_30D,
        SUM(O_TOTALPRICE)   AS TOTAL_REVENUE_30D
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
    WHERE O_ORDERDATE >= DATEADD(day, -30, CURRENT_DATE())
    GROUP BY O_CUSTKEY
""")

fv = FeatureView(
    name="customer_order_stats",
    entities=[customer_entity],
    feature_df=feature_df,
    refresh_freq="1 hour",   # 매 1시간 자동 갱신
    desc="최근 30일 주문 통계"
)

fs.register_feature_view(feature_view=fv, version="1")
```

**갱신 주기 선택 기준**

| refresh_freq | 적합 사례 |
|-------------|----------|
| `"1 minute"` | 실시간성이 중요한 피처 (거래 빈도 등) |
| `"1 hour"` | 시간 단위 집계 피처 |
| `"1 day"` | 일별 배치 피처 (가장 일반적) |

#### 3-4. 포인트-인-타임 정합성 (Point-in-Time Correctness)

가장 중요한 개념입니다. 화이트보드 또는 다이어그램으로 직접 설명하는 것을 권장합니다.

**핵심 질문**: "이 피처를 계산할 때 어느 시점까지의 데이터를 써야 하는가?"

학습 데이터를 **여러 시점의 샘플**로 구성할 때 문제가 생깁니다.

예: 고객 이탈 예측 모델을 위해 서로 다른 시점의 샘플을 모읍니다:

```
학습 데이터 샘플:
  고객 A → 2023-03-01 기준 이탈 여부 (레이블)
  고객 B → 2023-08-15 기준 이탈 여부 (레이블)
  고객 C → 2024-01-01 기준 이탈 여부 (레이블)
```

```
❌ 잘못된 방식 — 전체 기간 집계로 피처 생성 (Data Leakage):

  피처 테이블: SELECT AVG(amount) FROM orders GROUP BY customer_id
              → "전체 기간" 구매액 평균 (기준 시점 없음)

  고객 A의 피처: 2023-03-01 이후 구매 기록까지 포함된 평균
                                 ↑
                        미래 데이터가 피처에 섞임!

  결과: 모델이 "미래에 많이 살 고객"의 패턴을 미리 학습
        → 실제 배포 시 그 정보가 없어 성능 급락
```

```
✅ 올바른 방식 — 포인트-인-타임:

  고객 A의 피처: 2023-03-01 이전 구매 기록만으로 계산
  고객 B의 피처: 2023-08-15 이전 구매 기록만으로 계산
  고객 C의 피처: 2024-01-01 이전 구매 기록만으로 계산
                ↑
        각 샘플마다 "기준 시점"을 지정하여 그 이전 데이터만 사용

  결과: 실제 예측 환경과 동일한 조건 → 배포 후 성능 유지
```

Feature Store의 `generate_training_set()`은 spine에 명시된 타임스탬프를 기준으로 **자동으로** 각 샘플에 맞는 피처를 조회합니다:

```python
# spine: (고객 키, 기준 시점) 쌍
spine_df = session.create_dataframe([
    (고객A, datetime(2023, 3, 1)),   # 이 시점 이전 데이터로만 피처 계산
    (고객B, datetime(2023, 8, 15)),  # 이 시점 이전 데이터로만 피처 계산
    (고객C, datetime(2024, 1, 1)),   # 이 시점 이전 데이터로만 피처 계산
], schema=["C_CUSTKEY", "EVENT_TIMESTAMP"])

# generate_training_set()이 각 시점에 맞는 피처를 자동 조회
training_data = fs.generate_training_set(
    spine_df=spine_df,
    features=[fv],
    spine_timestamp_col="EVENT_TIMESTAMP",  # 이 컬럼 기준으로 시점 필터 자동 적용
    label_col="FUTURE_12M_REVENUE"
)
```

> **우리 데모에서는?** 모든 고객의 기준 시점이 `1997-06-30`으로 동일하므로 포인트-인-타임 오염 위험이 적습니다. 하지만 여러 기준 시점을 섞은 학습 데이터를 만들 때는 반드시 이 개념이 필요합니다.

#### 3-5. Snowflake Dataset으로 버전 관리

```python
# 불변 스냅샷으로 저장 → 모델 재현성 보장
dataset = fs.generate_dataset(
    name="customer_features_v1",
    spine_df=spine_df,
    features=[fv],
    version="2024_01",
    desc="2024년 1월 학습 데이터셋"
)

# 나중에도 동일 데이터로 재현 가능
ds = fs.get_dataset("customer_features_v1", "2024_01")
```

Dataset이 필요한 이유:
- **재현성**: 동일 데이터로 모델 재학습·비교 가능
- **감사(Audit)**: "어떤 데이터로 학습했는가?" 즉시 답변 가능
- **협업**: 팀원이 동일 데이터셋으로 다른 알고리즘 실험 가능

### 실습 포인트
- Feature View 등록 후 Snowsight에서 Dynamic Table로 생성된 것 확인
- `fs.list_feature_views()` 결과로 피처 검색 경험
- 포인트-인-타임 조인 결과와 단순 JOIN 결과 비교

---

## Module 4: 모델 학습, 실험 추적 및 Model Registry

**유형**: 실습  
**소요 시간**: 90분  
**노트북**: `04_model_training_and_registry.ipynb`

### 구성
- **Part 1**: 모델 학습 및 실험 추적 (XGBoost, Random Forest 비교)
- **Part 2**: Model Registry (등록, 버전 관리, Champion/Challenger, 거버넌스)

### 학습 목표
- Snowflake ML API로 다양한 알고리즘을 학습하는 방법 이해
- Experiment Tracking으로 여러 실험을 체계적으로 관리하는 방법 습득
- 최적 모델 선택 기준과 평가 지표(RMSE, MAE, R²) 이해
- Model Registry에 모델 등록 및 버전 관리 방법 습득
- Champion/Challenger 패턴으로 안전한 모델 교체 방법 이해

### 전달 내용

#### 4-1. Snowflake ML 모델링 API

sklearn 호환 API이지만 Snowflake Warehouse에서 실행되는 핵심 차이를 강조합니다.

```python
from snowflake.ml.modeling.xgboost import XGBRegressor
from snowflake.ml.modeling.ensemble import RandomForestRegressor
from snowflake.ml.modeling.pipeline import Pipeline

model = XGBRegressor(
    input_cols=FEATURE_COLS,
    label_cols=["FUTURE_12M_REVENUE"],
    output_cols=["PREDICTED_LTV"],
    n_estimators=100,
    max_depth=6,
    learning_rate=0.1
)

model.fit(train_df)                # Warehouse에서 학습
predictions = model.predict(test_df)  # Warehouse에서 예측
```

**Snowflake ML Modeling vs. sklearn 비교**

| 항목 | sklearn | Snowflake ML |
|------|---------|--------------|
| 실행 위치 | 클라이언트 (로컬 메모리) | Snowflake Warehouse |
| 데이터 전송 | 전체 데이터 클라이언트로 | 불필요 |
| 확장성 | 메모리 한계 | Warehouse 자동 스케일 |
| fit 입력 | `fit(X_array, y_array)` | `fit(snowpark_dataframe)` |

#### 4-2. Experiment Tracking 설정

실험 추적이 왜 필요한지 실제 문제 상황으로 설명합니다.

실험 추적 없을 때:
- "어제 실행한 모델의 파라미터가 뭐였지?"
- "RMSE 80,000이 나온 피처 조합이 뭐였지?"
- "두 달 전 모델이 지금보다 나았던 것 같은데..."

```python
from snowflake.ml.experiment import Experiment

exp = Experiment(
    session=session,
    database="DEMO",
    schema="ML_DEMO",
    experiment_name="customer_value_prediction"
)

with exp.start_run("xgboost_depth6") as run:
    # 파라미터 로깅
    run.log_parameter("max_depth", 6)
    run.log_parameter("n_estimators", 100)
    run.log_parameter("learning_rate", 0.1)

    # 학습
    model.fit(train_df)
    predictions = model.predict(test_df)

    # 메트릭 로깅
    run.log_metric("rmse", float(np.sqrt(mean_squared_error(y_true, y_pred))))
    run.log_metric("mae",  float(mean_absolute_error(y_true, y_pred)))
    run.log_metric("r2_score",  r2_score(y_true, y_pred))
```

#### 4-3. 세 가지 모델 비교 실험

| 실험 이름 | 알고리즘 | 주요 파라미터 | 특징 |
|-----------|----------|--------------|------|
| `xgboost_baseline` | XGBRegressor | max_depth=6, lr=0.1 | 빠른 수렴, 결측값 처리 강점 |
| `xgboost_tuned` | XGBRegressor | max_depth=8, lr=0.05 | 더 깊은 트리, 과적합 주의 |
| `random_forest` | RandomForestRegressor | n_estimators=200 | 앙상블, 과적합 저항성 |

**평가 지표 설명**

| 지표 | 설명 | 언제 중요한가 |
|------|------|--------------|
| RMSE | 예측 오차 크기 (USD) | 회귀 모델 기본 메트릭, 큰 오차에 민감 |
| MAE | 평균 절대 오차 (USD) | 이상치에 덜 민감한 평가 |
| R² | 설명력 (0~1) | 모델이 데이터 변동성을 얼마나 설명하는지 |

#### 4-4. Snowsight에서 실험 결과 비교

Snowsight UI 탐색 경로:
1. Snowsight 좌측 메뉴 → **AI & ML → Experiments**
2. 실험 이름 클릭 → 모든 Run 목록 확인
3. 여러 Run 선택 → **Compare** 버튼 클릭
4. 메트릭 차트, 파라미터 테이블에서 직접 비교

#### 4-5. 피처 중요도 분석

```python
# XGBoost 피처 중요도 추출 및 시각화
importances = dict(zip(
    FEATURE_COLS,
    model._sklearn_object.feature_importances_
))

import plotly.express as px
df_imp = pd.DataFrame(importances.items(), columns=["feature", "importance"])
df_imp = df_imp.sort_values("importance", ascending=True)

px.bar(df_imp, x="importance", y="feature", orientation="h",
       title="피처 중요도 Top 10").show()
```

피처 중요도 해석 시 논의 포인트:
- 높은 중요도 피처: 비즈니스 상식과 일치하는가?
- 낮은 중요도 피처: 제거해도 성능에 큰 영향 없음 → 모델 단순화
- 예상치 못한 중요 피처 발견 → 도메인 전문가와 검증 필요

### 실습 포인트
- 3개 실험 실행 후 Snowsight Experiment UI에서 결과 비교
- 직접 파라미터를 바꿔 추가 실험 수행 (hyperparameter 탐색)
- 최적 모델 선택 기준을 RMSE, R² 관점에서 토론
- `target_platforms=["WAREHOUSE", "SNOWPARK_CONTAINER_SERVICES"]`로 모델 등록
- V1, V2 두 버전 등록 후 Champion/Challenger 시나리오 롤플레이

---

## Module 5: Inference (추론)

**유형**: 실습  
**소요 시간**: 60분  
**노트북**: `05_inference.ipynb`

### 구성
- **Part 1**: 배치 추론 (mv.run, Dynamic Table, run_batch)
- **Part 2**: 실시간 추론 (SPCS Inference Service)

### 학습 목표
- Batch Inference와 Real-time Inference의 차이 및 적합한 사용 사례 이해
- `mv.run()`, Dynamic Table, `mv.run_batch()`, `mv.create_service()` 방법 습득
- 비용 효율적인 추론 방식 선택 기준 이해

### 전달 내용

#### 5-1. 추론 방식 선택 가이드

```
질문 1: 언제 예측이 필요한가?
    ├── 특정 이벤트 즉시 (고객 가입, 결제 순간)  → Real-time (SPCS)
    ├── 일정 주기 (일별, 시간별)               → Batch 또는 Dynamic Table
    └── 새 데이터가 들어올 때마다 자동으로       → Dynamic Table

질문 2: 얼마나 많은 데이터를 처리하는가?
    ├── 수백만 건 이상    → run_batch() (SPCS 병렬 처리)
    ├── 수천~수십만 건    → mv.run() (Warehouse)
    └── 1~수십 건        → Real-time Service

질문 3: 허용 응답 시간은?
    ├── 밀리초 단위       → Real-time Service (SPCS)
    ├── 초~분 단위        → Warehouse Batch
    └── 시간 단위         → Scheduled Batch
```

| 방식 | API | 응답시간 | 비용 구조 | 적합 사례 |
|------|-----|----------|-----------|-----------|
| SQL Native Batch | `mv.run()` | 초~분 | Warehouse 크레딧 (사용 시만) | 일별 전체 스코어링 |
| Dynamic Table | 자동 갱신 | 분~시간 | Warehouse 크레딧 | 지속적 파이프라인 |
| Large-scale Batch | `mv.run_batch()` | 분~시간 | Compute Pool | 수백만 건 처리 |
| Real-time (SPCS) | `mv.create_service()` | 밀리초 | Compute Pool 상시 | 이벤트 기반 즉시 응답 |

#### 5-2. SQL Native Batch Inference — `mv.run()`

```python
reg = Registry(session=session, database_name="DEMO", schema_name="ML_DEMO")
mv = reg.get_model("CUSTOMER_LTV_PREDICTOR").version("V1")

# 전체 고객 배치 추론 (Warehouse에서 실행)
predictions = mv.run(customer_df, function_name="predict")

# 결과 저장
predictions.write.mode("overwrite").save_as_table("DEMO.ML_DEMO.CUSTOMER_LTV_SCORES")
```

`function_name` 옵션:
- `"predict"`: 예측값 반환 (LTV 금액)
- `"explain"`: SHAP 기반 피처 기여도 (Module 7에서 자세히)

#### 5-3. Dynamic Table 기반 자동 갱신 추론

```sql
-- 새 고객이 추가될 때마다 자동으로 예측 결과 갱신
CREATE OR REPLACE DYNAMIC TABLE DEMO.ML_DEMO.LIVE_PREDICTED_LTVS
  TARGET_LAG = '1 minute'
  WAREHOUSE = COMPUTE_WH
AS
  SELECT
    cf.C_CUSTKEY,
    cf.C_MKTSEGMENT,
    cf.TOTAL_ORDERS,
    cf.AVG_ORDER_VALUE,
    cf.TOTAL_REVENUE,
    MODEL(DEMO.ML_DEMO.CUSTOMER_LTV_PREDICTOR)!PREDICT(
        cf.C_MKTSEGMENT, cf.C_ACCTBAL, cf.TOTAL_ORDERS,
        cf.AVG_ORDER_VALUE, cf.TOTAL_REVENUE, cf.AVG_DISCOUNT,
        cf.AVG_QUANTITY, cf.DAYS_SINCE_LAST_ORDER, cf.C_NATIONKEY
    ):PREDICTED_LABEL::FLOAT AS PREDICTED_LTV,
    CURRENT_TIMESTAMP() AS SCORED_AT
  FROM DEMO.ML_DEMO.CUSTOMER_FEATURES cf;
```

적합 사례:
- BI 대시보드: 항상 최신 예측 결과 표시
- 마케팅 캠페인: 매일 갱신되는 타겟 고객 리스트
- 리스크 관리: 거래 데이터 갱신 시 리스크 스코어 자동 갱신

#### 5-4. SPCS Real-time Inference 배포

배포 전 수강생에게 반드시 설명할 사항: 배포에 5~10분 소요, 그동안 개념 설명 진행.

```python
mv.create_service(
    service_name="LTV_INFERENCE_SVC",
    service_compute_pool="SYSTEM_COMPUTE_POOL_CPU",
    ingress_enabled=True,
    gpu_requests=None,
)
```

배포 후 내부에서 일어나는 일:
1. 모델 컨테이너 이미지 자동 빌드
2. SPCS Compute Pool에 서비스 시작
3. HTTPS REST 엔드포인트 생성
4. Snowflake 인증 자동 통합 (별도 API 키 불필요)

```python
# 응답 시간 측정
import time
start = time.time()
result = mv.run(single_sample_df, function_name="predict",
                service_name="LTV_INFERENCE_SVC")
print(f"응답 시간: {(time.time()-start)*1000:.1f}ms")  # ~50ms
```

#### 5-5. 비용 관리

```python
# 사용 후 서비스 삭제 (Compute Pool 크레딧 중단)
mv.delete_service("LTV_INFERENCE_SVC")

# Compute Pool 일시 중지
session.sql("ALTER COMPUTE POOL SYSTEM_COMPUTE_POOL_CPU SUSPEND").collect()
```

비용 가이드:
- 개발·테스트: Batch Inference (`mv.run()`) — 항상 켜두는 비용 없음
- 프로덕션 실시간: `auto_suspend_secs=300` (5분 유휴 시 자동 절전)
- 대규모 배치: `run_batch()` — 작업 시간만큼만 Compute Pool 비용

### 실습 포인트
- `mv.run()` 결과를 QUERY_HISTORY에서 확인 (생성된 SQL 확인)
- Batch vs Real-time 응답 시간 직접 측정·비교
- Dynamic Table 생성 후 Snowsight에서 갱신 상태 확인

---

## Module 6: ML Jobs & Pipeline Orchestration

**유형**: 실습  
**소요 시간**: 45분  
**노트북**: `06_pipeline_orchestration.ipynb`

### 학습 목표
- ML Jobs를 통해 로컬 코드를 Snowflake 컴퓨팅에서 실행하는 방법 이해
- Task Graph로 전체 ML 파이프라인을 자동화하는 방법 습득
- 개발 환경(Notebooks)에서 운영 환경(Task Graph)으로의 전환 방법 이해

### 전달 내용

#### 7-1. 개발에서 운영으로의 전환 문제

ML 프로젝트에서 흔히 발생하는 상황:

```
[개발 단계]  데이터 과학자가 Jupyter Notebook에서 모델 개발
             → "노트북에서는 잘 되는데..."

[운영 단계]  ML 엔지니어가 자동화 시도
             → "이걸 매일 자동으로 실행하려면?"
             → "노트북을 스케줄러에 연결하려면?"
             → "실패하면 어떻게 알림을 받지?"
```

Snowflake ML의 해결 경로:

```
Notebook (개발)  →  ML Jobs (Snowflake 컴퓨팅)  →  Task Graph (자동화)
     ↓                      ↓                             ↓
인터랙티브 개발        GPU·대용량 처리               스케줄링 + 모니터링
```

#### 7-2. ML Jobs — `@remote` 데코레이터

로컬 함수를 Snowflake Compute Pool에서 실행하는 방법:

```python
from snowflake.ml.jobs import remote

@remote(
    session=session,
    compute_pool="SYSTEM_COMPUTE_POOL_CPU",
    stage_name="@DEMO.ML_DEMO.ML_STAGE",
    num_instances=1
)
def train_customer_model(config: dict) -> dict:
    # 이 코드 블록은 Snowflake Compute Pool에서 실행됨
    from snowflake.snowpark.context import get_active_session
    from snowflake.ml.modeling.xgboost import XGBRegressor
    from snowflake.ml.registry import Registry

    session = get_active_session()
    # ... 학습 로직 ...
    return {"rmse": rmse_val, "model_version": version_name}

# 로컬에서 호출 → Snowflake에서 실행됨
job = train_customer_model({"max_depth": 6, "n_estimators": 100})
print(f"Job ID: {job.id}, Status: {job.status}")
job.wait()          # 완료 대기
result = job.result()
print(f"학습 완료, RMSE: {result['rmse']:,.0f} (USD)")
```

ML Jobs 활용 시나리오:
- 외부 IDE(VS Code, PyCharm)에서 Snowflake GPU 서버 활용
- 로컬 메모리 부족으로 Notebook에서 처리 불가한 대용량 데이터
- 멀티-노드 분산 학습 필요 시 (`num_instances > 1`)

#### 7-3. Task Graph (DAG) — 파이프라인 자동화

> **`@remote` vs Task Graph 관계**: `@remote`는 개발/수동 실행용이고, Task Graph는 자동화(스케줄링)용입니다.
> Task Graph의 각 Task는 SQL만 실행할 수 있으므로, 학습 로직은 **Stored Procedure로 래핑**하여 `CALL`로 호출합니다.

```python
from snowflake.core.task.dagv1 import DAG, DAGTask, DAGOperation
from snowflake.core._common import CreateMode
from datetime import timedelta

with DAG(
    name="ML_TRAINING_DAG",
    schedule=timedelta(days=1),    # 매일 자동 실행
    warehouse="COMPUTE_WH"
) as dag:

    task_prep = DAGTask(
        "PREPARE_FEATURES",
        definition="CALL DEMO.ML_DEMO.PREPARE_FEATURES()"
    )
    task_train = DAGTask(
        "TRAIN_MODEL",
        definition="CALL DEMO.ML_DEMO.TRAIN_MODEL_PROC()"
    )
    task_score = DAGTask(
        "SCORE_CUSTOMERS",
        definition="CALL DEMO.ML_DEMO.SCORE_CUSTOMERS()"
    )

    # 순서 의존성 정의
    task_prep >> task_train >> task_score

# DAG 배포 (DAGOperation 사용)
schema = root.databases["DEMO"].schemas["ML_DEMO"]
DAGOperation(schema).deploy(dag, mode=CreateMode.or_replace)
```

```
DAG 실행 흐름:

PREPARE_FEATURES   (피처 테이블 최신화)
        ↓
TRAIN_MODEL        (LTV 회귀 모델 재학습)
        ↓
SCORE_CUSTOMERS    (전체 고객 스코어링)
```

**프로덕션 권장 패턴**: Compute Pool 활용이 필요한 학습의 경우:
- `submit_file()`: Python 스크립트를 Compute Pool에서 실행하는 SP 생성
- `@remote` + SP wrapper: `@remote` 함수를 SP 내부에서 호출하여 Task Graph에서 실행

#### 7-4. Snowsight에서 DAG 모니터링

Snowsight 탐색 경로: **Monitoring → Task History**

확인 가능한 정보:
- 각 Task의 실행 시간, 성공/실패 상태
- 실패 Task 클릭 → 오류 로그 확인
- DAG 시각화: 의존성 구조 그래프

#### 7-5. DEV / PROD 환경 분리

```python
import os

ENV = os.getenv("SNOWFLAKE_ENV", "dev")

CONFIG = {
    "dev":  {"database": "DEMO_DEV",  "schedule": None},           # 수동 실행
    "prod": {"database": "DEMO",      "schedule": timedelta(days=1)} # 자동 실행
}
```

CI/CD 연동 개념:
```
코드 변경 (GitHub PR)
    → 단위 테스트 실행 (DEV 환경)
    → 검증 통과 → Merge
    → PROD 환경 자동 배포
```

### 실습 포인트
- `@remote` Job 제출 후 `job.status`, `job.get_logs()` 확인
- Task Graph 배포: `DAGOperation(schema).deploy(dag, mode=CreateMode.or_replace)`
- Task Graph 배포 후 수동 트리거 실행
- Snowsight Task History에서 실행 결과 확인

---

## Module 7: ML Observability

**유형**: 실습  
**소요 시간**: 30분  
**노트북**: `07_ml_observability.ipynb`

### 학습 목표
- 프로덕션 모델이 시간이 지남에 따라 성능이 저하되는 원인 이해
- Model Monitor 설정 및 드리프트 감지 방법 습득
- SHAP 기반 모델 설명 가능성(Explainability) 실습

### 전달 내용

#### 8-1. 왜 모델을 모니터링해야 하는가?

현실에서 발생하는 모델 성능 저하 원인:

**데이터 드리프트**: 입력 데이터 분포 변화
```
학습 시: 고객 평균 주문 금액 $186,000
시간 경과 후: 인플레이션 영향으로 $220,000 수준으로 증가
→ 모델이 "고가치" 기준을 과소평가하기 시작
```

**컨셉 드리프트**: 타겟 변수의 의미 자체 변화
```
학습 시: 연간 구매 5회 이상 = 충성 고객
2년 후: 온라인 쇼핑 일상화로 5회가 평균
→ 기존 "고가치" 기준이 더 이상 유효하지 않음
```

**모니터링이 없을 때의 비용**:
```
배포 직후:  RMSE 85,000 (USD), R² 0.78
3개월 후:  RMSE 130,000 (USD), R² 0.55  ← 비즈니스에서 인지 못함
시간 경과 후:  RMSE 증가, R² 하락  ← 비즈니스 피해 발생 후 발견
```

#### 8-2. Model Monitor 설정 (SQL DDL)

Snowflake Model Monitor는 SQL DDL로 생성하며, 모델의 예측 성능과 데이터 드리프트를 자동으로 추적합니다.

```sql
CREATE MODEL MONITOR DEMO.ML_DEMO.LTV_MODEL_MONITOR WITH
    MODEL = DEMO.ML_DEMO.CUSTOMER_LTV_PREDICTOR
    VERSION = 'V1'
    FUNCTION = 'PREDICT'
    SOURCE = DEMO.ML_DEMO.INFERENCE_LOG
    WAREHOUSE = COMPUTE_WH
    REFRESH_INTERVAL = '1 day'
    AGGREGATION_WINDOW = '1 day'
    TIMESTAMP_COLUMN = PREDICTED_LTV_TIME
    ID_COLUMNS = ('PREDICTED_LTV_ID')
    PREDICTION_SCORE_COLUMNS = ('PREDICTED_LTV')
    ACTUAL_SCORE_COLUMNS = ('ACTUAL_LABEL');
```

주요 파라미터:
| 파라미터 | 설명 |
|---------|------|
| `MODEL` | 모니터링 대상 모델 (Model Registry에 등록된 모델) |
| `SOURCE` | 추론 로그 테이블 (예측값 + 실제값 저장) |
| `REFRESH_INTERVAL` | 지표 계산 주기 |
| `PREDICTION_SCORE_COLUMNS` | 예측값 컬럼 |
| `ACTUAL_SCORE_COLUMNS` | 실제값 컬럼 (지연 레이블) |

모니터링 결과 조회:
```sql
-- 성능 지표 조회
SELECT * FROM TABLE(MODEL_MONITOR_PERFORMANCE_METRIC(
    'DEMO.ML_DEMO.LTV_MODEL_MONITOR',
    METRIC_NAME => 'mean_absolute_error'
));

-- 드리프트 지표 조회
SELECT * FROM TABLE(MODEL_MONITOR_DRIFT_METRIC(
    'DEMO.ML_DEMO.LTV_MODEL_MONITOR',
    METRIC_NAME => 'psi'
));

-- 기본 통계 조회
SELECT * FROM TABLE(MODEL_MONITOR_STAT_METRIC(
    'DEMO.ML_DEMO.LTV_MODEL_MONITOR',
    METRIC_NAME => 'mean'
));
```

모니터링 주기:
- 자동: `REFRESH_INTERVAL`에 따라 Snowflake가 지표 계산
- 수동: `ALTER MODEL MONITOR ... SUSPEND / RESUME`
- Snowsight: **AI & ML → Model Monitor** 대시보드

#### 8-3. 데이터 드리프트 감지 — PSI

PSI(Population Stability Index) 직관적 설명:

```
PSI = 학습 시 데이터 분포와 현재 데이터 분포의 차이 수치화

해석 기준:
  PSI < 0.10  : 안정    → 현 모델 유지
  0.10 ~ 0.20 : 주의    → 원인 분석 필요
  PSI ≥ 0.20  : 위험    → 재학습 강력 권고
```

```python
# Model Monitor 드리프트 조회 (SQL 테이블 함수)
drift_df = session.sql("""
    SELECT * FROM TABLE(MODEL_MONITOR_DRIFT_METRIC(
        'DEMO.ML_DEMO.LTV_MODEL_MONITOR',
        METRIC_NAME => 'psi'
    ))
""").to_pandas()
high_drift = drift_df[drift_df["METRIC_VALUE"] >= 0.2]
print(f"드리프트 위험 피처: {high_drift['COLUMN_NAME'].tolist()}")

# Model Monitor 미사용 시 수동 PSI 계산 (폴백)
import numpy as np
def calculate_psi(expected, actual, bins=10):
    expected_pct = np.histogram(expected, bins=bins)[0] / len(expected)
    actual_pct = np.histogram(actual, bins=bins)[0] / len(actual)
    expected_pct = np.clip(expected_pct, 0.001, None)
    actual_pct = np.clip(actual_pct, 0.001, None)
    return np.sum((actual_pct - expected_pct) * np.log(actual_pct / expected_pct))
```

#### 8-4. ML Explainability — SHAP

```python
# 전체 데이터 SHAP 값 계산 (전역 설명)
shap_df = mv.run(test_df, function_name="explain")

# 특정 고객 예측 설명 (로컬 설명)
# "이 고객의 LTV가 $250,000로 예측된 이유:
#   AVG_ORDER_VALUE 기여도: +45,000  (가장 큰 요인)
#   TOTAL_ORDERS 기여도:    +12,000
#   C_ACCTBAL 기여도:       +8,000
#   C_MKTSEGMENT 기여도:    -3,000"
```

> **주의**: `explain()` 함수는 **단일 추정기(single estimator)** 모델에만 자동 등록됩니다.
> `Pipeline` (전처리 + 모델)으로 등록된 모델은 `explain()` 을 직접 호출할 수 없습니다.
> 이 경우 별도의 SHAP 라이브러리(`shap.TreeExplainer`)를 사용하여 설명을 생성합니다.

SHAP 활용 사례:
- **규제 대응**: "왜 이 고객에게 서비스가 거부됐나요?" → SHAP으로 근거 제시
- **모델 디버깅**: 예상치 못한 예측 원인 파악
- **피처 엔지니어링 힌트**: 중요한 새 피처 필요성 발견

#### 8-5. 재학습 트리거 조건 설계

```python
def should_retrain(session) -> bool:
    """Model Monitor 지표 기반 재학습 판단"""
    # 성능 지표 조회
    perf_df = session.sql("""
        SELECT * FROM TABLE(MODEL_MONITOR_PERFORMANCE_METRIC(
            'DEMO.ML_DEMO.LTV_MODEL_MONITOR',
            METRIC_NAME => 'mean_absolute_error'
        ))
    """).to_pandas()

    # 드리프트 지표 조회
    drift_df = session.sql("""
        SELECT * FROM TABLE(MODEL_MONITOR_DRIFT_METRIC(
            'DEMO.ML_DEMO.LTV_MODEL_MONITOR',
            METRIC_NAME => 'psi'
        ))
    """).to_pandas()

    conditions = {
        "mae_increase": perf_df["METRIC_VALUE"].iloc[-1] > 150000 if len(perf_df) > 0 else False,
        "high_drift": (drift_df["METRIC_VALUE"] >= 0.2).any() if len(drift_df) > 0 else False
    }
    triggered = [k for k, v in conditions.items() if v]
    if triggered:
        print(f"재학습 트리거: {triggered}")
        return True
    return False
```

### 실습 포인트
- `CREATE MODEL MONITOR` SQL DDL로 모니터 생성
- `MODEL_MONITOR_PERFORMANCE_METRIC()` / `MODEL_MONITOR_DRIFT_METRIC()` 테이블 함수로 지표 조회
- 드리프트 시뮬레이션: 분포가 다른 데이터를 주입 후 PSI 변화 확인
- SHAP 값으로 잘못 예측된 케이스 원인 분석 (Pipeline 모델은 `shap.TreeExplainer` 사용)
- Snowsight Model Monitor 대시보드 탐색

---

## Module 8: ML Lineage & Governance

**유형**: 이론 + 실습  
**소요 시간**: 30분

### 학습 목표
- ML Lineage가 왜 중요한지 이해
- `GET_LINEAGE()` 함수로 데이터~모델 흐름 추적 방법 습득
- Snowflake 거버넌스(RBAC, 태그, 마스킹)의 ML 적용 방법 이해

### 전달 내용

#### 9-1. ML Lineage가 필요한 상황

```
감사(Audit) 대응:
    "2024년 3월 모델이 사용한 학습 데이터는?"
    → Lineage 없음: 수 주에 걸친 수동 조사
    → Lineage 있음: 수 초 내 답변 가능

데이터 품질 이슈 영향 분석:
    "ORDERS 테이블에 오류 발견 → 영향받은 모델은?"
    → GET_LINEAGE()로 즉시 확인

모델 재현성:
    "이전 V1 모델을 동일하게 재현해달라"
    → 학습 데이터셋·피처 버전·코드 버전 모두 추적
```

#### 9-2. ML Lineage 계층 구조

```
Source Tables
    SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
    SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
         ↓
Feature Views
    DEMO.ML_DEMO.customer_features_fv
         ↓
Datasets
    DEMO.ML_DEMO.customer_features_v1/2024_01
         ↓
Models
    DEMO.ML_DEMO.CUSTOMER_LTV_PREDICTOR/V1
         ↓
Services
    DEMO.ML_DEMO.LTV_INFERENCE_SVC
```

```sql
-- 모델의 업스트림 계보 조회 (어떤 데이터로 학습했나?)
SELECT *
FROM TABLE(DEMO.ML_DEMO.GET_LINEAGE(
    'MODEL',
    'DEMO.ML_DEMO.CUSTOMER_LTV_PREDICTOR/V1',
    'UPSTREAM',
    TRUE   -- 재귀적으로 전체 계보
));

-- 특정 테이블의 다운스트림 영향 조회 (이 데이터를 쓰는 모델은?)
SELECT *
FROM TABLE(DEMO.ML_DEMO.GET_LINEAGE(
    'TABLE',
    'SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS',
    'DOWNSTREAM',
    TRUE
));
```

#### 9-3. Snowflake 거버넌스 통합

```sql
-- 민감 데이터 마스킹 정책 (학습 데이터에도 자동 적용)
CREATE MASKING POLICY mask_customer_name
    AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_SCIENTIST') THEN val
        ELSE '***MASKED***'
    END;

ALTER TABLE DEMO.ML_DEMO.CUSTOMER_FEATURES
    MODIFY COLUMN C_NAME SET MASKING POLICY mask_customer_name;

-- ML 모델에 메타데이터 태그 부착
ALTER MODEL DEMO.ML_DEMO.CUSTOMER_LTV_PREDICTOR
    SET TAG MODEL_TYPE = 'REGRESSION',
            BUSINESS_DOMAIN = 'CUSTOMER_ANALYTICS',
            DATA_CLASSIFICATION = 'INTERNAL';
```

---

## Module 9: Snowflake ML Functions

**유형**: 실습  
**소요 시간**: 30분

### 학습 목표
- SQL만으로 ML 기능을 사용할 수 있는 Cortex ML Functions 이해
- Python ML과 ML Functions의 차이 및 적합한 사용 사례 구분

### 전달 내용

#### 10-1. ML Functions vs. Python ML

| 항목 | Python ML (Module 4) | ML Functions (SQL) |
|------|---------------------|-------------------|
| 주 사용자 | 데이터 과학자 | 비즈니스 분석가, SQL 사용자 |
| 필요 지식 | Python + ML | SQL만 |
| 유연성 | 매우 높음 | 정해진 알고리즘 |
| 구현 시간 | 수 일~수 주 | 수 분 |
| 적합 사례 | 커스텀 복잡 모델 | 표준 패턴 (예측, 이상치 등) |

#### 10-2. FORECAST — 시계열 예측

```sql
-- 모델 학습
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST order_forecast(
    INPUT_DATA        => SYSTEM$REFERENCE('TABLE', 'DEMO.ML_DEMO.MONTHLY_ORDER_STATS'),
    SERIES_COLNAME    => 'SEGMENT',
    TIMESTAMP_COLNAME => 'ORDER_MONTH',
    TARGET_COLNAME    => 'TOTAL_REVENUE'
);

-- 향후 30일 예측
CALL order_forecast!FORECAST(FORECASTING_PERIODS => 30);
```

적합 사례: 재고 예측, 매출 예측, 수요 예측, 용량 계획

#### 10-3. ANOMALY_DETECTION — 이상치 감지

```sql
CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION order_anomaly(
    INPUT_DATA        => SYSTEM$REFERENCE('TABLE', 'DEMO.ML_DEMO.CUSTOMER_DAILY_ORDERS'),
    SERIES_COLNAME    => 'C_CUSTKEY',
    TIMESTAMP_COLNAME => 'ORDER_DATE',
    TARGET_COLNAME    => 'DAILY_ORDER_AMOUNT'
);

CALL order_anomaly!DETECT_ANOMALIES(
    INPUT_DATA        => SYSTEM$REFERENCE('TABLE', 'DEMO.ML_DEMO.CUSTOMER_DAILY_ORDERS'),
    SERIES_COLNAME    => 'C_CUSTKEY',
    TIMESTAMP_COLNAME => 'ORDER_DATE',
    TARGET_COLNAME    => 'DAILY_ORDER_AMOUNT'
);
```

적합 사례: 사기 거래 감지, 장비 이상 감지, KPI 이상 경보

#### 10-4. Cortex AI Functions

```sql
-- 감정 분석
SELECT C_CUSTKEY,
       SNOWFLAKE.CORTEX.SENTIMENT(C_COMMENT) AS SENTIMENT
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER LIMIT 10;

-- 텍스트 요약
SELECT SNOWFLAKE.CORTEX.SUMMARIZE(C_COMMENT) AS SUMMARY
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
WHERE LENGTH(C_COMMENT) > 100 LIMIT 5;

-- LLM 완성 (COMPLETE)
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-7b',
    CONCAT('다음 고객 코멘트에서 주요 불만을 한 문장으로 요약하세요: ', C_COMMENT)
) AS ANALYSIS
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER LIMIT 3;
```

### 실습 포인트
- Module 4 XGBoost 회귀 결과와 ML Functions FORECAST 결과 비교
- SQL 사용자가 Python 없이 예측을 수행하는 시나리오 직접 체험

---

## 교육 운영 가이드

### 사전 준비 체크리스트

**강사 준비**
- [ ] krdemo 계정(wf00579.ap-northeast-2.aws) ACCOUNTADMIN 권한 확인
- [ ] `DEMO.ML_DEMO.CUSTOMER_FEATURES` 테이블 생성 확인 (99,996건)
- [ ] `ML_EDUCATION_WS` 워크스페이스에 노트북 7개 업로드 확인
- [ ] `SYSTEM_COMPUTE_POOL_CPU` RESUME 상태 확인 (SPCS 데모용)
- [ ] `COMPUTE_WH` 웨어하우스 활성화 확인

**수강생 준비**
- [ ] Snowsight 접근 권한 확인
- [ ] Python 기본 문법 이해 (필수 아님, 권장)
- [ ] SQL 기본 지식 (권장)

### 모듈별 강사 팁

| 모듈 | 강사 팁 |
|------|---------|
| Module 1 | 수강생 ML 경험 수준 확인 후 설명 깊이 조정. AWS SageMaker 경험자가 있으면 대응 서비스 비교가 효과적 |
| Module 2 | `df.explain()`으로 Pushdown SQL을 직접 보여주면 "아, 정말 Snowflake에서 실행되는구나" 체감 |
| Module 3 | 포인트-인-타임 개념은 화이트보드 타임라인 그림으로 설명할 것. 가장 오해가 많은 부분 |
| Module 4 | 수강생이 직접 파라미터를 바꿔 4번째 실험을 추가하도록 유도. 실험 추적의 가치를 직접 경험 |
| Module 4 후반 | V1과 V2 배포 후 Champion/Challenger 역할극 진행 효과적 |
| Module 5 | SPCS 배포는 5~10분 소요. 배포 대기 중 Batch vs Real-time 비교 개념 설명으로 시간 활용 |
| Module 6 | Task Graph의 실제 DAG 시각화를 Snowsight에서 직접 보여주는 것이 핵심 |
| Module 7 | "모델을 배포하면 끝이 아니다"는 메시지를 비즈니스 피해 사례로 강조 |

### 자주 묻는 질문 (FAQ)

**Q. PyTorch, TensorFlow 같은 딥러닝도 Snowflake에서 할 수 있나요?**  
A. 네. Snowflake Notebooks Container Runtime에 PyTorch·TensorFlow가 기본 포함되어 있으며, GPU Compute Pool(`GPU_NV_S` 등)을 사용할 수 있습니다.

**Q. SageMaker나 로컬에서 학습한 모델도 Registry에 올릴 수 있나요?**  
A. 네. `reg.log_model()`은 외부에서 학습한 pickle, ONNX 등 다양한 포맷의 모델을 지원합니다. 외부 모델을 올리면 Snowflake 내 Batch 및 Real-time 추론이 모두 가능합니다.

**Q. Feature Store 온라인 서빙 지연시간이 Redis 수준인가요?**  
A. 아닙니다. Snowflake Feature Store의 온라인 서빙은 전용 온라인 스토어보다 지연시간이 높습니다. 수 ms 이하 응답이 필요한 경우 별도 캐싱 레이어를 고려해야 합니다.

**Q. SPCS 비용이 걱정됩니다. 효율적으로 사용하는 방법은?**  
A. 개발·테스트 단계에서는 항상 켜두는 비용이 없는 Batch Inference(`mv.run()`)를 사용하세요. 실시간 서비스 요구사항이 확인된 경우에만 SPCS를 배포하고, `auto_suspend_secs=300` (5분 유휴 시 자동 절전)을 설정하면 비용을 절감할 수 있습니다.

**Q. ML Functions와 Cortex AI Functions의 차이는?**  
A. ML Functions(FORECAST, ANOMALY_DETECTION, CLASSIFICATION)는 테이블 데이터에 대한 전통적 ML 태스크를 SQL로 처리합니다. Cortex AI Functions(COMPLETE, SUMMARIZE, SENTIMENT)는 대형 언어 모델(LLM) 기반으로 텍스트 처리에 특화되어 있습니다.

**Q. 이미 Airflow로 파이프라인을 운영 중인데 Task Graph가 필요한가요?**  
A. Snowflake ML은 Airflow, Dagster, Prefect 등 기존 오케스트레이터와 호환됩니다. 기존 워크플로우가 있다면 Airflow에서 ML Jobs를 호출하는 방식으로 통합하면 됩니다. 새로 구축한다면 Task Graph가 Snowflake 네이티브 옵션입니다.

---

## 총 교육 시간 요약

| 모듈 | 유형 | 시간 |
|------|------|------|
| Module 1: Snowflake ML 개요 | 이론 | 30분 |
| Module 2: 데이터 탐색 및 전처리 | 실습 | 45분 |
| Module 3: Feature Store | 실습 | 45분 |
| Module 4: 모델 학습 + 실험 추적 + Model Registry | 실습 | 90분 |
| Module 5: Inference (추론) | 실습 | 60분 |
| Module 6: ML Jobs & Pipeline Orchestration | 실습 | 45분 |
| Module 7: ML Observability | 실습 | 30분 |
| Module 8: ML Lineage & Governance | 이론+실습 | 30분 |
| Module 9: Snowflake ML Functions | 실습 | 30분 |

| **합계** | | **6시간 45분** |

> 반일 워크샵: Module 1~5 (4시간 30분)  
> 전일 워크샵: Module 1~9 (6시간 45분)

---

*작성: Cortex Code | krdemo 환경 기준 | 2026년 4월 26일*
