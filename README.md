# Snowflake End-to-End ML Workshop

**데모 시나리오**: TPC-H 고객 생애 가치 예측 (LTV Prediction)  
**대상 환경**: krdemo (wf00579.ap-northeast-2.aws)  
**총 교육 시간**: 약 6시간  
**그래프 언어**: 영문 (한글 폰트 의존성 없음)

---

## 교육 개요

TPC-H 데이터베이스의 고객·주문 데이터를 활용해 **"향후 12개월 구매금액(FUTURE_12M_REVENUE)"** 을 예측하는 회귀 모델을 구축합니다. 데이터 준비부터 모델 운영까지 Snowflake ML 전체 라이프사이클을 단일 플랫폼에서 경험합니다.

### 사용 데이터

| 테이블 | 위치 | 설명 |
|--------|------|------|
| CUSTOMER | SNOWFLAKE_SAMPLE_DATA.TPCH_SF1 | 고객 정보 (150,000건) |
| ORDERS | SNOWFLAKE_SAMPLE_DATA.TPCH_SF1 | 주문 내역 (1,500,000건) |
| LINEITEM | SNOWFLAKE_SAMPLE_DATA.TPCH_SF1 | 주문 상세 (6,000,000건) |
| CUSTOMER_FEATURES | DEMO.ML_DEMO | 전처리된 피처 테이블 (99,996건) |

### 시간 분리 전략

과거 데이터(1992\~1997.06)로 피처를 생성하고, 예측 윈도우(1997.07\~1998.06) 12개월 총 구매금액을 타겟으로 사용합니다. 미래 정보가 피처에 유출되지 않도록 시간 기준 분리를 적용합니다.

---

## Module 1: Snowflake ML 개요

**유형**: 이론 | **시간**: 30분

### 핵심 컴포넌트

| 컴포넌트 | 역할 |
|----------|------|
| Notebooks (Container Runtime) | 브라우저 기반 Jupyter, CPU/GPU 학습 |
| Feature Store | 피처 정의·저장·재사용·실시간 서빙 |
| ML Jobs | 외부 IDE에서 Snowflake 컴퓨팅으로 코드 제출 |
| Experiment Tracking | 실험 메트릭·파라미터 로깅·비교 |
| Model Registry | 모델 버전 관리·거버넌스·배포 |
| Batch Inference | SQL 기반 대량 예측 (`mv.run()`) |
| SPCS Real-time | 컨테이너 기반 REST 추론 서비스 |
| ML Observability | 모델 드리프트·성능 모니터링 |
| ML Lineage | 데이터~모델 계보 추적 |
| Task Graph | ML 파이프라인 자동화·스케줄링 |

---

## Module 2: 데이터 탐색 및 전처리

**유형**: 실습 | **시간**: 45분 | **노트북**: `02_data_exploration.ipynb`

### 학습 목표
- Snowpark DataFrame API로 대규모 데이터를 Snowflake 내에서 처리
- ML 타겟 변수 설계 및 피처 엔지니어링

### 생성 피처

| 피처명 | 설명 |
|--------|------|
| C_ACCTBAL | 계정 잔액 |
| C_MKTSEGMENT | 시장 세그먼트 (5개) |
| TOTAL_ORDERS | 총 주문 횟수 |
| AVG_ORDER_VALUE | 평균 주문 금액 |
| TOTAL_REVENUE | 총 매출액 |
| AVG_DISCOUNT | 평균 할인율 |
| AVG_QUANTITY | 평균 주문 수량 |
| DAYS_SINCE_LAST_ORDER | 최근 주문 이후 경과일 |
| C_NATIONKEY | 국가 키 |

### 전처리 파이프라인

```python
from snowflake.ml.modeling.preprocessing import StandardScaler, OrdinalEncoder
from snowflake.ml.modeling.pipeline import Pipeline

pipeline = Pipeline(steps=[
    ("encoder", OrdinalEncoder(input_cols=CAT_COLS, output_cols=CAT_COLS_ENC)),
    ("scaler", StandardScaler(input_cols=NUM_COLS, output_cols=NUM_COLS_SCALED))
])
pipeline.fit(train_df)  # Warehouse에서 실행
```

---

## Module 3: Feature Store

**유형**: 실습 | **시간**: 45분 | **노트북**: `03_feature_store.ipynb`

### 학습 목표
- Feature Store의 필요성과 핵심 개념(Entity, Feature View) 이해
- 포인트-인-타임 정합성(Point-in-Time Correctness) 이해

### 핵심 개념

- **Entity**: 피처의 기준 비즈니스 객체 (예: Customer, C_CUSTKEY)
- **Feature View**: Entity에 대한 피처 집합 (Static / Managed)
- **포인트-인-타임**: 각 샘플의 기준 시점 이전 데이터만으로 피처 계산 → Data Leakage 방지

### Snowflake 객체 매핑

| Feature Store 개념 | 실제 Snowflake 객체 | 설명 |
|---|---|---|
| Feature Store | Schema | 피처를 담는 스키마 |
| Entity | Tag | 피처의 기준 키 정의 (예: C_CUSTKEY) |
| Managed Feature View | Dynamic Table | Snowflake가 `refresh_freq`에 따라 자동 갱신 |
| External Feature View | View | 기존 테이블을 참조만 (사용자가 직접 관리) |

### Managed vs External Feature View

| 구분 | Managed Feature View | External Feature View |
|---|---|---|
| 내부 구현 | Dynamic Table | View |
| 갱신 주체 | **Snowflake** (자동) | **사용자** (dbt, INSERT 등) |
| 추가 비용 | 컴퓨팅 비용 발생 | 없음 |
| 적합한 경우 | 실시간 피처 파이프라인 | 이미 가공된 테이블 등록 |
| 이 데모에서 | `CUSTOMER_ORDER_STATS` | `CUSTOMER_FEATURES_FV` |

```python
fs = FeatureStore(session=session, database="DEMO", name="ML_DEMO",
                  default_warehouse="COMPUTE_WH",
                  creation_mode=CreationMode.CREATE_IF_NOT_EXIST)

customer_entity = Entity(name="CUSTOMER", join_keys=["C_CUSTKEY"])
fs.register_entity(customer_entity)

fv = FeatureView(name="customer_order_stats", entities=[customer_entity],
                 feature_df=feature_df, refresh_freq="1 day")
fs.register_feature_view(feature_view=fv, version="1")
```

---

## Module 4: 모델 학습, Experiment Tracking 및 Model Registry

**유형**: 실습 | **시간**: 90분 | **노트북**: `04_model_training_and_registry.ipynb`

### 학습 목표
- Snowflake ML API로 다양한 알고리즘 학습 (XGBoost, Random Forest)
- Experiment Tracking으로 실험 체계적 관리
- Model Registry에 모델 등록 및 버전 관리 (Champion/Challenger)

### 실험 비교

| 실험 | 알고리즘 | 특징 |
|------|----------|------|
| xgboost_baseline | XGBRegressor (depth=6) | 빠른 수렴 |
| xgboost_tuned | XGBRegressor (depth=8, lr=0.05) | 더 깊은 트리 |
| random_forest | RandomForestRegressor (n=200) | 과적합 저항성 |

### 평가 지표: RMSE (오차 크기), MAE (평균 절대 오차), R² (설명력)

```python
from snowflake.ml.modeling.xgboost import XGBRegressor
from snowflake.ml.registry import Registry

model = XGBRegressor(input_cols=FEATURE_COLS, label_cols=["FUTURE_12M_REVENUE"],
                     output_cols=["PREDICTED_LTV"])
model.fit(train_df)

# Model Registry 등록
reg = Registry(session=session, database_name="DEMO", schema_name="ML_DEMO")
mv = reg.log_model(model, model_name="CUSTOMER_LTV_PREDICTOR",
                   version_name="V1", sample_input_data=train_df)
```

---

## Module 5: Inference (추론)

**유형**: 실습 | **시간**: 60분 | **노트북**: `05_inference.ipynb`

### 학습 목표
- Batch / Real-time 추론의 차이 및 선택 기준
- `mv.run()`, Dynamic Table, SPCS 서비스 방법 습득

### 추론 방식 선택

| 방식 | API | 응답시간 | 적합 사례 |
|------|-----|----------|-----------|
| SQL Batch | `mv.run()` | 초~분 | 일별 전체 스코어링 |
| Dynamic Table | 자동 갱신 | 분~시간 | 지속적 파이프라인 |
| Large-scale Batch | `mv.run_batch()` | 분~시간 | 수백만 건 처리 |
| Real-time (SPCS) | `mv.create_service()` | 밀리초 | 이벤트 기반 즉시 응답 |

> 노트북은 반복 실행을 고려하여, `create_service()` 전에 기존 서비스를 삭제하도록 구현되어 있습니다.

```python
mv = reg.get_model("CUSTOMER_LTV_PREDICTOR").version("V1")
predictions = mv.run(customer_df, function_name="predict")
```

---

## Module 6: ML Jobs & Pipeline Orchestration

**유형**: 실습 | **시간**: 45분 | **노트북**: `06_pipeline_orchestration.ipynb`

### 학습 목표
- ML Jobs(`@remote`)로 Snowflake Compute Pool에서 코드 실행
- Task Graph(DAG)로 ML 파이프라인 자동화
- 개발 환경 → 운영 환경 전환 방법

### ML Jobs — `@remote`

```python
from snowflake.ml.jobs import remote

@remote(session=session, compute_pool="SYSTEM_COMPUTE_POOL_CPU",
        stage_name="@DEMO.ML_DEMO.ML_STAGE")
def train_customer_model(config: dict) -> dict:
    # Snowflake Compute Pool에서 실행
    ...
    return {"rmse": rmse_val, "model_version": version_name}

job = train_customer_model({"max_depth": 6, "n_estimators": 100})
print(f"Status: {job.status}")
job.wait()
```

### Task Graph (DAG)

> `@remote`는 개발/수동 실행용, Task Graph는 자동화(스케줄링)용.
> Task는 SQL만 실행하므로 학습 로직은 **Stored Procedure로 래핑**하여 `CALL`로 호출.

```python
from snowflake.core.task.dagv1 import DAG, DAGTask, DAGOperation
from snowflake.core._common import CreateMode

with DAG(name="ML_TRAINING_DAG", schedule=timedelta(days=1),
         warehouse="COMPUTE_WH") as dag:
    task_prep  = DAGTask("PREPARE_FEATURES", definition="CALL DEMO.ML_DEMO.PREPARE_FEATURES()")
    task_train = DAGTask("TRAIN_MODEL", definition="CALL DEMO.ML_DEMO.TRAIN_MODEL_PROC()")
    task_score = DAGTask("SCORE_CUSTOMERS", definition="CALL DEMO.ML_DEMO.SCORE_CUSTOMERS()")
    task_prep >> task_train >> task_score

schema = root.databases["DEMO"].schemas["ML_DEMO"]
DAGOperation(schema).deploy(dag, mode=CreateMode.or_replace)
```

**프로덕션 권장 패턴**: `submit_file()` 또는 `@remote` + SP wrapper로 Compute Pool 활용

---

## Module 7: ML Observability

**유형**: 실습 | **시간**: 30분 | **노트북**: `07_ml_observability.ipynb`

### 학습 목표
- 프로덕션 모델 성능 저하 원인(데이터 드리프트, 컨셉 드리프트) 이해
- Model Monitor 설정 및 드리프트 감지
- Precision/F1 기반 재학습 트리거 로직

### 모니터링 지표

| 지표 | 용도 |
|------|------|
| Precision / Recall / F1 | 모델 정확도 추적 |
| PSI (Population Stability Index) | 피처 분포 변화 감지 |

### 재학습 트리거 기준

| 조건 | 임계값 |
|------|--------|
| Precision < 0.80 | 재학습 |
| F1 Score < 0.80 | 재학습 |
| HIGH DRIFT 피처 >= 2개 | 재학습 |
| 최대 PSI > 0.30 | 재학습 |

### Model Monitor (SQL DDL)

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
    PREDICTION_SCORE_COLUMNS = ('PREDICTED_LTV')
    ACTUAL_SCORE_COLUMNS = ('ACTUAL_LABEL');
```

### 모니터링 결과 조회

```sql
SELECT * FROM TABLE(MODEL_MONITOR_PERFORMANCE_METRIC('DEMO.ML_DEMO.LTV_MODEL_MONITOR', METRIC_NAME => 'mean_absolute_error'));
SELECT * FROM TABLE(MODEL_MONITOR_DRIFT_METRIC('DEMO.ML_DEMO.LTV_MODEL_MONITOR', METRIC_NAME => 'psi'));
```

### PSI 기준

| PSI | 상태 | 조치 |
|-----|------|------|
| < 0.10 | 안정 | 현 모델 유지 |
| 0.10 ~ 0.20 | 주의 | 원인 분석 필요 |
| >= 0.20 | 위험 | 재학습 강력 권고 |

---

## Snowsight UI 탐색 가이드

각 모듈 실습 후 Snowsight에서 결과를 직접 확인할 수 있습니다.

| 탐색 경로 | 확인 내용 | 관련 모듈 |
|-----------|-----------|-----------|
| **AI & ML → Features** | Feature View 목록, 엔티티별 정리, 컬럼 상세, Lineage | Module 3 |
| **AI & ML → Experiments** | 실험 Run 목록, 파라미터/메트릭 비교 차트 | Module 4 |
| **AI & ML → Models** | 등록된 모델 목록, 버전별 메트릭, 호출 코드 스니펫 | Module 4, 5 |
| **AI & ML → Models → (모델 선택) → Lineage** | 소스 테이블 → 피처 → 모델 데이터 흐름 | Module 4 |
| **AI & ML → Models → (모델 선택) → Inference Services** | SPCS 배포 상태, REST 엔드포인트 | Module 5 |
| **AI & ML → Models → (모델 선택) → Monitors** | 성능/드리프트 메트릭 대시보드, 버전 비교 | Module 7 |
| **Monitoring → Task History** | DAG 실행 이력, 성공/실패 상태, 의존성 그래프 | Module 6 |

---

## Snowflake ML 주요 기능 요약

### 데이터 & 피처

| 기능 | 설명 |
|------|------|
| **Snowpark DataFrame** | Python에서 SQL Pushdown으로 대규모 데이터 처리. 데이터가 Snowflake를 떠나지 않음 |
| **Feature Store** | 피처 중앙 관리. Entity/Feature View 기반으로 팀 간 재사용, Managed Feature View는 Dynamic Table로 자동 갱신 |
| **Datasets** | 학습 데이터의 불변 스냅샷 저장 → 모델 재현성 보장 |

### 모델 개발 & 학습

| 기능 | 설명 |
|------|------|
| **Notebooks (Container Runtime)** | 브라우저 기반 Jupyter 환경. CPU/GPU 선택 가능, 패키지 사전 설치됨 |
| **ML Modeling API** | sklearn 호환 API (`XGBRegressor`, `RandomForestRegressor` 등)이지만 Warehouse에서 실행 |
| **Experiment Tracking** | 파라미터·메트릭·아티팩트 로깅. Snowsight UI에서 실험 비교 |
| **ML Jobs (`@remote`)** | 로컬/Notebook 함수를 Compute Pool(CPU/GPU)에서 실행. 멀티노드 분산 학습 지원 |

### 모델 관리 & 배포

| 기능 | 설명 |
|------|------|
| **Model Registry** | 모델 버전 관리, Champion/Challenger 태그, RBAC 거버넌스 자동 적용 |
| **Batch Inference (`mv.run()`)** | Warehouse에서 SQL 기반 대량 예측. 별도 인프라 불필요 |
| **Dynamic Table Inference** | 원본 데이터 변경 시 예측 결과 자동 갱신 |
| **SPCS Real-time** | 모델을 컨테이너 서비스로 배포. REST 엔드포인트 자동 생성, 밀리초 응답 |

### 운영 & 자동화

| 기능 | 설명 |
|------|------|
| **Task Graph (DAG)** | Snowflake 네이티브 파이프라인 오케스트레이션. 스케줄링 + 의존성 관리 |
| **Model Monitor** | `CREATE MODEL MONITOR` SQL DDL로 생성. 성능/드리프트 지표 자동 계산 |
| **ML Lineage** | 소스 테이블 → 피처 → 모델 → 서비스까지 계보 자동 추적 (`GET_LINEAGE()`) |
| **Snowflake Alerts** | 임계값 기반 자동 알림 (이메일, Slack 등) |

---

## 자주 묻는 질문 (FAQ)

**Q. PyTorch, TensorFlow 같은 딥러닝도 Snowflake에서 할 수 있나요?**  
A. 네. Notebooks Container Runtime에 PyTorch·TensorFlow가 포함되어 있으며, GPU Compute Pool을 사용할 수 있습니다.

**Q. SageMaker나 로컬에서 학습한 모델도 Registry에 올릴 수 있나요?**  
A. 네. `reg.log_model()`은 pickle, ONNX 등 다양한 포맷을 지원합니다.

**Q. Feature Store 온라인 서빙 지연시간이 Redis 수준인가요?**  
A. 아닙니다. 수 ms 이하 응답이 필요한 경우 별도 캐싱 레이어를 고려해야 합니다.

**Q. SPCS 비용이 걱정됩니다.**  
A. 개발·테스트에는 `mv.run()` (항상 켜둘 필요 없음)을 사용하고, 프로덕션에서는 `auto_suspend_secs=300`을 설정하세요.

**Q. ML Functions와 Cortex AI Functions의 차이는?**  
A. ML Functions(FORECAST, ANOMALY_DETECTION)는 테이블 데이터 ML, Cortex AI Functions(COMPLETE, SUMMARIZE)는 LLM 기반 텍스트 처리입니다.

**Q. 이미 Airflow로 운영 중인데 Task Graph가 필요한가요?**  
A. 기존 Airflow에서 ML Jobs를 호출하는 방식으로 통합 가능합니다. 새로 구축한다면 Task Graph가 네이티브 옵션입니다.

---

## 노트북 실행 참고사항

- **반복 실행 가능**: 모든 노트북은 `overwrite=True` 또는 기존 객체 삭제 후 재생성 패턴으로 구현되어 있어 여러 번 실행해도 에러 없음
- **한글 폰트 불필요**: 그래프 텍스트는 모두 영문으로 작성. Container Runtime에 폰트 설치 불필요
- **Spine DataFrame 미사용**: 시간 분리를 코드에서 직접 처리 (WHERE 조건). Feature Store의 Point-in-Time(ASOF JOIN)은 이 데모에서 사용하지 않음

---

*krdemo 환경 기준 | 2026년 5월*
