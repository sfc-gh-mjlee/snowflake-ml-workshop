# Snowflake End-to-End ML Workshop

**데모 시나리오**: TPC-H 고객 생애 가치 예측 (LTV Prediction)  
**대상 환경**: krdemo (wf00579.ap-northeast-2.aws)  
**총 교육 시간**: 약 6시간  
**그래프 언어**: 영문 (한글 폰트 의존성 없음)

---

## 교육 개요

TPC-H 데이터베이스의 고객·주문 데이터를 활용해 **"향후 12개월 구매금액(FUTURE_12M_REVENUE)"** 을 예측하는 회귀 모델을 구축합니다. 데이터 준비부터 모델 운영까지 Snowflake ML 전체 라이프사이클을 단일 플랫폼에서 경험합니다.

### 아키텍처

```
TPC-H 원본 (ORDERS + LINEITEM + CUSTOMER)
    │
    ▼  [03: 일별 피처 스냅샷 생성]
CUSTOMER_FEATURES_DAILY (10,000 고객 × 180일)
    │
    ▼  [03: Feature Store — Managed FV + timestamp_col]
customer_ltv_features (Dynamic Table, Point-in-Time 지원)
    │
    ├── 학습 [03→04]: generate_dataset() → Dataset(불변) → 모델 학습 → Registry
    │
    └── 추론 [05]: Managed FV → LIVE_PREDICTED_LTVS (자동 재추론 DT 체인)
```

### 시간 분리 전략

과거 데이터(1992\~1997.06)로 피처를 생성하고, 예측 윈도우(1997.07\~1998.06) 12개월 총 구매금액을 타겟으로 사용합니다. Feature Store의 Point-in-Time JOIN(ASOF JOIN)으로 Data Leakage를 자동 방지합니다.

---

## Module 2: 데이터 탐색 (EDA)

**유형**: 실습 | **시간**: 30분 | **노트북**: `02_data_exploration.ipynb`

- TPC-H 테이블 구조 탐색 (CUSTOMER, ORDERS, LINEITEM)
- 피처 후보 및 타겟 변수 설계
- 시각화 (Market Segment, Order Amount 분포)

---

## Module 3: Feature Store

**유형**: 실습 | **시간**: 60분 | **노트북**: `03_feature_store.ipynb`

### 핵심 개념

| Feature Store 개념 | 실제 Snowflake 객체 | 설명 |
|---|---|---|
| Feature Store | Schema | 피처를 담는 스키마 |
| Entity | Tag | 피처의 기준 키 (C_CUSTKEY) |
| Managed Feature View | Dynamic Table | 자동 갱신, `timestamp_col`로 Point-in-Time 지원 |
| Dataset | Dataset 객체 | 불변 학습 데이터 스냅샷 |

### 이 모듈에서 수행하는 작업

1. **일별 피처 스냅샷 테이블 생성** (`CUSTOMER_FEATURES_DAILY`)
   - 10,000 고객 × 180일 = 180만 행
   - 각 날짜 기준 누적 피처 계산

2. **Managed Feature View 등록** (`customer_ltv_features@1`)
   - `timestamp_col="FEATURE_DATE"` → Point-in-Time 지원
   - `refresh_freq="1 hour"` → 자동 갱신

3. **학습 Dataset 생성** (`customer_ltv_training@v1`)
   - Spine: 10,000 고객 × `EVENT_TIMESTAMP='1997-06-30'`
   - Feature Store가 ASOF JOIN으로 해당 시점 이전 최신 피처 자동 매칭
   - 불변 저장 → 모델 재현성 보장

---

## Module 4: 모델 학습, Experiment Tracking 및 Model Registry

**유형**: 실습 | **시간**: 90분 | **노트북**: `04_model_training_and_registry.ipynb`

### 학습 목표
- 불변 Dataset에서 데이터를 읽어 모델 학습
- XGBoost vs Random Forest 성능 비교 (Experiment Tracking)
- Best 모델을 Model Registry에 Champion/Challenger로 등록

### 실험 비교

| 실험 | 알고리즘 | 특징 |
|------|----------|------|
| xgboost_baseline | XGBRegressor (depth=6) | 빠른 수렴 |
| xgboost_tuned | XGBRegressor (depth=4, lr=0.05) | 정규화 강화 |
| random_forest | RandomForestRegressor (n=150) | 과적합 저항성 |

### 평가 지표: RMSE, R²

```python
training_ds = dataset.load_dataset(session, 'customer_ltv_training', 'v1')
df = training_ds.read.to_snowpark_dataframe()
# → Point-in-Time이 적용된 불변 데이터로 학습
```

---

## Module 5: Inference (추론)

**유형**: 실습 | **시간**: 60분 | **노트북**: `05_inference.ipynb`

### 추론 방식

| 방식 | API | 응답시간 | 적합 사례 |
|------|-----|----------|-----------|
| SQL Batch | `mv.run()` | 초~분 | 일별 전체 스코어링 |
| Dynamic Table 체인 | Managed FV → 추론 DT | 분~시간 | 자동 갱신 파이프라인 |
| Large-scale Batch | `mv.run_batch()` | 분~시간 | 수백만 건 처리 |
| Real-time (SPCS) | `mv.create_service()` | 밀리초 | 이벤트 기반 즉시 응답 |

### 추론 DT 체인

```
CUSTOMER_LTV_FEATURES (Managed FV — 1시간마다 갱신)
    │
    ▼  MODEL!PREDICT()
LIVE_PREDICTED_LTVS (추론 DT — 피처 변경 시 자동 재추론)
```

---

## Module 6: ML Jobs & Pipeline Orchestration

**유형**: 실습 | **시간**: 45분 | **노트북**: `06_pipeline_orchestration.ipynb`

- `@remote`: Compute Pool에서 학습 코드 실행
- Task Graph (DAG): 파이프라인 자동화·스케줄링

---

## Module 7: ML Observability

**유형**: 실습 | **시간**: 30분 | **노트북**: `07_ml_observability.ipynb`

- Model Monitor로 성능/드리프트 감지
- PSI 기반 피처 드리프트 시뮬레이션
- Precision/F1 기반 재학습 트리거 로직

---

## Snowsight UI 탐색 가이드

| 탐색 경로 | 확인 내용 | 관련 모듈 |
|-----------|-----------|-----------|
| **AI & ML → Features** | Feature View, 엔티티, Lineage | Module 3 |
| **AI & ML → Experiments** | 실험 Run 비교, 메트릭 차트 | Module 4 |
| **AI & ML → Models** | 모델 버전, 메트릭, 호출 코드 | Module 4, 5 |
| **Monitoring → Task History** | DAG 실행 이력 | Module 6 |

---

## 자주 묻는 질문 (FAQ)

**Q. PyTorch, TensorFlow도 Snowflake에서 가능한가요?**  
A. 네. Container Runtime에 포함되어 있으며, GPU Compute Pool 사용 가능합니다.

**Q. 외부에서 학습한 모델도 Registry에 올릴 수 있나요?**  
A. 네. `reg.log_model()`은 pickle, ONNX 등 다양한 포맷을 지원합니다.

**Q. SPCS 비용이 걱정됩니다.**  
A. 개발 시에는 `mv.run()`을 사용하고, 프로덕션에서는 `auto_suspend_secs=300`을 설정하세요.

**Q. Feature Store의 Point-in-Time은 어떻게 작동하나요?**  
A. `timestamp_col`이 설정된 Managed FV에서 `generate_dataset(spine_timestamp_col=...)`을 호출하면, Spine의 EVENT_TIMESTAMP 이전 가장 최근 피처를 ASOF JOIN으로 자동 매칭합니다.

---

*krdemo 환경 기준 | 2026년 5월*
