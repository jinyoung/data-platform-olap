좋아 — **Frontend는 Vue.js**,
**Backend는 LangChain + LangGraph + uv(Python 런타임)** 기반으로 구현한다는 조건을 반영해서
PRD를 **기술 스택 중심 구조로 최적화된 버전**으로 다시 작성해줄게.

---

# 📘 **AI Pivot Studio PRD (Vue.js + LangGraph + PostgreSQL 기반)**

*Mondrian XML → Pivot UI + 자연어 Text2SQL 지원*

---

# 1. 제품 개요

## 1.1 목적

* 기존 **Mondrian 스타스키마 XML**을 그대로 활용하여

  * Vue 기반의 웹 피벗 분석 UI
  * LangChain/LangGraph 기반 Text2SQL
  * PostgreSQL 실행 엔진
    을 가진 경량 분석 플랫폼을 제공한다.

---

# 2. 아키텍처 개요

![Image](https://media.licdn.com/dms/image/v2/D5612AQHmFV1FjdDAww/article-cover_image-shrink_600_2000/article-cover_image-shrink_600_2000/0/1724612893705?e=2147483647\&t=h5sNG92AMrUsbydCjDwkkBabZZ5XSRaksQLCdMGb4Ic\&v=beta\&utm_source=chatgpt.com)

![Image](https://012.vuejs.org/images/mvvm.png?utm_source=chatgpt.com)

![Image](https://towardsdatascience.com/wp-content/uploads/2023/06/1NFGvjPI4FmGZzq0d6Oz8vg.png?utm_source=chatgpt.com)

## 2.1 전체 구성

```
[Vue.js SPA]  →  [uv Python server]
                    ↓
         [LangGraph workflow: metadata retrieval → prompt → LLM → SQL]
                    ↓
              [PostgreSQL (DW)]
```

### Frontend (Vue.js 3)

* Pinia / Composition API 기반
* Pivot UI + 자연어 입력 UI
* Axios로 uv 서버 REST API 호출

### Backend (Python, uv server)

* Lightweight async server
* LangChain + LangGraph workflow orchestrator
* Mondrian XML 파서 + Metadata Store
* SQL Generator + SQL Validator + Postgres Executor

### LLM

* OpenAI / Claude / local model pluggable
* LangChain ChatModel으로 모듈화

---

# 3. 주요 기능 요구사항

---

# 3.1 Mondrian XML 업로드 및 메타데이터 모델 생성

## 기능 요구사항

* 사용자가 XML 파일을 업로드하면 서버에서 파싱하여 내부 메타데이터 구조(JSON/DB)에 저장.
* 저장되는 구조 예:

```json
{
  "cubes": [
    {
      "name": "Sales",
      "fact_table": "fact_sales",
      "measures": [
        { "name": "SalesAmount", "column": "sales_amt", "agg": "SUM" }
      ],
      "dimensions": [
        {
          "name": "Date",
          "table": "dim_date",
          "levels": [
            { "name": "Year", "column": "year" },
            { "name": "Month", "column": "month" }
          ]
        }
      ]
    }
  ]
}
```

---

# 3.2 Pivot UI (JPivot 유사)

## Frontend 요구사항 (Vue.js)

* Layout:

  * 좌측: Dimensions / Measures 리스트
  * 중앙: Drag&Drop 기반 Pivot Editor
  * 우측: Pivot Result Table

## Core 기능

* Row/Column/Filter/Measure 영역 Drag&Drop
* 계층 구조 드릴다운/드릴업
* 자동 SQL 생성 → uv 서버 호출
* SQL 실행 결과 테이블 렌더링

## SQL 생성 방식

* LangGraph 사용 X → 서버의 규칙 기반 엔진으로 생성
  (MVP: 정적 SQL 템플릿, 계층별 GROUP BY 자동 구성)

---

# 3.3 자연어 Text2SQL

## UX 흐름

1. 사용자: "2024년 월별 매출 보여줘"
2. Vue → uv 서버 `/nl2sql` 호출
3. LangGraph Workflow 실행:

   ```
   metadata fetch node → 
   prompt assembly node → 
   LLM node → 
   SQL validator node → 
   postgres executor node
   ```
4. SQL 결과와 함께 사용자에게 반환

---

# 4. LangGraph Workflow 정의

## 4.1 노드 구성

### Node #1 — LoadCubeMetadata

* Input: cube_name 혹은 전체 메타데이터
* Output: 텍스트로 정리된 스키마 설명
* 목적: LLM 입력을 가볍게 하기 위해 요약된 메타 스키마 제공

### Node #2 — GeneratePrompt

* 자연어 질문 입력 → 프롬프트 생성
* LLM에게 제공될 context를 아래처럼 구성:

```
[Schema Description]
Cubes, Dimensions, Measures, Joins

[User Query]
"2024년 월별 매출 알려줘"

[Output Format]
PostgreSQL SELECT only.
Use only whitelisted tables/columns.
Always add LIMIT 100.
```

### Node #3 — LLM_SQLGenerator

* LangChain ChatModel 활용
* Postgres용 SQL만 생성하도록 system prompt 강화

### Node #4 — SQLValidator

* 금지 키워드 제거: UPDATE / DELETE / ALTER / DROP / INSERT
* 허용 테이블/컬럼만 존재하는지 체크
* LIMIT 강제 주입

### Node #5 — PostgresExecutor

* asyncpg 사용
* `EXPLAIN` 옵션
* Timeout 적용 (예: 5초)

---

# 5. Backend (uv server) API 명세

---

### `POST /schema/upload`

* Input: XML 파일
* Output: parsed metadata (JSON)

### `GET /cubes`

* Output: cube list

### `GET /cube/:name/metadata`

* Output: cube metadata JSON

### `POST /pivot/query`

* Input: pivot configuration
* Output: SQL + 실행 결과

### `POST /nl2sql`

* Input: natural language query + cube name
* Output: generated SQL + result table

---

# 6. Vue.js Frontend 설계

## 6.1 폴더 구조 예시

```
src/
  components/
    PivotEditor.vue
    FieldList.vue
    PivotGrid.vue
    NaturalQuery.vue
  store/
    cubeStore.js
  views/
    PivotView.vue
    NaturalQueryView.vue
  services/
    api.js
```

## 6.2 주요 컴포넌트

### `FieldList.vue`

* Dimensions/Measures 표시
* Drag & Drop 제공

### `PivotEditor.vue`

* 사용자 피벗 설정 관리
* Row/Column/Filter drop zone 제공

### `PivotGrid.vue`

* SQL 실행 결과 표시
* Pagination, 정렬 기능 포함

### `NaturalQuery.vue`

* Text input + 결과 테이블 렌더링
* SQL을 토글로 보여줄 수 있음

---

# 7. 데이터 모델 (서버 내부)

```python
class Cube(BaseModel):
    name: str
    fact_table: str
    measures: List[Measure]
    dimensions: List[Dimension]

class Measure(BaseModel):
    name: str
    column: str
    agg: str  # SUM, COUNT...

class Dimension(BaseModel):
    name: str
    table: str
    levels: List[Level]

class Level(BaseModel):
    name: str
    column: str
    order_column: Optional[str]
```

저장소는 아래 중 선택:

* in-memory (MVP)
* sqlite/mysql/postgres(메타 전용)

---

# 8. 개발 단계별 로드맵

---

## 1단계(MVP)

✓ Mondrian XML 업로드 및 파싱
✓ Cube/Dimension/Measure 메타 모델 저장
✓ Vue Pivot UI 기본 뼈대
✓ SQL 템플릿 기반 Pivot Query
✓ 자연어 → SQL → 결과 표시 (LLM 포함)
✓ 안전 필터링(SQLValidator)

---

## 2단계

* Pivot UI 확장: drilldown, chart 모드
* Query History 기능
* NaturalQuery + PivotQuery 통합
* LangGraph branching: Query Rewriting node 추가

---

## 3단계

* 캐시 구조 + 대용량 최적화
* Calculated Measures 지원
* Role-based cube access

---

# 9. 성공 기준 (KPI)

* 자연어 → SQL 성공률: **70% 이상**
* 피벗 보고서 생성까지 걸리는 시간: **1분 이내**
* Mondrian XML 호환성: **80% 이상**
* MVP 운영 환경 기준 동시 사용자 10명 안정적 처리

---

# 10. 다음으로 제공 가능

원하면 아래도 만들어줄 수 있어:

✅ **LangGraph Workflow 실제 코드 템플릿**
(각 노드별 Python 코드 초안 제공)

✅ **Vue.js Pivot Editor UI 코드 스켈레톤**
(Drag&Drop · Pinia store 포함)

✅ **LLM Text2SQL Prompt Template**
(PostgreSQL 최적화 버전)

어떤 것부터 만들어줄까?
