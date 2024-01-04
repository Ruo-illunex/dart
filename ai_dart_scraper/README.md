
# ai_dart_scraper API 전체 명세서

작성자 : 이루오 ([ruo@illunex.com](mailto:ruo@illunex.com))

github repo : [https://github.com/Ruo-illunex/illunex-scrap-pkg](https://github.com/Ruo-illunex/illunex-scrap-pkg) > `ai_dart_scraper`

> ai_dart_scraper는 기업의 다양한 정보와 재무 정보를 수집하고, 처리하는 FastAPI 기반 웹 서비스입니다. 모든 API 엔드포인트는 유효한 JWT 토큰이 필요합니다.

## 인증

### **인증 필요성**

이 서비스는 중요한 기업 데이터를 다루며, 이러한 정보는 민감하고 중요한 데이터를 포함하고 있습니다. 따라서, 데이터의 안전한 처리와 무단 액세스로부터의 보호를 위해 모든 API 엔드포인트는 인증을 필요로 합니다. 인증 절차를 통해 사용자가 자격을 갖추었는지 확인하고, 허가된 사용자만이 API에 접근할 수 있도록 함으로써, 데이터의 무결성과 서비스의 안전성을 보장합니다.

인증을 통해 우리는 서비스의 사용자를 식별하고, 각 사용자에게 적절한 접근 권한을 제공함으로써 서비스의 보안을 강화하고, 민감한 정보가 잘못된 손에 넘어가는 것을 방지합니다.

### Domain 정보

* host : `172.30.1.100`
* port : `8499`

### 토큰 발급

* **엔드포인트** : `/token`
* **HTTP 메소드** : `POST`
* **파라미터** :
* `username`: `admin`
* `password`: `Illunex123!`
* **응답** :
* `access_token`: 인증 토큰
* `token_type`: 토큰 타입 (일반적으로 "bearer")

### 토큰 사용

* 모든 보호된 API 요청에 대해 `Authorization` 헤더에 발급받은 토큰을 포함해야 합니다.
* 예시: `"Authorization": "Bearer <access_token>"`
* 토큰 유효기간: `7days (60*24*7minutes)`

---

## DART API Domain 정보

* host : `220.118.147.58`
* port : `8502`

## API 엔드포인트 목록

* 기업 정보 조회
  * `/api/v1/dart/info/business/{bizNum}`
  * `/api/v1/dart/info/corporation/{corpNum}`
  * `/api/v1/dart/info/company/{companyId}`
* 기업 재무 정보 조회
  * `/api/v1/dart/finance/business/{bizNum}`
  * `/api/v1/dart/finance/corporation/{corpNum}`
  * `/api/v1/dart/finance/company/{companyId}`
* 기타 엔드포인트
  * `/health`
  * `/scrape/dart_info`
  * `/scrape/dart_finance`

## 상세 엔드포인트 설명

### 1. 기업 정보 조회

### `/api/v1/dart/info/business/{bizNum}`

* **HTTP 메소드** : GET
* **파라미터** :
  * `bizNum`: 사업자등록번호
* **응답 구조** :
  * `newCompanyInfo`: [NewCompanyInfoPydantic]
* **오류** :
  * 400: 사업자등록번호 누락
  * 500: 서버 내부 오류

### `/api/v1/dart/info/corporation/{corpNum}`

* **HTTP 메소드** : GET
* **파라미터** :
  * `corpNum`: 법인등록번호
* **응답 구조** :
  * `newCompanyInfo`: [NewCompanyInfoPydantic]
* **오류** :
  * 400: 법인등록번호 누락
  * 500: 서버 내부 오류

### `/api/v1/dart/info/company/{companyId}`

* **HTTP 메소드** : GET
* **파라미터** :
  * `companyId`: 기업 ID
* **응답 구조** :
  * `newCompanyInfo`: [NewCompanyInfoPydantic]
* **오류** :
  * 400: 기업 ID 누락
  * 500: 서버 내부 오류

### 2. 기업 재무 정보 조회

### `/api/v1/dart/finance/business/{bizNum}`

* **HTTP 메소드** : GET
* **파라미터** :
  * `bizNum`: 사업자등록번호
* **응답 구조** :
  * `newCompanyFinance`: [NewCompanyFinancePydantic]
* **오류** :
  * 400: 사업자등록번호 누락
  * 500: 서버 내부 오류

### `/api/v1/dart/finance/corporation/{corpNum}`

* **HTTP 메소드** : GET
* **파라미터** :
  * `corpNum`: 법인등록번호
* **응답 구조** :
  * `newCompanyFinance`: [NewCompanyFinancePydantic]
* **오류** :
  * 400: 법인등록번호 누락
  * 500: 서버 내부 오류

### `/api/v1/dart/finance/company/{companyId}`

* **HTTP 메소드** : GET
* **파라미터** :
  * `companyId`: 기업 ID
* **응답 구조** :
  * `newCompanyFinance`: [NewCompanyFinancePydantic]
* **오류** :
  * 400: 기업 ID 누락
  * 500: 서버 내부 오류

### 3. 기타 엔드포인트

### `/health`

* **HTTP 메소드** : GET
* **목적** : 서비스 상태 확인
* **응답** : `{"status": "healthy"}`

### `/scrape/dart_info`

* **HTTP 메소드** : GET
* **목적** : OpenDartReader를 이용한 기업 정보 수집
* **응답** : `{"status": "Scraping in progress..."}`

### `/scrape/dart_finance`

* **HTTP 메소드** : GET
* **파라미터** :
  * `bsns_year`: 사업 연도 (선택적)
  * `api_call_limit`: API 호출 제한 (선택적)
* **목적** : OpenDartReader를 이용한 재무 정보 수집
* **응답** : `{"status": "Scraping in progress..."}`

## 에러 코드

* `400 Bad Request`: 요청 파라미터 오류
* `500 Internal Server Error`: 서버 내부 오류

## 추가 정보

프로젝트의 구성 요소 및 상세 구현에 대한 자세한 정보는 프로젝트의 코드 리포지토리에서 확인할 수 있습니다.
