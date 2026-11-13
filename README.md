# Apache Airflow 實戰專案：架構實踐、使用心得與開發準則

[![Docker Hub](https://img.shields.io/badge/Docker%20Hub-uck729%2Fmy--ubuntu--airflow-blue?logo=docker&logoColor=white)](https://hub.docker.com/r/uck729/my-ubuntu-airflow)
[![GitHub Repository](https://img.shields.io/badge/GitHub-rex729%2FAirFlowDemo-black?logo=github)](https://github.com/rex729/AirFlowDemo)

本專案示範如何以 **Ubuntu 22.04** 為基礎映像檔，將 **Apache Airflow** 容器化打包，建構具備高穩定性、乾淨輸出與持續循環執行的現代化自動化管線（Pipeline）。

* **Docker Hub 公開映像檔**：[`uck729/my-ubuntu-airflow:latest`](https://hub.docker.com/r/uck729/my-ubuntu-airflow)

---

## 目錄
- [一、專案簡介與快速上手](#一專案簡介與快速上手)
- [二、Airflow 實戰使用心得（踩坑經驗精華）](#二airflow-實戰使用心得踩坑經驗精華)
- [三、Airflow 開發核心準則（Best Practices）](#三airflow-開發核心準則best-practices)
- [四、專案檔案結構說明](#四專案檔案結構說明)

---

## 一、專案簡介與快速上手

本專案包含一組依序執行的網頁爬蟲與資料擷取任務（Task A ➔ Task B ➔ Task C），並透過調度機制實現每隔 10 秒自動執行一輪的持續性排程作業。

### 方法 A：直接從 Docker Hub 執行（推薦，免下載原始碼）
只要您安裝了 Docker，全世界任何一台電腦直接輸入以下指令即可一鍵執行：
```powershell
docker run --rm uck729/my-ubuntu-airflow:latest
```

### 方法 B：自行於本機建置並執行
如果您複製（Clone）了本專案原始碼，亦可在本機自行編譯：
```powershell
# 1. 建置 Docker 映像檔
docker build -t uck729/my-ubuntu-airflow .

# 2. 啟動容器執行
docker run --rm uck729/my-ubuntu-airflow
```
> **說明**：加上 `--rm` 可以在您按 `Ctrl + C` 停止時，自動清理退出的容器，避免在硬碟累積無用的殘留實例。

---

## 二、Airflow 實戰使用心得（踩坑經驗精華）

在從零打造本專案的過程中，我們歸納出以下數項極具價值的實戰心得：

### 1. Windows 原生 vs. Docker 容器化
* **心得**：Airflow 底層架構深度依賴 Linux 的多行程管理（如 `fork` 系統呼叫）與 POSIX 標準，原生 Windows 環境無法直接提供良好支援。
* **結論**：不要浪費時間在 Windows 本機進行原生安裝。**直接以 Ubuntu 為基底建立 Docker Image 是最穩定、最專業的作法**，真正實現「一次建置，隨處部署（Build Once, Run Anywhere）」。

### 2. 純 Python 腳本 vs. Airflow 工作流排程
* **純 Python 腳本（`while True` 輪詢）**：
  * **適用場景**：極輕量、幾十行內的小程式、不需追蹤歷史執行紀錄、掛掉無所謂的本機工具。
* **Airflow 工作流編排**：
  * **適用場景**：複雜的任務依賴（DAG）、定時批次排程（ETL）、視覺化監控面板、失敗自動重試（Retry）與警報通知。
* **心得**：切勿拿大砲打小鳥，但只要涉及多步驟依賴與穩定的資料管線，引入 Airflow 能省下大量自己造輪子寫排程與重試機制的成本。

### 3. DAG「無環」本質與循環機制的正確理解
* **心得**：Airflow 的全名是 **DAG（Directed Acyclic Graph，有向「無環」圖）**，系統層面嚴格禁止在單一流程內循環（例如 `A ➔ B ➔ C ➔ A` 會直接噴出 `CycleException` 錯誤）。
* **大忌（Anti-pattern）**：**嚴禁在 Task 內部寫 `while True:` 進行死循環**！這樣會永久佔死一個 Worker 執行插槽，日誌會無限暴增撐爆硬碟，且無法記錄每一次執行的成功/失敗。
* **正解**：讓每一次執行的任務都是**短暫、獨立、有紀錄的**。循環與延遲應交由外層調度機制（如 Runner 循環或排程器）來喚醒，這才是成熟的管線設計。

### 4. 職責分離（Separation of Concerns）
* **心得**：業務任務（如 Task C）只該做好自己份內的事（抓取網頁標題、輸出結果並立刻結束，耗時 0.2 秒）。
* **原則**：**「等待 10 秒後再出發」這項邏輯屬於「調度層（Scheduling Layer）」的責任**，不該硬塞在業務 Task 裡面卡住執行緒。

### 5. 除錯哲學：謹慎使用輸出遮蔽
* **教訓**：在容器啟動腳本中，若過度使用 `> /dev/null 2>&1` 將標準輸出與標準錯誤全部吃掉，一旦系統在開機前置階段發生錯誤（例如資料庫尚未解析 DAG），畫面會變成完全無聲無息的閃退或卡死，極難排查。
* **準則**：只遮蔽已知無害的初始化雜訊，**絕對保留標準錯誤輸出（stderr）**，讓任何潛在錯誤能在第一時間被看見。

---

## 三、Airflow 開發核心準則（Best Practices）

為了維持工作流程的健壯性與可維護性，在撰寫 Airflow DAG 時務必遵循以下黃金準則：

### 準則 1：任務冪等性（Idempotency）⭐ 最核心
> **定義**：同一個任務使用相同的輸入參數執行一次或重複執行十次，對外部系統產生的最終結果必須完全相同。
* **反例**：任務每跑一次就無條件對資料庫 `INSERT` 一筆重複資料。
* **正例**：採用 `UPSERT`、覆蓋特定日期分區、或在寫入前先確認該批次是否已存在。

### 準則 2：單一職責原則（Single Responsibility）
* 一個 Task 只專注做好一件事：
  * ❌ `下載資料_清洗資料_並寫入資料庫()`（全部混在一個 Task，失敗時無從判斷死在哪一步）。
  * ✅ `下載資料` ➔ `清洗資料` ➔ `寫入資料庫`（拆成三個 Task，失敗時紅燈一目了然，且支援單點重跑）。

### 準則 3：避免在 Task 之間傳遞龐大資料（XCom 陷阱）
* Airflow 的 **XCom**（任務間通訊）本質是將資料儲存在 Airflow 的中繼資料庫（Metadata DB）中。
* **適用**：傳遞小字串、狀態標籤、計數、或檔案路徑（如 `s3://bucket/data_20261113.parquet`）。
* **嚴禁**：直接透過 XCom 傳遞幾十 MB 的 Pandas DataFrame 或巨量 JSON，這會迅速拖垮資料庫效能。

### 準則 4：嚴禁在 DAG 定義層編寫耗時程式碼（Top-level Code Rule）
* Airflow 排程器每隔數秒就會重新讀取並解析一次 `dags/` 目錄下的所有 Python 檔案。
* **嚴禁**：在 `@task` 函式之外直接呼叫 HTTP API、查詢資料庫、或進行大檔案讀寫。
* **正確做法**：所有實質的運算與網路請求，必須完整封裝在 Task 函式本體內部。

### 準則 5：機密憑證與金鑰分離
* 嚴禁將 API Key（例如 OpenAI API Key、資料庫密碼）硬編碼寫在 DAG Python 檔案中。
* 應統一透過 **Airflow Variables**、**Airflow Connections** 或 **Docker 環境變數（`-e`）** 進行安全注入。

---

## 四、專案檔案結構說明

```text
AirFlowDemo/
├── Dockerfile              # 定義 Ubuntu 22.04 基底、Airflow 全域安裝與開機自動化運行邏輯
├── requirements.txt        # Python 額外依賴套件清單
├── README.md               # 專案實戰心得、架構解析與開發準則說明文件
└── dags/
    └── abc_pipeline.py     # 核心業務管線：Task A -> Task B -> Task C 網頁標題擷取流程
```
