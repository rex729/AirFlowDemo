import urllib.request
import re
from datetime import datetime
from airflow.decorators import dag, task

# 抓取網頁標題的工具
def fetch_title(url: str) -> str:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            html = response.read().decode("utf-8", errors="ignore")
            match = re.search(r"<title>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
            if match:
                return match.group(1).strip()
            return "（未找到 <title> 標籤）"
    except Exception as e:
        return f"連線失敗: {e}"

@dag(
    dag_id="task_abc_demo",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["web", "loop"],
)
def task_abc_pipeline():

    @task
    def task_a():
        url = "https://example.com"
        title = fetch_title(url)
        print("\n" + "=" * 50)
        print("【Task A】")
        print(f"  目標網址: {url}")
        print(f"  網頁標題: {title}")
        print("=" * 50)
        return title

    @task
    def task_b(prev_title: str):
        url = "https://www.python.org"
        title = fetch_title(url)
        print("\n" + "=" * 50)
        print("【Task B】")
        print(f"  目標網址: {url}")
        print(f"  網頁標題: {title}")
        print("=" * 50)
        return title

    @task
    def task_c(prev_title: str):
        url = "https://news.ycombinator.com"
        title = fetch_title(url)
        print("\n" + "=" * 50)
        print("【Task C】")
        print(f"  目標網址: {url}")
        print(f"  網頁標題: {title}")
        print("=" * 50)
        # Task C 完全不等待，印完標題立刻結束！
        return title

    # 依序執行：A -> B -> C
    res_a = task_a()
    res_b = task_b(res_a)
    task_c(res_b)

task_abc_pipeline()
