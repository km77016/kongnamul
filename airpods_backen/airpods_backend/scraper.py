"""
외부 시세 참고 스크래퍼

⚠️ 반드시 읽어주세요 ⚠️
- 이 코드는 Anthropic 샌드박스(외부 네트워크 완전 차단 환경)에서 작성되었고,
  실제 사이트에 단 한 번도 접속해 테스트하지 못했습니다. 인터넷이 되는
  본인 컴퓨터/서버에서 먼저 소량으로 테스트한 뒤 사용하세요.
- 중고나라의 "시세조회" 페이지 등은 자바스크립트로 데이터를 그려주는 방식이라
  requests만으로는 빈 페이지만 받아올 가능성이 높습니다. 그래서 아래에
  requests 버전과 playwright(헤드리스 브라우저) 버전을 함께 넣었습니다.
  requests 버전이 안 되면 playwright 버전을 쓰세요.
- 실행 전 각 플랫폼의 이용약관 / robots.txt에서 자동 수집이 허용되는
  범위인지 반드시 확인하세요. 과도한 요청은 IP 차단이나 약관 위반으로
  이어질 수 있습니다. 아래 코드는 요청 사이 sleep(3)을 넣어뒀지만,
  실제 운영에서는 더 긴 간격 + 캐싱을 권장합니다.
- 이 스크립트는 결과를 백엔드(app.py)의 /api/external_ref 엔드포인트로
  전송해서 반영합니다. 이 엔드포인트는 관리자 인증이 필요하므로, 실행 전
  AIRMRKT_ADMIN_PASS 환경변수에 관리자 비밀번호를 설정해야 합니다.
  백엔드가 먼저 실행 중이어야 합니다 (python app.py).

설치 (인터넷이 되는 환경에서):
    pip install requests beautifulsoup4 lxml playwright
    playwright install chromium

실행:
    export AIRMRKT_ADMIN_PASS=서버콘솔에서-확인한-관리자-비밀번호
    python scraper.py                 # 1회 수집 후 백엔드에 반영
    python scraper.py --loop 28800    # 8시간(28800초)마다 반복 실행 (하루 3회 수준)
"""
import re
import sys
import os
import time
import json
import argparse

import requests

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
BACKEND_URL = 'http://localhost:5000'
ADMIN_USERNAME = os.environ.get('AIRMRKT_ADMIN_USER', 'admin')
ADMIN_PASSWORD = os.environ.get('AIRMRKT_ADMIN_PASS', '')  # 실행 전 환경변수로 설정하세요

QUERIES = {
    'pro1': '에어팟 프로1세대',
    'pro2': '에어팟 프로2',
    'pro3': '에어팟 프로3',
    'ap4':  '에어팟4 노캔',
}


def fetch_joongna_avg_requests(query):
    """
    중고나라 '시세조회' 페이지에서 '평균 가격 000,000원' 패턴을 찾아 파싱 시도.
    ⚠️ JS 렌더링 페이지라 실패할 가능성이 높음 -> 실패 시 fetch_joongna_avg_playwright() 사용.
    """
    url = f'https://web.joongna.com/search-price?searchWord={requests.utils.quote(query)}'
    resp = requests.get(url, headers=HEADERS, timeout=10)
    text = resp.text
    m = re.search(r'평균\s*가격\s*([\d,]+)\s*원', text)
    return int(m.group(1).replace(',', '')) if m else None


def fetch_joongna_avg_playwright(query):
    """
    헤드리스 브라우저로 JS 렌더링까지 기다린 뒤 텍스트에서 평균가를 파싱.
    pip install playwright && playwright install chromium 필요.
    """
    from playwright.sync_api import sync_playwright
    url = f'https://web.joongna.com/search-price?searchWord={requests.utils.quote(query)}'
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(url, timeout=15000)
        page.wait_for_timeout(2500)  # 데이터 렌더링 대기
        text = page.inner_text('body')
        browser.close()
    m = re.search(r'평균\s*가격\s*([\d,]+)\s*원', text)
    return int(m.group(1).replace(',', '')) if m else None


def collect_all(use_playwright=True, verbose=True):
    results = {}
    for key, q in QUERIES.items():
        avg = None
        try:
            if use_playwright:
                avg = fetch_joongna_avg_playwright(q)
            else:
                avg = fetch_joongna_avg_requests(q)
        except Exception as e:
            if verbose:
                print(f'[{key}] 수집 실패: {e}', file=sys.stderr)
        if avg:
            results[key] = {'avg': avg, 'note': f'중고나라 시세조회 "{q}" 평균가 (자동수집)'}
            if verbose:
                print(f'[{key}] {q} -> 평균 {avg:,}원')
        else:
            if verbose:
                print(f'[{key}] {q} -> 파싱 실패 (셀렉터/파서를 실제 페이지 구조에 맞게 조정 필요)')
        time.sleep(3)  # 과도한 요청 방지용 딜레이. 실제 운영에서는 더 길게 권장.
    return results


def push_to_backend(results):
    if not results:
        print('반영할 데이터가 없어요.')
        return
    if not ADMIN_PASSWORD:
        print('AIRMRKT_ADMIN_PASS 환경변수가 설정되어 있지 않아요. 백엔드에 반영하지 않고 종료합니다.')
        print('예: export AIRMRKT_ADMIN_PASS=서버콘솔에서-확인한-비밀번호')
        return
    session = requests.Session()
    login_resp = session.post(f'{BACKEND_URL}/api/admin/login', json={
        'username': ADMIN_USERNAME, 'password': ADMIN_PASSWORD
    }, timeout=5)
    if login_resp.status_code != 200:
        print('관리자 로그인 실패:', login_resp.status_code, login_resp.text)
        return
    csrf_token = login_resp.json().get('csrf_token')
    resp = session.post(f'{BACKEND_URL}/api/external_ref', json=results,
                         headers={'X-CSRF-Token': csrf_token}, timeout=5)
    print('백엔드 반영 결과:', resp.status_code, resp.text)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--loop', type=int, default=0, help='N초마다 반복 실행 (0이면 1회만 실행)')
    parser.add_argument('--no-playwright', action='store_true', help='playwright 대신 requests만 사용')
    args = parser.parse_args()

    def run_once():
        results = collect_all(use_playwright=not args.no_playwright)
        print(json.dumps(results, ensure_ascii=False, indent=2))
        push_to_backend(results)

    if args.loop > 0:
        while True:
            run_once()
            time.sleep(args.loop)
    else:
        run_once()


if __name__ == '__main__':
    main()
