"""
AIRMRKT 백엔드 API 서버
- Flask + SQLite (둘 다 별도 설치 없이 동작)
- 30초마다(운영시엔 원하는 주기로 조정) 자체 거래 + 외부 참고 시세를 블렌딩해 시세를 갱신
- 프론트엔드(static/index.html)를 같은 서버에서 서빙

실행: python app.py  ->  http://localhost:5000
"""
import sqlite3, time, random, threading, os, uuid, re, smtplib, secrets, queue, json, base64, shutil
import requests
from datetime import datetime, timezone, timedelta
from email.mime.text import MIMEText
from flask import Flask, jsonify, request, session, Response, stream_with_context
from werkzeug.security import generate_password_hash, check_password_hash

# --- DB 파일 위치 ---
# 앱 폴더(app.py가 있는 곳) 안에 market.db를 두면, 새 zip을 받아서 폴더를 덮어쓸 때마다
# 데이터가 통째로 사라질 위험이 있어요. 그래서 기본적으로 "홈 폴더/kongnamul_data/market.db"처럼
# 앱 폴더 밖에 저장해요. 원하는 위치를 쓰고 싶으면 AIRMRKT_DB_PATH 환경변수로 지정하세요.
_APP_DIR = os.path.dirname(os.path.abspath(__file__))
_OLD_DB_PATH = os.path.join(_APP_DIR, 'market.db')  # 예전 버전(앱 폴더 안)에 저장했던 위치
_DEFAULT_DATA_DIR = os.path.join(os.path.expanduser('~'), 'kongnamul_data')
DB_PATH = os.environ.get('AIRMRKT_DB_PATH') or os.path.join(_DEFAULT_DATA_DIR, 'market.db')
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

if not os.path.exists(DB_PATH) and os.path.exists(_OLD_DB_PATH):
    # 예전 버전에서 쓰던 market.db가 앱 폴더 안에 남아있으면, 잃어버리지 않도록 새 위치로 옮겨요.
    shutil.copy2(_OLD_DB_PATH, DB_PATH)
    print('=' * 60)
    print('기존 market.db를 발견해서 앱 폴더 밖 안전한 위치로 복사했어요.')
    print(f'  이전 위치: {_OLD_DB_PATH}  (그대로 남겨뒀어요, 확인 후 지우셔도 돼요)')
    print(f'  새 위치:   {DB_PATH}  (앞으로 이 파일을 계속 써요)')
    print('앞으로는 zip을 새로 받아서 폴더를 덮어써도 이 데이터는 안전해요.')
    print('=' * 60)


def load_dotenv_simple(path):
    """python-dotenv 없이 .env 파일을 읽어 os.environ에 채워넣는 최소 구현.
    이미 설정된 환경변수는 덮어쓰지 않습니다."""
    if not os.path.exists(path):
        return
    with open(path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            key, _, value = line.partition('=')
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)


load_dotenv_simple(os.path.join(os.path.dirname(__file__), '.env'))

# --- 이메일 발송 설정 ---
# 아래 3개 환경변수를 설정하면 실제로 이메일이 발송됩니다 (예: Gmail 앱 비밀번호).
#   SMTP_HOST=smtp.gmail.com  SMTP_USER=you@gmail.com  SMTP_PASS=앱비밀번호16자리
# 설정하지 않으면 DEV_MODE로 동작해서 서버 콘솔에 코드가 출력되고, API 응답에도
# dev_code로 코드가 함께 내려가서(로컬 테스트용) 실제 메일함 없이도 흐름을 테스트할 수 있습니다.
SMTP_HOST = os.environ.get('SMTP_HOST')
SMTP_PORT = int(os.environ.get('SMTP_PORT', '587'))
SMTP_USER = os.environ.get('SMTP_USER')
SMTP_PASS = os.environ.get('SMTP_PASS')
DEV_MODE = not (SMTP_HOST and SMTP_USER and SMTP_PASS)

CODE_TTL_MINUTES = 5
CODE_RESEND_COOLDOWN_SECONDS = 60
CODE_MAX_ATTEMPTS = 5
EMAIL_RE = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')

# --- 토스페이먼츠(카드/간편결제) 연동 설정 ---
# 아래 기본값은 토스페이먼츠 문서(docs.tosspayments.com)에 공개된 "결제창(Payment Window) V1"
# 범용 테스트 키입니다. 실 서비스에서는 반드시 본인 상점의 키로 교체하세요
# (developers.tosspayments.com 가입 -> 개발자센터 -> API 키).
# ⚠️ 이 샌드박스는 외부 네트워크가 차단되어 있어 아래 키로 실제 결제 승인 API를 호출하는 부분은
# 여기서 검증하지 못했습니다. 표준 문서에 나온 요청 형식 그대로 구현했습니다.
TOSS_CLIENT_KEY = os.environ.get('TOSS_CLIENT_KEY', 'test_ck_D5GePWvyJnrK0W0k6q8gLzN97Eoq')
TOSS_SECRET_KEY = os.environ.get('TOSS_SECRET_KEY', 'test_sk_zXLkKEypNArWmo50nX3lmeaxYG5R')
TOSS_API_BASE = 'https://api.tosspayments.com/v1'

MODEL_DEFAULTS = {
    'pro1': {'code': 'PRO1', 'name': 'AirPods Pro 1세대', 'ratios': {'S': 1.27, 'A': 1.00, 'B': 0.77, 'C': 0.55}, 'mid': 80000,  'floor': 45000,  'ceil': 120000},
    'pro2': {'code': 'PRO2', 'name': 'AirPods Pro 2세대', 'ratios': {'S': 1.22, 'A': 1.00, 'B': 0.78, 'C': 0.56}, 'mid': 150000, 'floor': 90000,  'ceil': 220000},
    'pro3': {'code': 'PRO3', 'name': 'AirPods Pro 3세대', 'ratios': {'S': 1.15, 'A': 1.00, 'B': 0.85, 'C': 0.69}, 'mid': 260000, 'floor': 190000, 'ceil': 330000},
    'ap4':  {'code': 'AP4',  'name': 'AirPods 4세대(노캔)', 'ratios': {'S': 1.30, 'A': 1.00, 'B': 0.75, 'C': 0.55}, 'mid': 170000, 'floor': 100000, 'ceil': 240000},
}

# 2026-07-07 실제 검색으로 확인한 참고 시세 (수동 스냅샷). scraper.py로 갱신하거나
# /api/external_ref 로 직접 업데이트할 수 있음.
EXTERNAL_REF_DEFAULTS = {
    'pro1': {'avg': 96000,  'note': '중고나라 시세조회 자체 집계 평균가'},
    'pro2': {'avg': 150000, 'note': '풀박스 매물 다수 표본 (13~23만원대 분포)'},
    'pro3': {'avg': 260000, 'note': '미개봉·새상품급 매물 다수 표본 (25~31만원대)'},
    'ap4':  {'avg': 170000, 'note': '전체세트 매물 표본 부족 · 참고치'},
}

GRADES = ['S', 'A', 'B', 'C']

# 아래 값들은 이제 하드코딩이 아니라 DB(settings 테이블)에 저장되고, 관리자 대시보드의
# "설정" 탭에서 실시간으로 바꿀 수 있습니다. 여기 있는 값은 최초 설치 시의 기본값이에요.
DEFAULT_SETTINGS = {
    'buy_spread': 0.045,          # 우리가 매입: 시세보다 저렴하게
    'sell_markup': 0.035,         # 우리가 판매: 시세보다 살짝 비싸게 (기존 6% -> 3.5%로 인하)
    'storage_free_days': 30,
    'storage_fee_per_month': 2000,
    'delivery_base_fee': 5000,    # 포장비 포함
    'remote_surcharge': 3000,     # 제주/도서산간 추가요금
    'tick_seconds': 30,           # 데모용 주기. 운영에서는 하루 N회 수준(예: 28800)으로
    'instant_withdraw_fee': 0.015,  # 즉시출금 수수료 (일반출금은 무료)
    'plus_monthly_fee': 4900,       # 콩나물 플러스 월 구독료
    'fast_track_fee': 5000,         # 매입 빠른처리 수수료 (정산액에서 차감)
    'certificate_fee': 3000,        # 정품 인증서 발급 수수료 (플러스 회원 무료)
    'gift_service_fee': 2000,       # 선물하기 포장/서비스 수수료
    'card_payment_enabled': 0,      # 카드/간편결제 사용 여부. 0=계좌이체만, 1=카드결제도 노출
    'instant_withdraw_enabled': 0,  # 즉시출금 노출 여부. 0=일반(주간 일괄정산)만, 1=즉시출금도 노출
}
_settings_cache = {}

def load_settings_cache():
    conn = get_db()
    rows = conn.execute('SELECT key,value FROM settings').fetchall()
    conn.close()
    for r in rows:
        _settings_cache[r['key']] = float(r['value'])
    for k, v in DEFAULT_SETTINGS.items():
        _settings_cache.setdefault(k, float(v))

def get_setting(key):
    if key not in _settings_cache:
        load_settings_cache()
    return _settings_cache.get(key, float(DEFAULT_SETTINGS[key]))

def set_setting(key, value):
    conn = get_db(); c = conn.cursor()
    c.execute('INSERT OR REPLACE INTO settings(key,value) VALUES (?,?)', (key, str(value)))
    conn.commit(); conn.close()
    _settings_cache[key] = float(value)


def next_settlement_date():
    """다음 출금 정산일(일요일, 한국시간 기준) 날짜를 ISO 문자열로 반환해요. 오늘이 일요일이면 오늘 날짜를 줘요."""
    kst_now = datetime.now(timezone.utc) + timedelta(hours=9)
    days_until_sunday = (6 - kst_now.weekday()) % 7  # 월요일=0 ... 일요일=6
    return (kst_now + timedelta(days=days_until_sunday)).date().isoformat()


def calc_storage_fee(ts_iso, is_plus=False):
    if is_plus:
        return 0
    from datetime import datetime as _dt
    stored_at = _dt.fromisoformat(ts_iso)
    now = _dt.now(timezone.utc)
    if stored_at.tzinfo is None:
        stored_at = stored_at.replace(tzinfo=timezone.utc)
    days = (now - stored_at).total_seconds() / 86400
    free_days = get_setting('storage_free_days')
    if days <= free_days:
        return 0
    import math
    extra_months = math.ceil((days - free_days) / 30)
    return extra_months * get_setting('storage_fee_per_month')

app = Flask(__name__, static_folder='static', static_url_path='')
app.secret_key = os.environ.get('AIRMRKT_SECRET_KEY', secrets.token_hex(32))
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_HTTPONLY'] = True
# HTTPS 뒤에서 운영할 때 AIRMRKT_FORCE_HTTPS=1로 설정하면 쿠키에 Secure 플래그가 붙습니다.
# 로컬 http://localhost 테스트 중에는 켜면 쿠키가 아예 전달되지 않으니 꺼둔 채로 두세요.
app.config['SESSION_COOKIE_SECURE'] = os.environ.get('AIRMRKT_FORCE_HTTPS') == '1'
LAST_TICK = time.time()
SERVER_STARTED_AT = datetime.now(timezone.utc).isoformat()
_last_settlement_reminder_date = None  # 일요일 정산 리마인더를 하루에 한 번만 보내기 위한 추적용


@app.after_request
def set_security_headers(resp):
    resp.headers['X-Content-Type-Options'] = 'nosniff'
    resp.headers['X-Frame-Options'] = 'DENY'
    resp.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
    resp.headers['Content-Security-Policy'] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline'; "
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://cdn.jsdelivr.net; "
        "font-src 'self' https://fonts.gstatic.com https://cdn.jsdelivr.net; "
        "connect-src 'self'"
    )
    if os.environ.get('AIRMRKT_FORCE_HTTPS') == '1':
        resp.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
    return resp


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    # WAL(Write-Ahead Logging) 모드: 읽기와 쓰기가 서로 덜 막게 해줘서 동시 접속에 훨씬 유리해요.
    # busy_timeout: 다른 연결이 쓰고 있을 때 바로 에러내지 않고 최대 5초까지 기다렸다가 재시도해요.
    # 둘 다 SQLite 파일 자체에 적용되는 설정이라 매 연결마다 실행해도 비용이 거의 없어요.
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA busy_timeout=5000')
    conn.execute('PRAGMA synchronous=NORMAL')
    return conn


def init_db(force=False):
    if force and os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    fresh = not os.path.exists(DB_PATH)
    conn = get_db()
    c = conn.cursor()
    c.executescript('''
    CREATE TABLE IF NOT EXISTS models(
        key TEXT PRIMARY KEY, code TEXT, name TEXT,
        ratio_s REAL, ratio_a REAL, ratio_b REAL, ratio_c REAL,
        mid REAL, floor_p REAL, ceil_p REAL, internal_trade_count INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS history(
        id INTEGER PRIMARY KEY AUTOINCREMENT, model_key TEXT, mid REAL, ts TEXT
    );
    CREATE TABLE IF NOT EXISTS stock(
        model_key TEXT, grade TEXT, qty INTEGER, PRIMARY KEY(model_key, grade)
    );
    CREATE TABLE IF NOT EXISTS trades(
        id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, model_key TEXT, grade TEXT,
        side TEXT, price REAL, label TEXT, ts TEXT, pending INTEGER DEFAULT 1
    );
    CREATE TABLE IF NOT EXISTS portfolio(
        id TEXT PRIMARY KEY, user_id INTEGER, model_key TEXT, grade TEXT, bought_price REAL, ts TEXT
    );
    CREATE TABLE IF NOT EXISTS wallet(
        user_id INTEGER PRIMARY KEY, balance REAL
    );
    CREATE TABLE IF NOT EXISTS external_ref(
        model_key TEXT PRIMARY KEY, avg REAL, note TEXT, updated_at TEXT
    );
    CREATE TABLE IF NOT EXISTS stats(
        id INTEGER PRIMARY KEY, total_inspected INTEGER, today_trades INTEGER
    );
    CREATE TABLE IF NOT EXISTS users(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        created_at TEXT,
        is_suspended INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS settings(
        key TEXT PRIMARY KEY, value TEXT
    );
    CREATE TABLE IF NOT EXISTS verification_codes(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT NOT NULL,
        code TEXT NOT NULL,
        purpose TEXT NOT NULL,
        created_at TEXT,
        expires_at TEXT,
        attempts INTEGER DEFAULT 0,
        verified INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS admins(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        created_at TEXT
    );
    CREATE TABLE IF NOT EXISTS bank_accounts(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bank_name TEXT, account_number TEXT, holder_name TEXT,
        is_active INTEGER DEFAULT 0, created_at TEXT
    );
    CREATE TABLE IF NOT EXISTS deposit_requests(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, amount REAL, reference_code TEXT,
        bank_account_id INTEGER, status TEXT DEFAULT 'pending',
        created_at TEXT, decided_at TEXT, decided_by TEXT
    );
    CREATE TABLE IF NOT EXISTS login_failures(
        email TEXT PRIMARY KEY, count INTEGER DEFAULT 0, locked_until TEXT
    );
    CREATE TABLE IF NOT EXISTS audit_log(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        actor_type TEXT, actor TEXT, action TEXT, detail TEXT, ip TEXT, ts TEXT
    );
    CREATE TABLE IF NOT EXISTS revenue_events(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        type TEXT, amount REAL, user_id INTEGER, ts TEXT
    );
    CREATE TABLE IF NOT EXISTS sell_requests(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, model_key TEXT, self_grade TEXT, note TEXT,
        estimated_price REAL, final_grade TEXT, final_price REAL,
        status TEXT DEFAULT 'submitted', tracking_note TEXT, admin_note TEXT,
        created_at TEXT, updated_at TEXT
    );
    CREATE TABLE IF NOT EXISTS announcements(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT, body TEXT, pinned INTEGER DEFAULT 0, created_at TEXT
    );
    CREATE TABLE IF NOT EXISTS reservations(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, model_key TEXT, grade TEXT, created_at TEXT,
        UNIQUE(user_id, model_key, grade)
    );
    CREATE TABLE IF NOT EXISTS reviews(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, trade_id INTEGER UNIQUE, model_key TEXT, grade TEXT,
        rating INTEGER, comment TEXT, created_at TEXT
    );
    CREATE TABLE IF NOT EXISTS business_info(
        key TEXT PRIMARY KEY, value TEXT
    );
    CREATE TABLE IF NOT EXISTS pg_orders(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, order_id TEXT UNIQUE, amount REAL,
        status TEXT DEFAULT 'pending', created_at TEXT, paid_at TEXT
    );
    CREATE TABLE IF NOT EXISTS price_alerts(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, model_key TEXT, grade TEXT, target_price REAL,
        triggered INTEGER DEFAULT 0, created_at TEXT
    );
    CREATE TABLE IF NOT EXISTS wishlist(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, model_key TEXT, grade TEXT, created_at TEXT,
        UNIQUE(user_id, model_key, grade)
    );
    CREATE TABLE IF NOT EXISTS withdrawal_requests(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, amount REAL, fee REAL, net_amount REAL,
        bank_name TEXT, account_number TEXT, holder_name TEXT,
        priority TEXT DEFAULT 'normal', status TEXT DEFAULT 'pending',
        created_at TEXT, decided_at TEXT, decided_by TEXT
    );
    CREATE TABLE IF NOT EXISTS certificates(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trade_id INTEGER UNIQUE, user_id INTEGER, cert_code TEXT UNIQUE,
        fee_paid REAL, issued_at TEXT
    );
    CREATE TABLE IF NOT EXISTS gifts(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        from_user_id INTEGER, to_email TEXT, model_key TEXT, grade TEXT,
        price REAL, service_fee REAL, message TEXT, status TEXT DEFAULT 'pending',
        created_at TEXT, claimed_at TEXT, portfolio_item_id TEXT
    );
    ''')
    # 기존 DB(이 컬럼이 추가되기 전에 만들어진 market.db)를 위한 간단한 마이그레이션
    try:
        c.execute('ALTER TABLE users ADD COLUMN is_suspended INTEGER DEFAULT 0')
    except sqlite3.OperationalError:
        pass  # 이미 컬럼이 있으면 무시
    try:
        c.execute('ALTER TABLE users ADD COLUMN payment_pin_hash TEXT')
    except sqlite3.OperationalError:
        pass
    try:
        c.execute('ALTER TABLE models ADD COLUMN is_active INTEGER DEFAULT 1')
    except sqlite3.OperationalError:
        pass
    try:
        c.execute('ALTER TABLE models ADD COLUMN drop_start TEXT')
    except sqlite3.OperationalError:
        pass
    try:
        c.execute('ALTER TABLE models ADD COLUMN drop_end TEXT')
    except sqlite3.OperationalError:
        pass
    try:
        c.execute('ALTER TABLE portfolio ADD COLUMN risk_notified INTEGER DEFAULT 0')
    except sqlite3.OperationalError:
        pass
    try:
        c.execute('ALTER TABLE users ADD COLUMN phone_number TEXT')
    except sqlite3.OperationalError:
        pass
    try:
        c.execute('ALTER TABLE users ADD COLUMN phone_verified INTEGER DEFAULT 0')
    except sqlite3.OperationalError:
        pass
    try:
        c.execute('ALTER TABLE users ADD COLUMN plus_active INTEGER DEFAULT 0')
    except sqlite3.OperationalError:
        pass
    try:
        c.execute('ALTER TABLE users ADD COLUMN plus_expires_at TEXT')
    except sqlite3.OperationalError:
        pass
    try:
        c.execute('ALTER TABLE users ADD COLUMN plus_auto_renew INTEGER DEFAULT 1')
    except sqlite3.OperationalError:
        pass
    try:
        c.execute('ALTER TABLE sell_requests ADD COLUMN is_fast_track INTEGER DEFAULT 0')
    except sqlite3.OperationalError:
        pass
    try:
        c.execute('ALTER TABLE sell_requests ADD COLUMN fast_track_fee REAL DEFAULT 0')
    except sqlite3.OperationalError:
        pass
    c.execute("INSERT OR IGNORE INTO business_info(key,value) VALUES ('admin_notify_emails','')")
    if fresh:
        for k, m in MODEL_DEFAULTS.items():
            c.execute('''INSERT INTO models(key,code,name,ratio_s,ratio_a,ratio_b,ratio_c,mid,floor_p,ceil_p,
                         internal_trade_count,is_active,drop_start,drop_end) VALUES (?,?,?,?,?,?,?,?,?,?,0,1,NULL,NULL)''',
                      (k, m['code'], m['name'], m['ratios']['S'], m['ratios']['A'],
                       m['ratios']['B'], m['ratios']['C'], m['mid'], m['floor'], m['ceil']))
            v = m['mid'] * random.uniform(0.96, 0.98)
            for _ in range(19):
                v = v * (1 + random.uniform(-0.006, 0.006))
                c.execute('INSERT INTO history(model_key,mid,ts) VALUES (?,?,?)', (k, round(v), now_iso()))
            c.execute('INSERT INTO history(model_key,mid,ts) VALUES (?,?,?)', (k, m['mid'], now_iso()))
            for g in GRADES:
                qty = random.randint(1, 3) if g in ('S', 'C') else random.randint(2, 6)
                c.execute('INSERT INTO stock VALUES (?,?,?)', (k, g, qty))
        for k, r in EXTERNAL_REF_DEFAULTS.items():
            c.execute('INSERT INTO external_ref VALUES (?,?,?,?)', (k, r['avg'], r['note'], now_iso()))
        c.execute('INSERT INTO stats VALUES (1, 1842, 27)')
        c.execute('INSERT INTO bank_accounts(bank_name,account_number,holder_name,is_active,created_at) VALUES (?,?,?,1,?)',
                   ('국민은행', '123456-78-901234', '콩나물(주)', now_iso()))
        admin_user = os.environ.get('AIRMRKT_ADMIN_USER', 'admin')
        admin_pass = os.environ.get('AIRMRKT_ADMIN_PASS') or secrets.token_urlsafe(9)
        c.execute('INSERT INTO admins(username,password_hash,created_at) VALUES (?,?,?)',
                   (admin_user, generate_password_hash(admin_pass), now_iso()))
        if not os.environ.get('AIRMRKT_ADMIN_PASS'):
            print('=' * 60)
            print(f'초기 관리자 계정이 생성됐어요 -> username: {admin_user}  password: {admin_pass}')
            print('이 비밀번호는 다시 표시되지 않으니 지금 기록해두세요.')
            print('=' * 60)
        c.execute('INSERT INTO announcements(title,body,pinned,created_at) VALUES (?,?,1,?)',
                  ('콩나물에 오신 걸 환영해요 🌱', '검수완료 에어팟을 안심하고 사고팔 수 있는 곳, 콩나물이에요. 궁금한 점은 언제든 문의해주세요.', now_iso()))
        biz_defaults = {
            'company_name': '콩나물(예시 상호명 - 실제 상호로 변경하세요)',
            'ceo_name': '홍길동',
            'biz_reg_no': '000-00-00000',
            'mail_order_no': '제0000-서울강남-00000호',
            'address': '서울특별시 강남구 테헤란로 000 (실제 주소로 변경하세요)',
            'phone': '02-0000-0000',
            'email': 'help@example.com',
        }
        for k, v in biz_defaults.items():
            c.execute('INSERT INTO business_info(key,value) VALUES (?,?)', (k, v))
    conn.commit()
    conn.close()


# ---------------- 정품 인증서 PDF 생성 ----------------
# 리포트랩(reportlab)의 TrueType 폰트 로더는 CFF/OpenType 윤곽선(예: Noto Sans CJK 원본)을
# 지원하지 않아서, 미리 TrueType(glyf) 형식으로 변환한 한글 서브셋 폰트를 fonts/ 폴더에
# 번들해뒀습니다. 사용자 OS에 어떤 한글 폰트가 깔려있는지와 무관하게 항상 동작해요.
CERT_FONT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fonts', 'NotoSansKR-Subset.ttf')
_cert_font_ready = False

def register_cert_font():
    global _cert_font_ready
    if _cert_font_ready:
        return True
    if not os.path.exists(CERT_FONT_PATH):
        print(f'[경고] 인증서용 폰트를 찾을 수 없어요: {CERT_FONT_PATH}')
        return False
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont as RLTTFont
    pdfmetrics.registerFont(RLTTFont('KoreanCert', CERT_FONT_PATH))
    _cert_font_ready = True
    return True


def generate_certificate_pdf(cert_code, model_name, grade, price, buyer_email, trade_date, issued_date):
    from reportlab.pdfgen import canvas as rl_canvas
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    import io

    have_font = register_cert_font()
    FONT = 'KoreanCert' if have_font else 'Helvetica'

    buf = io.BytesIO()
    c = rl_canvas.Canvas(buf, pagesize=A4)
    W, H = A4

    c.setStrokeColorRGB(0.055, 0.62, 0.451)
    c.setLineWidth(2)
    c.rect(15 * mm, 15 * mm, W - 30 * mm, H - 30 * mm)
    c.setStrokeColorRGB(0.58, 0.76, 0.23)
    c.setLineWidth(0.6)
    c.rect(18 * mm, 18 * mm, W - 36 * mm, H - 36 * mm)

    c.setFont(FONT, 22)
    c.setFillColorRGB(0.13, 0.17, 0.12)
    c.drawCentredString(W / 2, H - 42 * mm, '콩나물')
    c.setFont(FONT, 15)
    c.drawCentredString(W / 2, H - 52 * mm, '정품 검수 인증서')
    c.setFont('Helvetica', 9)
    c.setFillColorRGB(0.45, 0.48, 0.4)
    c.drawCentredString(W / 2, H - 59 * mm, 'Authenticity & Inspection Certificate')

    c.setStrokeColorRGB(0.85, 0.85, 0.78)
    c.line(30 * mm, H - 68 * mm, W - 30 * mm, H - 68 * mm)

    rows = [
        ('인증코드', cert_code),
        ('상품명', model_name),
        ('검수 등급', f'{grade}급'),
        ('구매가', f'{int(price):,}원'),
        ('구매자', buyer_email),
        ('구매일', trade_date),
        ('발급일', issued_date),
    ]
    y = H - 84 * mm
    for label, value in rows:
        c.setFont(FONT, 10)
        c.setFillColorRGB(0.45, 0.48, 0.4)
        c.drawString(32 * mm, y, label)
        c.setFont(FONT, 12)
        c.setFillColorRGB(0.13, 0.17, 0.12)
        c.drawString(65 * mm, y, str(value))
        y -= 10 * mm

    y -= 6 * mm
    c.setFont(FONT, 9)
    c.setFillColorRGB(0.45, 0.48, 0.4)
    c.drawString(32 * mm, y, '본 인증서는 콩나물이 자체 검수 프로세스를 통해 위 상품의 정품 여부와')
    c.drawString(32 * mm, y - 5.5 * mm, '상태 등급을 확인했음을 증명합니다.')

    c.setFont(FONT, 8)
    c.setFillColorRGB(0.6, 0.6, 0.6)
    c.drawCentredString(W / 2, 24 * mm, f'kongnamul market · {cert_code}')

    c.showPage()
    c.save()
    buf.seek(0)
    return buf.read()


def send_email(to_email, subject, body):
    """
    SMTP_HOST/SMTP_USER/SMTP_PASS 환경변수가 설정되어 있으면 실제로 이메일을 발송합니다.
    설정되어 있지 않으면 DEV_MODE로 서버 콘솔에만 출력합니다 (로컬 테스트용).
    반환값: (sent: bool, mode: 'sent'|'dev_mode'|'error')
    """
    if DEV_MODE:
        print(f'[DEV MODE] {to_email} 로 보낼 메일 [{subject}]: {body}')
        return False, 'dev_mode'
    try:
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = SMTP_USER
        msg['To'] = to_email
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_USER, [to_email], msg.as_string())
        return True, 'sent'
    except Exception as e:
        print('이메일 발송 실패:', e)
        return False, 'error'


SMS_API_KEY = os.environ.get('SMS_API_KEY')  # 알리고/NHN Cloud 등 실제 SMS 게이트웨이 키. 없으면 DEV MODE.

def send_sms(phone, message):
    """
    실제 SMS 발송에는 알리고(Aligo), NHN Cloud SENS, 네이버클라우드 등 유료 게이트웨이 계약이 필요해요.
    SMS_API_KEY가 설정되어 있지 않으면 DEV MODE로 서버 콘솔에만 출력합니다 (로컬 테스트용).
    실제 게이트웨이 연동 시 이 함수 안의 TODO 부분만 해당 업체 API 호출로 바꾸면 돼요.
    """
    if not SMS_API_KEY:
        print(f'[DEV MODE] {phone} 로 보낼 SMS: {message}')
        return False, 'dev_mode'
    try:
        # TODO: 실제 SMS 게이트웨이(예: 알리고) API 호출로 교체하세요.
        # resp = requests.post('https://apis.aligo.in/send/', data={...}, timeout=10)
        # return resp.status_code == 200, 'sent'
        print(f'[SMS_API_KEY 설정됨이지만 실제 게이트웨이 연동 코드는 비어있어요] {phone}: {message}')
        return False, 'not_implemented'
    except Exception as e:
        print('SMS 발송 실패:', e)
        return False, 'error'


def send_verification_email(to_email, code):
    return send_email(to_email, '[콩나물] 이메일 인증코드', f'콩나물 인증코드: {code}\n{CODE_TTL_MINUTES}분 이내에 입력해주세요.')


def generate_code():
    return f'{secrets.randbelow(1000000):06d}'


def current_user_id():
    return session.get('user_id')


def login_required(fn):
    from functools import wraps
    @wraps(fn)
    def wrapper(*args, **kwargs):
        uid = current_user_id()
        if not uid:
            return jsonify({'error': '로그인이 필요해요'}), 401
        conn = get_db()
        row = conn.execute('SELECT is_suspended FROM users WHERE id=?', (uid,)).fetchone()
        conn.close()
        if not row:
            session.pop('user_id', None)
            return jsonify({'error': '로그인이 필요해요'}), 401
        if row['is_suspended']:
            session.pop('user_id', None)
            return jsonify({'error': '계정이 정지되었어요. 관리자에게 문의해주세요'}), 403
        return fn(*args, **kwargs)
    return wrapper


# ---------------- 보안 유틸 ----------------

# 앱 레벨 요청 제한 (in-memory). 진짜 디도스는 네트워크 경계(Cloudflare 등 CDN/WAF)에서
# 막아야 하고, 이건 그 앞단이 뚫렸을 때의 2차 방어선 + 브루트포스 방지용입니다.
# 여러 프로세스로 운영하면 프로세스마다 카운터가 따로 놀기 때문에, 트래픽이 커지면
# Redis 기반(Flask-Limiter + Redis) 등으로 교체하세요.
_RATE_BUCKETS = {}
_RATE_LOCK = threading.Lock()

def rate_limit(max_calls, window_seconds):
    def deco(fn):
        from functools import wraps
        @wraps(fn)
        def wrapper(*args, **kwargs):
            key = f'{fn.__name__}:{request.remote_addr}'
            now = time.time()
            with _RATE_LOCK:
                bucket = _RATE_BUCKETS.setdefault(key, [])
                bucket[:] = [t for t in bucket if now - t < window_seconds]
                if len(bucket) >= max_calls:
                    return jsonify({'error': '요청이 너무 잦아요. 잠시 후 다시 시도해주세요'}), 429
                bucket.append(now)
            return fn(*args, **kwargs)
        return wrapper
    return deco


LOGIN_MAX_FAILURES = 5
LOGIN_LOCK_MINUTES = 15

def check_login_lock(email):
    conn = get_db()
    row = conn.execute('SELECT * FROM login_failures WHERE email=?', (email,)).fetchone()
    conn.close()
    if row and row['locked_until']:
        if datetime.now(timezone.utc) < datetime.fromisoformat(row['locked_until']):
            return True
    return False

def record_login_failure(email):
    conn = get_db(); c = conn.cursor()
    row = c.execute('SELECT * FROM login_failures WHERE email=?', (email,)).fetchone()
    count = (row['count'] if row else 0) + 1
    locked_until = None
    if count >= LOGIN_MAX_FAILURES:
        locked_until = (datetime.now(timezone.utc) + timedelta(minutes=LOGIN_LOCK_MINUTES)).isoformat()
        count = 0  # 잠금 후 카운트 리셋
    if row:
        c.execute('UPDATE login_failures SET count=?, locked_until=? WHERE email=?', (count, locked_until, email))
    else:
        c.execute('INSERT INTO login_failures(email,count,locked_until) VALUES (?,?,?)', (email, count, locked_until))
    conn.commit(); conn.close()

def clear_login_failures(email):
    conn = get_db()
    conn.execute('DELETE FROM login_failures WHERE email=?', (email,))
    conn.commit(); conn.close()


def issue_csrf_token():
    token = secrets.token_hex(16)
    session['csrf_token'] = token
    return token

def csrf_protect(fn):
    from functools import wraps
    @wraps(fn)
    def wrapper(*args, **kwargs):
        token = request.headers.get('X-CSRF-Token')
        if not token or token != session.get('csrf_token'):
            return jsonify({'error': 'CSRF 토큰이 유효하지 않아요. 새로고침 후 다시 시도해주세요'}), 403
        return fn(*args, **kwargs)
    return wrapper


def write_audit(actor_type, actor, action, detail=''):
    conn = get_db()
    conn.execute('INSERT INTO audit_log(actor_type,actor,action,detail,ip,ts) VALUES (?,?,?,?,?,?)',
                 (actor_type, actor, action, detail, request.remote_addr, now_iso()))
    conn.commit(); conn.close()


# ---------------- 실시간 알림 (Server-Sent Events) ----------------
# 이 샌드박스에는 Flask-SocketIO/eventlet 같은 웹소켓 라이브러리를 설치할 수 없어서(외부 네트워크 차단),
# 순수 Flask만으로 되는 SSE(Server-Sent Events)로 "실시간 알림"을 구현했습니다.
# 웹소켓과 달리 서버->클라이언트 단방향이지만, 알림 용도로는 충분하고 별도 라이브러리가 필요 없어요.
_sse_subscribers = {}       # user_id -> [queue.Queue, ...]
_sse_lock = threading.Lock()

def sse_subscribe(user_id):
    q = queue.Queue()
    with _sse_lock:
        _sse_subscribers.setdefault(user_id, []).append(q)
    return q

def sse_unsubscribe(user_id, q):
    with _sse_lock:
        if user_id in _sse_subscribers:
            try:
                _sse_subscribers[user_id].remove(q)
            except ValueError:
                pass
            if not _sse_subscribers[user_id]:
                del _sse_subscribers[user_id]

def push_notification(user_id, event_type, message, extra=None):
    payload = {'type': event_type, 'message': message}
    if extra:
        payload.update(extra)
    with _sse_lock:
        for q in _sse_subscribers.get(user_id, []):
            q.put(payload)

def broadcast_notification(event_type, message, extra=None):
    payload = {'type': event_type, 'message': message}
    if extra:
        payload.update(extra)
    with _sse_lock:
        for qs in _sse_subscribers.values():
            for q in qs:
                q.put(payload)


# 관리자용 SSE (보통 관리자는 1~2명이라 별도의 단순 리스트로 관리해요)
_admin_sse_subscribers = []
_admin_sse_lock = threading.Lock()

def admin_sse_subscribe():
    q = queue.Queue()
    with _admin_sse_lock:
        _admin_sse_subscribers.append(q)
    return q

def admin_sse_unsubscribe(q):
    with _admin_sse_lock:
        if q in _admin_sse_subscribers:
            _admin_sse_subscribers.remove(q)

def push_admin_notification(event_type, message, extra=None):
    payload = {'type': event_type, 'message': message}
    if extra:
        payload.update(extra)
    with _admin_sse_lock:
        for q in _admin_sse_subscribers:
            q.put(payload)


def notify_admin_emails(subject, body):
    """관리자 대시보드 '사업자정보' 탭에서 설정한 admin_notify_emails로 운영 알림 이메일을 보내요.
    비워두면 아무 것도 하지 않아요 (SSE 알림은 별개로 계속 동작해요)."""
    conn = get_db()
    row = conn.execute("SELECT value FROM business_info WHERE key='admin_notify_emails'").fetchone()
    conn.close()
    if not row or not row['value']:
        return
    for addr in [a.strip() for a in row['value'].split(',') if a.strip()]:
        send_email(addr, subject, body)


def notify_wishlist_if_restocked(c, model_key, grade, was_zero):
    """재고가 0 -> 양수로 바뀌었을 때만 찜한 유저들에게 알려요."""
    if not was_zero:
        return
    for w in c.execute('SELECT * FROM wishlist WHERE model_key=? AND grade=?', (model_key, grade)).fetchall():
        push_notification(w['user_id'], 'restock', f'찜하신 {model_key} {grade}급 재고가 들어왔어요! 지금 확인해보세요.')


def current_admin_id():
    return session.get('admin_id')

def admin_required(fn):
    from functools import wraps
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not current_admin_id():
            return jsonify({'error': '관리자 로그인이 필요해요'}), 401
        return fn(*args, **kwargs)
    return wrapper


LOYALTY_TIERS = [  # (최소 거래횟수, 등급명, 스프레드 할인율)
    (30, '플래티넘', 0.015),
    (15, '골드', 0.010),
    (5, '실버', 0.005),
    (0, '브론즈', 0.0),
]
PLUS_DISCOUNT = 0.005       # 플러스 회원 추가 스프레드 할인폭 (등급 할인에 더해짐)
PLUS_PERIOD_DAYS = 30

def get_plus_status(uid):
    conn = get_db()
    row = conn.execute('SELECT plus_active, plus_expires_at, plus_auto_renew FROM users WHERE id=?', (uid,)).fetchone()
    conn.close()
    if not row:
        return {'active': False, 'expires_at': None, 'auto_renew': True}
    active = bool(row['plus_active']) and bool(row['plus_expires_at']) and \
             datetime.fromisoformat(row['plus_expires_at']) > datetime.now(timezone.utc)
    return {'active': active, 'expires_at': row['plus_expires_at'], 'auto_renew': bool(row['plus_auto_renew'])}

def get_user_tier(uid):
    conn = get_db()
    row = conn.execute('SELECT COUNT(*) c FROM trades WHERE user_id=?', (uid,)).fetchone()
    conn.close()
    count = row['c'] if row else 0
    plus = get_plus_status(uid)
    plus_bonus = PLUS_DISCOUNT if plus['active'] else 0.0
    for threshold, name, discount in LOYALTY_TIERS:
        if count >= threshold:
            next_tier = None
            idx = LOYALTY_TIERS.index((threshold, name, discount))
            if idx > 0:
                nt, nn, nd = LOYALTY_TIERS[idx-1]
                next_tier = {'name': nn, 'need': nt - count}
            return {'tier': name, 'trade_count': count, 'discount': discount + plus_bonus, 'next_tier': next_tier, 'plus': plus}
    return {'tier': '브론즈', 'trade_count': count, 'discount': plus_bonus, 'next_tier': None, 'plus': plus}


def price_for(model_row, grade, side, discount=0.0):
    ratio = {'S': model_row['ratio_s'], 'A': model_row['ratio_a'],
             'B': model_row['ratio_b'], 'C': model_row['ratio_c']}[grade]
    base = model_row['mid'] * ratio
    if side == 'buy':   # 우리가 매입
        spread = max(0, get_setting('buy_spread') - discount)
        return round(base * (1 - spread) / 500) * 500
    markup = max(0, get_setting('sell_markup') - discount)
    return round(base * (1 + markup) / 500) * 500  # 우리가 판매


def external_weight(internal_trade_count):
    return max(0.05, min(0.35, 0.35 - internal_trade_count * 0.02))


def do_tick():
    global LAST_TICK
    conn = get_db()
    c = conn.cursor()
    models = c.execute('SELECT * FROM models').fetchall()
    for m in models:
        key = m['key']
        pend = c.execute('SELECT * FROM trades WHERE model_key=? AND pending=1', (key,)).fetchall()
        ref = c.execute('SELECT * FROM external_ref WHERE model_key=?', (key,)).fetchone()
        w = external_weight(m['internal_trade_count'])
        if pend:
            implied = []
            sell_markup = get_setting('sell_markup')
            buy_spread = get_setting('buy_spread')
            for t in pend:
                ratio = {'S': m['ratio_s'], 'A': m['ratio_a'], 'B': m['ratio_b'], 'C': m['ratio_c']}[t['grade']]
                back = t['price'] / (1 + sell_markup) if t['side'] == 'sell_to_user' else t['price'] / (1 - buy_spread)
                implied.append(back / ratio)
            avg_implied = sum(implied) / len(implied)
            internal_blend = m['mid'] * 0.55 + avg_implied * 0.45
            new_mid = internal_blend * (1 - w * 0.4) + ref['avg'] * (w * 0.4)
        else:
            new_mid = m['mid'] * (1 - w) + ref['avg'] * w + m['mid'] * random.uniform(-0.003, 0.003)
        new_mid = max(m['floor_p'], min(m['ceil_p'], new_mid))
        c.execute('UPDATE models SET mid=? WHERE key=?', (round(new_mid), key))
        c.execute('INSERT INTO history(model_key,mid,ts) VALUES (?,?,?)', (key, round(new_mid), now_iso()))
        c.execute('UPDATE trades SET pending=0 WHERE model_key=? AND pending=1', (key,))

        # 가격 알림 체크: 목표가 이하로 내려간 미발송 알림을 찾아서 보내요
        updated_model = dict(m); updated_model['mid'] = round(new_mid)
        for alert in c.execute(
            'SELECT a.*, u.email FROM price_alerts a JOIN users u ON a.user_id=u.id WHERE a.model_key=? AND a.triggered=0',
            (key,)).fetchall():
            current_price = price_for(updated_model, alert['grade'], 'sell')
            if current_price <= alert['target_price']:
                push_notification(alert['user_id'], 'price_alert',
                    f'{updated_model["name"]} {alert["grade"]}급이 목표가 {int(alert["target_price"]):,}원 이하로 내려갔어요! (현재 {int(current_price):,}원)')
                c.execute('UPDATE price_alerts SET triggered=1 WHERE id=?', (alert['id'],))
        ids = [r['id'] for r in c.execute(
            'SELECT id FROM history WHERE model_key=? ORDER BY id DESC', (key,)).fetchall()]
        if len(ids) > 20:
            c.executemany('DELETE FROM history WHERE id=?', [(i,) for i in ids[20:]])
    stats = c.execute('SELECT * FROM stats WHERE id=1').fetchone()
    c.execute('UPDATE stats SET total_inspected=? WHERE id=1',
              (stats['total_inspected'] + random.randint(0, 2),))
    if random.random() < 0.4:
        key = random.choice(list(MODEL_DEFAULTS.keys()))
        grade = random.choice(GRADES)
        prev = c.execute('SELECT qty FROM stock WHERE model_key=? AND grade=?', (key, grade)).fetchone()
        c.execute('UPDATE stock SET qty=qty+1 WHERE model_key=? AND grade=?', (key, grade))
        notify_wishlist_if_restocked(c, key, grade, prev and prev['qty'] == 0)

    # 보관료가 물건 가치를 위협할 만큼 쌓인 사용자에게 알림 (레벨 1: 70%↑, 레벨 2: 100%↑)
    for item in c.execute('SELECT p.*, u.email FROM portfolio p JOIN users u ON p.user_id=u.id').fetchall():
        fee = calc_storage_fee(item['ts'], get_plus_status(item['user_id'])['active'])
        if fee <= 0:
            continue
        model_row = c.execute('SELECT * FROM models WHERE key=?', (item['model_key'],)).fetchone()
        if not model_row:
            continue
        value = price_for(model_row, item['grade'], 'buy')
        ratio = (fee / value) if value > 0 else 0
        level = 2 if ratio >= 1.0 else (1 if ratio >= 0.7 else 0)
        if level > 0 and level > (item['risk_notified'] or 0):
            subject = '[콩나물] 보관료가 물건 가치를 넘었어요' if level == 2 else '[콩나물] 보관 중인 물건을 확인해주세요'
            body = (f'{item["model_key"]} {item["grade"]}급 보관 아이템의 누적 보관료가 {int(fee):,}원이에요.\n'
                    f'현재 매도가({int(value):,}원) 대비 {int(ratio*100)}% 수준이라, 계속 두면 매도해도 남는 돈이 '
                    f'{"거의 없거나 오히려 손해" if level==2 else "많이 줄어들"} 수 있어요.\n'
                    f'지금 매도하거나 배송 신청을 해서 손해를 막아주세요.')
            send_email(item['email'], subject, body)
            c.execute('UPDATE portfolio SET risk_notified=? WHERE id=?', (level, item['id']))
            push_admin_notification('at_risk', f'매각위기: {item["email"]}님의 {item["model_key"]} {item["grade"]}급 (보관료 {int(ratio*100)}%)')

    # 콩나물 플러스 자동갱신/만료 처리
    now_dt = datetime.now(timezone.utc)
    plus_fee = get_setting('plus_monthly_fee')
    for u in c.execute('SELECT id, email, plus_expires_at, plus_auto_renew FROM users WHERE plus_active=1').fetchall():
        if not u['plus_expires_at'] or datetime.fromisoformat(u['plus_expires_at']) > now_dt:
            continue  # 아직 만료 안 됨
        if u['plus_auto_renew']:
            wrow = c.execute('SELECT balance FROM wallet WHERE user_id=?', (u['id'],)).fetchone()
            if wrow and wrow['balance'] >= plus_fee:
                c.execute('UPDATE wallet SET balance=balance-? WHERE user_id=?', (plus_fee, u['id']))
                new_expiry = now_dt + timedelta(days=PLUS_PERIOD_DAYS)
                c.execute('UPDATE users SET plus_expires_at=? WHERE id=?', (new_expiry.isoformat(), u['id']))
                c.execute('INSERT INTO revenue_events(type,amount,user_id,ts) VALUES (?,?,?,?)',
                           ('plus_subscription', plus_fee, u['id'], now_iso()))
                push_notification(u['id'], 'plus_renewed', f'콩나물 플러스가 자동 갱신됐어요 ({int(plus_fee):,}원 차감)', {'wallet_changed': True})
                continue
        # 자동갱신 꺼져있거나 잔액 부족 -> 만료 처리
        c.execute('UPDATE users SET plus_active=0 WHERE id=?', (u['id'],))
        push_notification(u['id'], 'plus_expired', '콩나물 플러스 구독이 만료됐어요. 계속 이용하려면 다시 구독해주세요.')
        send_email(u['email'], '[콩나물] 플러스 구독이 만료됐어요', '자동갱신이 꺼져있거나 잔액이 부족해서 콩나물 플러스가 만료됐어요. 다시 구독하시려면 사이트에서 구독하기를 눌러주세요.')

    # 매주 일요일(한국시간) 출금 일괄정산 리마인더 - 하루에 한 번만 보내요
    global _last_settlement_reminder_date
    kst_today = (datetime.now(timezone.utc) + timedelta(hours=9)).date()
    if kst_today.weekday() == 6 and _last_settlement_reminder_date != kst_today.isoformat():  # 6=일요일
        pending = c.execute("SELECT COUNT(*) c, COALESCE(SUM(net_amount),0) s FROM withdrawal_requests WHERE status='pending'").fetchone()
        if pending['c'] > 0:
            push_admin_notification('settlement_day',
                f'📅 오늘은 출금 정산일이에요! 대기중인 출금 {pending["c"]}건, 총 {int(pending["s"]):,}원')
            notify_admin_emails(
                '[콩나물] 오늘은 출금 정산일이에요',
                f'대기중인 출금 요청이 {pending["c"]}건, 총 {int(pending["s"]):,}원 있어요.\n'
                f'은행 대량이체로 실제 송금을 처리하신 뒤, 관리자 대시보드 "출금관리" 탭에서 '
                f'"일괄 정산 처리" 버튼을 눌러주세요.'
            )
        _last_settlement_reminder_date = kst_today.isoformat()

    conn.commit()
    conn.close()
    LAST_TICK = time.time()


def scheduler_loop():
    while True:
        time.sleep(max(5, get_setting('tick_seconds')))  # 최소 5초, 매 루프 최신 설정값 반영
        try:
            do_tick()
        except Exception as e:
            print('tick error:', e)


# ---------------- API ----------------

@app.route('/api/state')
def api_state():
    conn = get_db()
    c = conn.cursor()
    uid = current_user_id()
    my_discount = get_user_tier(uid)['discount'] if uid else 0.0
    my_plus_active = get_plus_status(uid)['active'] if uid else False
    models = {}
    for m in c.execute('SELECT * FROM models').fetchall():
        hist = [r['mid'] for r in c.execute(
            'SELECT mid FROM history WHERE model_key=? ORDER BY id ASC', (m['key'],)).fetchall()]
        stock = {r['grade']: r['qty'] for r in c.execute(
            'SELECT grade,qty FROM stock WHERE model_key=?', (m['key'],)).fetchall()}
        ref = c.execute('SELECT * FROM external_ref WHERE model_key=?', (m['key'],)).fetchone()
        prices = {g: {'buy': price_for(m, g, 'buy', my_discount), 'sell': price_for(m, g, 'sell', my_discount)} for g in GRADES}
        models[m['key']] = {
            'code': m['code'], 'name': m['name'], 'mid': m['mid'], 'history': hist,
            'stock': stock, 'prices': prices,
            'external_ref': {'avg': ref['avg'], 'note': ref['note'], 'updated_at': ref['updated_at']} if ref else {'avg': m['mid'], 'note': '', 'updated_at': ''},
            'internal_weight_pct': round((1 - external_weight(m['internal_trade_count'])) * 100),
            'drop_start': m['drop_start'], 'drop_end': m['drop_end'], 'is_active': bool(m['is_active']),
        }
    feed = [dict(r) for r in c.execute(
        'SELECT model_key,grade,side,price,label,ts FROM trades ORDER BY id DESC LIMIT 10').fetchall()]

    user_info = None
    wallet_balance = None
    portfolio = []
    csrf_token = None
    if uid:
        urow = c.execute('SELECT email, is_suspended, phone_number, phone_verified FROM users WHERE id=?', (uid,)).fetchone()
        if urow and urow['is_suspended']:
            session.pop('user_id', None)
            urow = None
        if urow:
            user_info = {'email': urow['email'], 'tier': get_user_tier(uid),
                         'phone_number': urow['phone_number'], 'phone_verified': bool(urow['phone_verified'])}
            csrf_token = session.get('csrf_token') or issue_csrf_token()
            wrow = c.execute('SELECT balance FROM wallet WHERE user_id=?', (uid,)).fetchone()
            wallet_balance = wrow['balance'] if wrow else 0
            for r in c.execute('SELECT * FROM portfolio WHERE user_id=? ORDER BY ts DESC', (uid,)).fetchall():
                item = dict(r)
                item['storage_fee'] = calc_storage_fee(item['ts'], my_plus_active)
                stored_at = datetime.fromisoformat(item['ts'])
                if stored_at.tzinfo is None:
                    stored_at = stored_at.replace(tzinfo=timezone.utc)
                item['days_stored'] = int((datetime.now(timezone.utc) - stored_at).total_seconds() // 86400)
                current_value = models.get(item['model_key'], {}).get('prices', {}).get(item['grade'], {}).get('buy', 0)
                ratio = (item['storage_fee'] / current_value) if current_value > 0 else 0
                item['risk_level'] = 2 if ratio >= 1.0 else (1 if ratio >= 0.7 else 0)
                item['current_value'] = current_value
                portfolio.append(item)

    stats = dict(c.execute('SELECT * FROM stats WHERE id=1').fetchone())
    conn.close()
    tick_seconds = get_setting('tick_seconds')
    remaining = max(0, round(tick_seconds - (time.time() - LAST_TICK)))
    return jsonify({
        'models': models, 'feed': feed, 'portfolio': portfolio,
        'user': user_info, 'wallet': wallet_balance, 'stats': stats, 'csrf_token': csrf_token,
        'tick_seconds': tick_seconds, 'seconds_to_next_tick': remaining,
        'fees': {
            'storage_free_days': get_setting('storage_free_days'),
            'storage_fee_per_month': get_setting('storage_fee_per_month'),
            'delivery_base_fee': get_setting('delivery_base_fee'),
            'remote_surcharge': get_setting('remote_surcharge'),
            'instant_withdraw_fee': get_setting('instant_withdraw_fee'),
            'plus_monthly_fee': get_setting('plus_monthly_fee'),
            'fast_track_fee': get_setting('fast_track_fee'),
            'certificate_fee': get_setting('certificate_fee'),
            'gift_service_fee': get_setting('gift_service_fee'),
            'card_payment_enabled': bool(get_setting('card_payment_enabled')),
            'instant_withdraw_enabled': bool(get_setting('instant_withdraw_enabled')),
            'next_settlement_date': next_settlement_date(),
        },
    })


# ---------------- 인증 (회원가입/이메일 인증코드/로그인) ----------------

@app.route('/api/auth/send-code', methods=['POST'])
@rate_limit(5, 300)  # 같은 IP에서 5분에 5회까지
def api_send_code():
    data = request.get_json(force=True)
    email = (data.get('email') or '').strip().lower()
    if not EMAIL_RE.match(email):
        return jsonify({'error': '올바른 이메일 형식이 아니에요'}), 400

    conn = get_db(); c = conn.cursor()
    existing = c.execute('SELECT id FROM users WHERE email=?', (email,)).fetchone()
    if existing:
        conn.close()
        return jsonify({'error': '이미 가입된 이메일이에요'}), 400

    last = c.execute(
        'SELECT created_at FROM verification_codes WHERE email=? AND purpose=? ORDER BY id DESC LIMIT 1',
        (email, 'signup')).fetchone()
    if last:
        elapsed = (datetime.now(timezone.utc) - datetime.fromisoformat(last['created_at'])).total_seconds()
        if elapsed < CODE_RESEND_COOLDOWN_SECONDS:
            conn.close()
            return jsonify({'error': f'{int(CODE_RESEND_COOLDOWN_SECONDS-elapsed)}초 후 다시 시도해주세요'}), 429

    code = generate_code()
    created = datetime.now(timezone.utc)
    expires = created + timedelta(minutes=CODE_TTL_MINUTES)
    c.execute('INSERT INTO verification_codes(email,code,purpose,created_at,expires_at,attempts,verified) VALUES (?,?,?,?,?,0,0)',
              (email, code, 'signup', created.isoformat(), expires.isoformat()))
    conn.commit(); conn.close()

    sent, mode = send_verification_email(email, code)
    resp = {'ok': True, 'mode': mode, 'expires_in_minutes': CODE_TTL_MINUTES}
    if mode == 'dev_mode':
        resp['dev_code'] = code  # 로컬 테스트 편의용. 실제 운영에서는 SMTP 설정 시 이 필드가 사라집니다.
    return jsonify(resp)


@app.route('/api/auth/verify-code', methods=['POST'])
def api_verify_code():
    data = request.get_json(force=True)
    email = (data.get('email') or '').strip().lower()
    code = (data.get('code') or '').strip()
    conn = get_db(); c = conn.cursor()
    row = c.execute(
        'SELECT * FROM verification_codes WHERE email=? AND purpose=? ORDER BY id DESC LIMIT 1',
        (email, 'signup')).fetchone()
    if not row:
        conn.close(); return jsonify({'error': '발급된 인증코드가 없어요'}), 400
    if row['verified']:
        conn.close(); return jsonify({'ok': True, 'already_verified': True})
    if datetime.now(timezone.utc) > datetime.fromisoformat(row['expires_at']):
        conn.close(); return jsonify({'error': '인증코드가 만료됐어요. 다시 받아주세요'}), 400
    if row['attempts'] >= CODE_MAX_ATTEMPTS:
        conn.close(); return jsonify({'error': '시도 횟수를 초과했어요. 코드를 다시 받아주세요'}), 400
    if row['code'] != code:
        c.execute('UPDATE verification_codes SET attempts=attempts+1 WHERE id=?', (row['id'],))
        conn.commit(); conn.close()
        remaining = CODE_MAX_ATTEMPTS - (row['attempts'] + 1)
        return jsonify({'error': f'코드가 일치하지 않아요 (남은 시도 {remaining}회)'}), 400
    c.execute('UPDATE verification_codes SET verified=1 WHERE id=?', (row['id'],))
    conn.commit(); conn.close()
    return jsonify({'ok': True})


@app.route('/api/auth/signup', methods=['POST'])
def api_signup():
    data = request.get_json(force=True)
    email = (data.get('email') or '').strip().lower()
    password = data.get('password') or ''
    if not EMAIL_RE.match(email):
        return jsonify({'error': '올바른 이메일 형식이 아니에요'}), 400
    if len(password) < 8:
        return jsonify({'error': '비밀번호는 8자 이상이어야 해요'}), 400

    conn = get_db(); c = conn.cursor()
    verified_row = c.execute(
        'SELECT * FROM verification_codes WHERE email=? AND purpose=? AND verified=1 ORDER BY id DESC LIMIT 1',
        (email, 'signup')).fetchone()
    if not verified_row:
        conn.close(); return jsonify({'error': '이메일 인증을 먼저 완료해주세요'}), 400
    if datetime.now(timezone.utc) > datetime.fromisoformat(verified_row['expires_at']):
        conn.close(); return jsonify({'error': '인증이 만료됐어요. 코드를 다시 받아주세요'}), 400

    existing = c.execute('SELECT id FROM users WHERE email=?', (email,)).fetchone()
    if existing:
        conn.close(); return jsonify({'error': '이미 가입된 이메일이에요'}), 400

    pw_hash = generate_password_hash(password)
    c.execute('INSERT INTO users(email,password_hash,created_at) VALUES (?,?,?)',
              (email, pw_hash, now_iso()))
    user_id = c.lastrowid
    c.execute('INSERT INTO wallet(user_id,balance) VALUES (?,?)', (user_id, 0))

    # 가입 전에 받은 선물이 있으면 자동으로 보관함에 넣어줘요
    claimed_count = 0
    for g in c.execute("SELECT * FROM gifts WHERE to_email=? AND status='pending'", (email,)).fetchall():
        item_id = 'itm-' + uuid.uuid4().hex[:8]
        c.execute('INSERT INTO portfolio(id,user_id,model_key,grade,bought_price,ts) VALUES (?,?,?,?,?,?)',
                   (item_id, user_id, g['model_key'], g['grade'], g['price'], now_iso()))
        c.execute("UPDATE gifts SET status='claimed', claimed_at=?, portfolio_item_id=? WHERE id=?",
                   (now_iso(), item_id, g['id']))
        claimed_count += 1
    conn.commit(); conn.close()

    session['user_id'] = user_id
    token = issue_csrf_token()
    write_audit('user', email, 'signup')
    resp = {'ok': True, 'user': {'email': email}, 'csrf_token': token}
    if claimed_count:
        resp['claimed_gifts'] = claimed_count
    return jsonify(resp)


@app.route('/api/auth/login', methods=['POST'])
@rate_limit(10, 300)  # 같은 IP에서 5분에 10회까지 (계정별 잠금과 별개의 IP 단위 방어)
def api_login():
    data = request.get_json(force=True)
    email = (data.get('email') or '').strip().lower()
    password = data.get('password') or ''
    if check_login_lock(email):
        return jsonify({'error': f'로그인 시도가 너무 많아 {LOGIN_LOCK_MINUTES}분간 잠겼어요'}), 429
    conn = get_db(); c = conn.cursor()
    user = c.execute('SELECT * FROM users WHERE email=?', (email,)).fetchone()
    conn.close()
    if not user or not check_password_hash(user['password_hash'], password):
        record_login_failure(email)
        write_audit('user', email, 'login_failed')
        return jsonify({'error': '이메일 또는 비밀번호가 올바르지 않아요'}), 401
    if user['is_suspended']:
        write_audit('user', email, 'login_blocked_suspended')
        return jsonify({'error': '정지된 계정이에요. 관리자에게 문의해주세요'}), 403
    clear_login_failures(email)
    session['user_id'] = user['id']
    token = issue_csrf_token()
    write_audit('user', email, 'login')
    return jsonify({'ok': True, 'user': {'email': user['email']}, 'csrf_token': token})


@app.route('/api/auth/logout', methods=['POST'])
def api_logout():
    session.pop('user_id', None)
    return jsonify({'ok': True})


@app.route('/api/auth/me')
def api_me():
    uid = current_user_id()
    if not uid:
        return jsonify({'user': None})
    conn = get_db()
    row = conn.execute('SELECT email FROM users WHERE id=?', (uid,)).fetchone()
    conn.close()
    return jsonify({'user': {'email': row['email']} if row else None})


PIN_RE = re.compile(r'^\d{6}$')

def verify_pin(uid, pin):
    """유저가 PIN을 설정해뒀다면 맞는지 확인. 설정 안 했으면 통과(하위호환)."""
    conn = get_db()
    row = conn.execute('SELECT payment_pin_hash FROM users WHERE id=?', (uid,)).fetchone()
    conn.close()
    if not row or not row['payment_pin_hash']:
        return True, None
    if not pin or not check_password_hash(row['payment_pin_hash'], pin):
        return False, '결제 비밀번호가 올바르지 않아요'
    return True, None


@app.route('/api/auth/pin-status')
@login_required
def api_pin_status():
    uid = current_user_id()
    conn = get_db()
    row = conn.execute('SELECT payment_pin_hash FROM users WHERE id=?', (uid,)).fetchone()
    conn.close()
    return jsonify({'has_pin': bool(row and row['payment_pin_hash'])})


@app.route('/api/stream')
def api_stream():
    uid = current_user_id()
    if not uid:
        return jsonify({'error': '로그인이 필요해요'}), 401
    q = sse_subscribe(uid)

    @stream_with_context
    def gen():
        try:
            yield 'retry: 3000\ndata: {"type":"connected"}\n\n'
            while True:
                try:
                    payload = q.get(timeout=15)
                    yield f'data: {json.dumps(payload, ensure_ascii=False)}\n\n'
                except queue.Empty:
                    yield ': keepalive\n\n'
        finally:
            sse_unsubscribe(uid, q)

    return Response(gen(), mimetype='text/event-stream', headers={
        'Cache-Control': 'no-cache',
        'X-Accel-Buffering': 'no',
        'Connection': 'keep-alive',
    })


@app.route('/api/admin/stream')
def api_admin_stream():
    if not current_admin_id():
        return jsonify({'error': '관리자 로그인이 필요해요'}), 401
    q = admin_sse_subscribe()

    @stream_with_context
    def gen():
        try:
            yield 'retry: 3000\ndata: {"type":"connected"}\n\n'
            while True:
                try:
                    payload = q.get(timeout=15)
                    yield f'data: {json.dumps(payload, ensure_ascii=False)}\n\n'
                except queue.Empty:
                    yield ': keepalive\n\n'
        finally:
            admin_sse_unsubscribe(q)

    return Response(gen(), mimetype='text/event-stream', headers={
        'Cache-Control': 'no-cache',
        'X-Accel-Buffering': 'no',
        'Connection': 'keep-alive',
    })


@app.route('/api/auth/set-pin', methods=['POST'])
@login_required
@csrf_protect
def api_set_pin():
    uid = current_user_id()
    data = request.get_json(force=True)
    pin = (data.get('pin') or '').strip()
    if not PIN_RE.match(pin):
        return jsonify({'error': '결제 비밀번호는 숫자 6자리여야 해요'}), 400
    conn = get_db(); c = conn.cursor()
    c.execute('UPDATE users SET payment_pin_hash=? WHERE id=?', (generate_password_hash(pin), uid))
    conn.commit(); conn.close()
    write_audit('user', str(uid), 'set_payment_pin')
    return jsonify({'ok': True})


PHONE_RE = re.compile(r'^01[0-9]{8,9}$')

@app.route('/api/auth/send-phone-code', methods=['POST'])
@login_required
@csrf_protect
@rate_limit(5, 300)
def api_send_phone_code():
    uid = current_user_id()
    data = request.get_json(force=True)
    phone = re.sub(r'[^0-9]', '', data.get('phone') or '')
    if not PHONE_RE.match(phone):
        return jsonify({'error': "올바른 휴대폰 번호가 아니에요 (예: 01012345678)"}), 400
    conn = get_db(); c = conn.cursor()
    last = c.execute(
        'SELECT created_at FROM verification_codes WHERE email=? AND purpose=? ORDER BY id DESC LIMIT 1',
        (phone, 'phone_verify')).fetchone()
    if last:
        elapsed = (datetime.now(timezone.utc) - datetime.fromisoformat(last['created_at'])).total_seconds()
        if elapsed < CODE_RESEND_COOLDOWN_SECONDS:
            conn.close(); return jsonify({'error': f'{int(CODE_RESEND_COOLDOWN_SECONDS-elapsed)}초 후 다시 시도해주세요'}), 429
    code = generate_code()
    created = datetime.now(timezone.utc)
    expires = created + timedelta(minutes=CODE_TTL_MINUTES)
    c.execute('INSERT INTO verification_codes(email,code,purpose,created_at,expires_at,attempts,verified) VALUES (?,?,?,?,?,0,0)',
              (phone, code, 'phone_verify', created.isoformat(), expires.isoformat()))
    conn.commit(); conn.close()
    sent, mode = send_sms(phone, f'[콩나물] 인증번호 {code} ({CODE_TTL_MINUTES}분 이내 입력)')
    resp = {'ok': True, 'mode': mode}
    if mode in ('dev_mode', 'not_implemented'):
        resp['dev_code'] = code
    return jsonify(resp)


@app.route('/api/auth/verify-phone-code', methods=['POST'])
@login_required
@csrf_protect
def api_verify_phone_code():
    uid = current_user_id()
    data = request.get_json(force=True)
    phone = re.sub(r'[^0-9]', '', data.get('phone') or '')
    code = (data.get('code') or '').strip()
    conn = get_db(); c = conn.cursor()
    row = c.execute(
        'SELECT * FROM verification_codes WHERE email=? AND purpose=? ORDER BY id DESC LIMIT 1',
        (phone, 'phone_verify')).fetchone()
    if not row:
        conn.close(); return jsonify({'error': '발급된 인증코드가 없어요'}), 400
    if datetime.now(timezone.utc) > datetime.fromisoformat(row['expires_at']):
        conn.close(); return jsonify({'error': '인증코드가 만료됐어요'}), 400
    if row['attempts'] >= CODE_MAX_ATTEMPTS:
        conn.close(); return jsonify({'error': '시도 횟수를 초과했어요. 코드를 다시 받아주세요'}), 400
    if row['code'] != code:
        c.execute('UPDATE verification_codes SET attempts=attempts+1 WHERE id=?', (row['id'],))
        conn.commit(); conn.close()
        return jsonify({'error': '코드가 일치하지 않아요'}), 400
    c.execute('UPDATE verification_codes SET verified=1 WHERE id=?', (row['id'],))
    c.execute('UPDATE users SET phone_number=?, phone_verified=1 WHERE id=?', (phone, uid))
    conn.commit(); conn.close()
    write_audit('user', str(uid), 'verify_phone', phone)
    return jsonify({'ok': True})


@app.route('/api/trade', methods=['POST'])
@login_required
@csrf_protect
@rate_limit(15, 10)  # 같은 IP에서 10초에 15회까지 (드랍 순간 연타 방어)
def api_trade():
    uid = current_user_id()
    data = request.get_json(force=True)
    action = data.get('action')
    model_key = data.get('model')
    grade = data.get('grade')
    delivery = data.get('delivery', 'storage')
    region = data.get('region', 'normal')
    conn = get_db()
    c = conn.cursor()
    m = c.execute('SELECT * FROM models WHERE key=?', (model_key,)).fetchone()
    if not m or grade not in GRADES:
        conn.close()
        return jsonify({'error': 'invalid model/grade'}), 400
    ts = now_iso()
    if action == 'buy':
        ok, err = verify_pin(uid, data.get('pin'))
        if not ok:
            conn.close(); return jsonify({'error': err}), 403
        # 드랍(판매 시간창)이 설정되어 있으면 그 시간 안에서만 구매 허용
        if m['drop_start'] or m['drop_end']:
            now = datetime.now(timezone.utc)
            if m['drop_start'] and now < datetime.fromisoformat(m['drop_start']):
                conn.close(); return jsonify({'error': '아직 판매 시작 전이에요', 'drop_start': m['drop_start']}), 400
            if m['drop_end'] and now > datetime.fromisoformat(m['drop_end']):
                conn.close(); return jsonify({'error': '판매 시간이 종료됐어요'}), 400
        price = price_for(m, grade, 'sell', get_user_tier(uid)['discount'])
        delivery_fee = 0
        if delivery == 'delivery' and not get_plus_status(uid)['active']:
            delivery_fee = get_setting('delivery_base_fee') + (get_setting('remote_surcharge') if region == 'remote' else 0)
        total = price + delivery_fee
        wallet_row = c.execute('SELECT balance FROM wallet WHERE user_id=?', (uid,)).fetchone()
        if not wallet_row or wallet_row['balance'] < total:
            conn.close(); return jsonify({'error': '잔액 부족'}), 400
        # 재고 차감은 SELECT 후 UPDATE가 아니라 '조건부 UPDATE' 한 번으로 원자적으로 처리해요.
        # 이래야 드랍 순간 수백 명이 동시에 눌러도 재고가 마이너스로 내려가거나 이중판매되지 않아요.
        c.execute('UPDATE stock SET qty=qty-1 WHERE model_key=? AND grade=? AND qty>0', (model_key, grade))
        if c.rowcount == 0:
            conn.close(); return jsonify({'error': '방금 품절됐어요'}), 400
        remaining = c.execute('SELECT qty FROM stock WHERE model_key=? AND grade=?', (model_key, grade)).fetchone()
        if remaining and remaining['qty'] == 0:
            push_admin_notification('out_of_stock', f'품절: {model_key} {grade}급 재고가 0개가 됐어요')
        c.execute('UPDATE wallet SET balance=balance-? WHERE user_id=? AND balance>=?', (total, uid, total))
        if c.rowcount == 0:
            # 잔액 재확인 실패 시 방금 차감한 재고를 되돌려요 (동시 요청으로 인한 드문 경합 대비)
            c.execute('UPDATE stock SET qty=qty+1 WHERE model_key=? AND grade=?', (model_key, grade))
            conn.commit(); conn.close()
            return jsonify({'error': '잔액 부족'}), 400
        c.execute('INSERT INTO trades(user_id,model_key,grade,side,price,label,ts,pending) VALUES (?,?,?,?,?,?,?,1)',
                   (uid, model_key, grade, 'sell_to_user', price, '구매체결', ts))
        c.execute('UPDATE models SET internal_trade_count=internal_trade_count+1 WHERE key=?', (model_key,))
        c.execute('UPDATE stats SET today_trades=today_trades+1 WHERE id=1')
        if delivery == 'storage':
            item_id = 'itm-' + uuid.uuid4().hex[:8]
            c.execute('INSERT INTO portfolio(id,user_id,model_key,grade,bought_price,ts) VALUES (?,?,?,?,?,?)',
                       (item_id, uid, model_key, grade, price, ts))
        if delivery_fee > 0:
            c.execute('INSERT INTO revenue_events(type,amount,user_id,ts) VALUES (?,?,?,?)',
                       ('delivery_fee', delivery_fee, uid, ts))
        conn.commit(); conn.close()
        return jsonify({'ok': True, 'price': price, 'delivery_fee': delivery_fee, 'total': total, 'delivery': delivery})
    conn.close()
    return jsonify({'error': 'invalid action'}), 400


@app.route('/api/portfolio/<item_id>/sell', methods=['POST'])
@login_required
@csrf_protect
def api_portfolio_sell(item_id):
    uid = current_user_id()
    conn = get_db(); c = conn.cursor()
    item = c.execute('SELECT * FROM portfolio WHERE id=? AND user_id=?', (item_id, uid)).fetchone()
    if not item:
        conn.close(); return jsonify({'error': 'not found'}), 404
    m = c.execute('SELECT * FROM models WHERE key=?', (item['model_key'],)).fetchone()
    price = price_for(m, item['grade'], 'buy', get_user_tier(uid)['discount'])
    storage_fee = calc_storage_fee(item['ts'], get_plus_status(uid)['active'])
    net = max(0, price - storage_fee)
    c.execute('UPDATE wallet SET balance=balance+? WHERE user_id=?', (net, uid))
    c.execute('DELETE FROM portfolio WHERE id=?', (item_id,))
    c.execute('INSERT INTO trades(user_id,model_key,grade,side,price,label,ts,pending) VALUES (?,?,?,?,?,?,?,1)',
               (uid, item['model_key'], item['grade'], 'buy_from_user', price, '보관함 매도', now_iso()))
    c.execute('UPDATE models SET internal_trade_count=internal_trade_count+1 WHERE key=?', (item['model_key'],))
    c.execute('UPDATE stats SET today_trades=today_trades+1 WHERE id=1')
    if storage_fee > 0:
        c.execute('INSERT INTO revenue_events(type,amount,user_id,ts) VALUES (?,?,?,?)',
                   ('storage_fee', storage_fee, uid, now_iso()))
    conn.commit(); conn.close()
    return jsonify({'ok': True, 'price': price, 'storage_fee': storage_fee, 'net': net})


@app.route('/api/portfolio/<item_id>/deliver', methods=['POST'])
@login_required
@csrf_protect
def api_portfolio_deliver(item_id):
    uid = current_user_id()
    data = request.get_json(force=True, silent=True) or {}
    region = data.get('region', 'normal')
    ok, err = verify_pin(uid, data.get('pin'))
    if not ok:
        return jsonify({'error': err}), 403
    conn = get_db(); c = conn.cursor()
    item = c.execute('SELECT * FROM portfolio WHERE id=? AND user_id=?', (item_id, uid)).fetchone()
    if not item:
        conn.close(); return jsonify({'error': 'not found'}), 404
    storage_fee = calc_storage_fee(item['ts'], get_plus_status(uid)['active'])
    delivery_base = 0 if get_plus_status(uid)['active'] else get_setting('delivery_base_fee')
    surcharge = get_setting('remote_surcharge') if region == 'remote' else 0
    total = delivery_base + surcharge + storage_fee
    c.execute('DELETE FROM portfolio WHERE id=?', (item_id,))
    c.execute('UPDATE wallet SET balance=MAX(0,balance-?) WHERE user_id=?', (total, uid))
    if delivery_base + surcharge > 0:
        c.execute('INSERT INTO revenue_events(type,amount,user_id,ts) VALUES (?,?,?,?)',
                   ('delivery_fee', delivery_base + surcharge, uid, now_iso()))
    if storage_fee > 0:
        c.execute('INSERT INTO revenue_events(type,amount,user_id,ts) VALUES (?,?,?,?)',
                   ('storage_fee', storage_fee, uid, now_iso()))
    conn.commit(); conn.close()
    return jsonify({'ok': True, 'delivery_fee': delivery_base, 'surcharge': surcharge,
                     'storage_fee': storage_fee, 'total': total})



@app.route('/api/external_ref', methods=['POST'])
@admin_required
def api_update_external_ref():
    """scraper.py가 수집한 값이나 수동 값을 반영. body 예: {"pro1": {"avg": 95000, "note": "..."}}
    관리자 세션으로 로그인한 뒤 호출해야 합니다 (scraper.py 참고)."""
    data = request.get_json(force=True)
    conn = get_db(); c = conn.cursor()
    for key, payload in data.items():
        c.execute('UPDATE external_ref SET avg=?, note=?, updated_at=? WHERE model_key=?',
                   (payload['avg'], payload.get('note', ''), now_iso(), key))
    conn.commit(); conn.close()
    write_audit('admin', session.get('admin_username', '?'), 'update_external_ref', str(data))
    return jsonify({'ok': True})


@app.route('/api/tick_now', methods=['POST'])
@admin_required
def api_tick_now():
    do_tick()
    write_audit('admin', session.get('admin_username', '?'), 'tick_now')
    return jsonify({'ok': True})


@app.route('/api/reset', methods=['POST'])
@admin_required
def api_reset():
    write_audit('admin', session.get('admin_username', '?'), 'full_reset', 'DB 전체 초기화')
    init_db(force=True)
    _settings_cache.clear()
    load_settings_cache()
    session.clear()
    return jsonify({'ok': True})


# ---------------- 계좌이체 충전 (사용자) ----------------

def get_active_bank_account(c):
    return c.execute('SELECT * FROM bank_accounts WHERE is_active=1 ORDER BY id DESC LIMIT 1').fetchone()


@app.route('/api/deposit/active-account')
@login_required
def api_deposit_active_account():
    conn = get_db()
    acc = get_active_bank_account(conn)
    conn.close()
    if not acc:
        return jsonify({'error': '현재 입금 가능한 계좌가 없어요. 관리자에게 문의해주세요'}), 400
    return jsonify({'bank_name': acc['bank_name'], 'account_number': acc['account_number'], 'holder_name': acc['holder_name']})


@app.route('/api/deposit/request', methods=['POST'])
@login_required
@csrf_protect
@rate_limit(10, 300)
def api_deposit_request():
    uid = current_user_id()
    data = request.get_json(force=True)
    try:
        amount = float(data.get('amount'))
    except (TypeError, ValueError):
        return jsonify({'error': '금액이 올바르지 않아요'}), 400
    if amount < 10000 or amount > 5000000:
        return jsonify({'error': '1회 충전은 10,000원 이상 5,000,000원 이하로 요청해주세요'}), 400

    conn = get_db(); c = conn.cursor()
    acc = get_active_bank_account(c)
    if not acc:
        conn.close(); return jsonify({'error': '현재 입금 가능한 계좌가 없어요. 관리자에게 문의해주세요'}), 400

    ref = 'AM-' + secrets.token_hex(3).upper()
    c.execute('INSERT INTO deposit_requests(user_id,amount,reference_code,bank_account_id,status,created_at) VALUES (?,?,?,?,?,?)',
              (uid, amount, ref, acc['id'], 'pending', now_iso()))
    conn.commit(); conn.close()
    push_admin_notification('new_deposit', f'새 충전요청: {int(amount):,}원 (참조코드 {ref})')
    return jsonify({
        'ok': True, 'reference_code': ref, 'amount': amount,
        'bank': {'bank_name': acc['bank_name'], 'account_number': acc['account_number'], 'holder_name': acc['holder_name']},
        'note': f'입금자명에 "{ref}"를 꼭 포함해서 이체해주세요. 확인까지 영업일 기준 다소 시간이 걸릴 수 있어요.'
    })


@app.route('/api/deposit/my')
@login_required
def api_deposit_my():
    uid = current_user_id()
    conn = get_db()
    rows = [dict(r) for r in conn.execute(
        'SELECT * FROM deposit_requests WHERE user_id=? ORDER BY id DESC LIMIT 20', (uid,)).fetchall()]
    conn.close()
    return jsonify({'requests': rows})


# ---------------- 출금 ----------------

@app.route('/api/withdraw/request', methods=['POST'])
@login_required
@csrf_protect
@rate_limit(10, 300)
def api_withdraw_request():
    uid = current_user_id()
    data = request.get_json(force=True)
    ok, err = verify_pin(uid, data.get('pin'))
    if not ok:
        return jsonify({'error': err}), 403
    try:
        amount = float(data.get('amount'))
    except (TypeError, ValueError):
        return jsonify({'error': '금액이 올바르지 않아요'}), 400
    priority = data.get('priority') if data.get('priority') in ('normal', 'instant') else 'normal'
    if priority == 'instant' and not get_setting('instant_withdraw_enabled'):
        return jsonify({'error': '지금은 즉시출금을 지원하지 않아요. 매주 일요일에 일괄 정산돼요'}), 400
    bank_name = (data.get('bank_name') or '').strip()[:50]
    account_number = (data.get('account_number') or '').strip()[:50]
    holder_name = (data.get('holder_name') or '').strip()[:50]
    if amount < 10000 or amount > 5000000:
        return jsonify({'error': '1회 출금은 10,000원 이상 5,000,000원 이하로 요청해주세요'}), 400
    if not (bank_name and account_number and holder_name):
        return jsonify({'error': '입금받을 은행/계좌번호/예금주를 모두 입력해주세요'}), 400

    conn = get_db(); c = conn.cursor()
    wallet_row = c.execute('SELECT balance FROM wallet WHERE user_id=?', (uid,)).fetchone()
    if not wallet_row or wallet_row['balance'] < amount:
        conn.close(); return jsonify({'error': '잔액이 부족해요'}), 400

    fee = round(amount * get_setting('instant_withdraw_fee')) if priority == 'instant' else 0
    net_amount = amount - fee
    ts = now_iso()

    # 요청 금액은 즉시 지갑에서 차감(보류)해서 이중 출금/이중 사용을 막아요.
    c.execute('UPDATE wallet SET balance=balance-? WHERE user_id=? AND balance>=?', (amount, uid, amount))
    if c.rowcount == 0:
        conn.close(); return jsonify({'error': '잔액이 부족해요'}), 400
    c.execute('''INSERT INTO withdrawal_requests(user_id,amount,fee,net_amount,bank_name,account_number,holder_name,
                 priority,status,created_at) VALUES (?,?,?,?,?,?,?,?,?,?)''',
              (uid, amount, fee, net_amount, bank_name, account_number, holder_name, priority, 'pending', ts))
    req_id = c.lastrowid
    requester = c.execute('SELECT email FROM users WHERE id=?', (uid,)).fetchone()
    conn.commit(); conn.close()

    label = '즉시출금' if priority == 'instant' else '일반출금'
    push_admin_notification('new_withdraw',
        f'{"🔥 " if priority=="instant" else ""}새 {label} 요청: {int(amount):,}원 (수수료 {int(fee):,}원)')
    notify_admin_emails(
        f'[콩나물] 새 {label} 요청이 들어왔어요',
        f'{requester["email"] if requester else "알 수 없음"}님이 {label}을 요청했어요.\n\n'
        f'요청 금액: {int(amount):,}원\n수수료: {int(fee):,}원\n실지급액: {int(net_amount):,}원\n'
        f'입금계좌: {bank_name} {account_number} ({holder_name})\n\n'
        f'관리자 대시보드 "출금관리" 탭에서 처리해주세요.'
    )
    write_audit('user', str(uid), 'withdraw_request', f'id={req_id} amount={amount} priority={priority}')
    return jsonify({'ok': True, 'id': req_id, 'fee': fee, 'net_amount': net_amount})


@app.route('/api/my-withdrawals')
@login_required
def api_my_withdrawals():
    uid = current_user_id()
    conn = get_db()
    rows = [dict(r) for r in conn.execute(
        'SELECT * FROM withdrawal_requests WHERE user_id=? ORDER BY id DESC LIMIT 20', (uid,)).fetchall()]
    conn.close()
    return jsonify({'requests': rows})


@app.route('/api/withdraw/<int:req_id>/cancel', methods=['POST'])
@login_required
@csrf_protect
def api_withdraw_cancel(req_id):
    uid = current_user_id()
    conn = get_db(); c = conn.cursor()
    row = c.execute('SELECT * FROM withdrawal_requests WHERE id=? AND user_id=?', (req_id, uid)).fetchone()
    if not row or row['status'] != 'pending':
        conn.close(); return jsonify({'error': '취소할 수 없는 상태예요'}), 400
    c.execute('UPDATE wallet SET balance=balance+? WHERE user_id=?', (row['amount'], uid))
    c.execute("UPDATE withdrawal_requests SET status='cancelled', decided_at=? WHERE id=?", (now_iso(), req_id))
    conn.commit(); conn.close()
    return jsonify({'ok': True})


# ---------------- 콩나물 플러스 (유료 멤버십) ----------------

@app.route('/api/plus/subscribe', methods=['POST'])
@login_required
@csrf_protect
@rate_limit(10, 300)
def api_plus_subscribe():
    uid = current_user_id()
    fee = get_setting('plus_monthly_fee')
    conn = get_db(); c = conn.cursor()
    wallet_row = c.execute('SELECT balance FROM wallet WHERE user_id=?', (uid,)).fetchone()
    if not wallet_row or wallet_row['balance'] < fee:
        conn.close(); return jsonify({'error': '잔액이 부족해요. 먼저 충전해주세요'}), 400
    user_row = c.execute('SELECT plus_active, plus_expires_at FROM users WHERE id=?', (uid,)).fetchone()
    now = datetime.now(timezone.utc)
    # 이미 활성 상태면 만료일에서 30일 연장, 아니면 지금부터 30일
    base = now
    if user_row['plus_active'] and user_row['plus_expires_at']:
        existing_expiry = datetime.fromisoformat(user_row['plus_expires_at'])
        if existing_expiry > now:
            base = existing_expiry
    new_expiry = base + timedelta(days=PLUS_PERIOD_DAYS)
    c.execute('UPDATE wallet SET balance=balance-? WHERE user_id=? AND balance>=?', (fee, uid, fee))
    if c.rowcount == 0:
        conn.close(); return jsonify({'error': '잔액이 부족해요'}), 400
    c.execute('UPDATE users SET plus_active=1, plus_expires_at=? WHERE id=?', (new_expiry.isoformat(), uid))
    c.execute('INSERT INTO revenue_events(type,amount,user_id,ts) VALUES (?,?,?,?)',
               ('plus_subscription', fee, uid, now_iso()))
    conn.commit(); conn.close()
    write_audit('user', str(uid), 'plus_subscribe', f'fee={fee} until={new_expiry.isoformat()}')
    return jsonify({'ok': True, 'expires_at': new_expiry.isoformat(), 'fee': fee})


@app.route('/api/plus/toggle-autorenew', methods=['POST'])
@login_required
@csrf_protect
def api_plus_toggle_autorenew():
    uid = current_user_id()
    conn = get_db(); c = conn.cursor()
    row = c.execute('SELECT plus_auto_renew FROM users WHERE id=?', (uid,)).fetchone()
    new_val = 0 if row['plus_auto_renew'] else 1
    c.execute('UPDATE users SET plus_auto_renew=? WHERE id=?', (new_val, uid))
    conn.commit(); conn.close()
    return jsonify({'ok': True, 'auto_renew': bool(new_val)})


# ---------------- 선물하기 ----------------

@app.route('/api/gift/send', methods=['POST'])
@login_required
@csrf_protect
@rate_limit(10, 300)
def api_gift_send():
    uid = current_user_id()
    data = request.get_json(force=True)
    ok, err = verify_pin(uid, data.get('pin'))
    if not ok:
        return jsonify({'error': err}), 403
    model_key = data.get('model')
    grade = data.get('grade')
    to_email = (data.get('to_email') or '').strip().lower()
    message = (data.get('message') or '').strip()[:200]
    if not EMAIL_RE.match(to_email):
        return jsonify({'error': '받는 분의 이메일을 올바르게 입력해주세요'}), 400

    conn = get_db(); c = conn.cursor()
    m = c.execute('SELECT * FROM models WHERE key=?', (model_key,)).fetchone()
    if not m or grade not in GRADES:
        conn.close(); return jsonify({'error': 'invalid model/grade'}), 400
    if m['drop_start'] or m['drop_end']:
        now = datetime.now(timezone.utc)
        if m['drop_start'] and now < datetime.fromisoformat(m['drop_start']):
            conn.close(); return jsonify({'error': '아직 판매 시작 전이에요'}), 400
        if m['drop_end'] and now > datetime.fromisoformat(m['drop_end']):
            conn.close(); return jsonify({'error': '판매 시간이 종료됐어요'}), 400

    price = price_for(m, grade, 'sell', get_user_tier(uid)['discount'])
    service_fee = get_setting('gift_service_fee')
    total = price + service_fee

    c.execute('UPDATE stock SET qty=qty-1 WHERE model_key=? AND grade=? AND qty>0', (model_key, grade))
    if c.rowcount == 0:
        conn.close(); return jsonify({'error': '방금 품절됐어요'}), 400
    c.execute('UPDATE wallet SET balance=balance-? WHERE user_id=? AND balance>=?', (total, uid, total))
    if c.rowcount == 0:
        c.execute('UPDATE stock SET qty=qty+1 WHERE model_key=? AND grade=?', (model_key, grade))
        conn.commit(); conn.close()
        return jsonify({'error': '잔액이 부족해요'}), 400

    ts = now_iso()
    c.execute('INSERT INTO trades(user_id,model_key,grade,side,price,label,ts,pending) VALUES (?,?,?,?,?,?,?,1)',
              (uid, model_key, grade, 'sell_to_user', price, '선물구매', ts))
    c.execute('UPDATE models SET internal_trade_count=internal_trade_count+1 WHERE key=?', (model_key,))
    c.execute('UPDATE stats SET today_trades=today_trades+1 WHERE id=1')
    if service_fee > 0:
        c.execute('INSERT INTO revenue_events(type,amount,user_id,ts) VALUES (?,?,?,?)',
                   ('gift_service_fee', service_fee, uid, ts))

    recipient = c.execute('SELECT id, email FROM users WHERE email=?', (to_email,)).fetchone()
    if recipient:
        item_id = 'itm-' + uuid.uuid4().hex[:8]
        c.execute('INSERT INTO portfolio(id,user_id,model_key,grade,bought_price,ts) VALUES (?,?,?,?,?,?)',
                   (item_id, recipient['id'], model_key, grade, price, ts))
        c.execute('''INSERT INTO gifts(from_user_id,to_email,model_key,grade,price,service_fee,message,status,
                     created_at,claimed_at,portfolio_item_id) VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
                  (uid, to_email, model_key, grade, price, service_fee, message, 'claimed', ts, ts, item_id))
        push_notification(recipient['id'], 'gift_received',
            f'🎁 선물이 도착했어요! {m["name"]} {grade}급' + (f' - "{message}"' if message else ''))
        send_email(to_email, '[콩나물] 선물이 도착했어요',
            f'{m["name"]} {grade}급 상품을 선물로 받으셨어요!\n' + (f'메시지: {message}\n\n' if message else '\n') +
            '로그인해서 보관함을 확인해보세요.')
        recipient_exists = True
    else:
        c.execute('''INSERT INTO gifts(from_user_id,to_email,model_key,grade,price,service_fee,message,status,created_at)
                     VALUES (?,?,?,?,?,?,?,?,?)''',
                  (uid, to_email, model_key, grade, price, service_fee, message, 'pending', ts))
        send_email(to_email, '[콩나물] 선물이 도착했어요! 가입하고 받아보세요',
            f'콩나물에서 {m["name"]} {grade}급 상품을 선물로 받으셨어요!\n' + (f'메시지: {message}\n\n' if message else '\n') +
            f'이 이메일({to_email})로 회원가입하시면 자동으로 보관함에 들어와요.')
        recipient_exists = False

    conn.commit(); conn.close()
    write_audit('user', str(uid), 'gift_send', f'to={to_email} model={model_key} grade={grade}')
    return jsonify({'ok': True, 'price': price, 'service_fee': service_fee, 'total': total, 'recipient_exists': recipient_exists})


@app.route('/api/my-gifts-sent')
@login_required
def api_my_gifts_sent():
    uid = current_user_id()
    conn = get_db()
    rows = conn.execute('SELECT * FROM gifts WHERE from_user_id=? ORDER BY id DESC LIMIT 30', (uid,)).fetchall()
    conn.close()
    return jsonify({'gifts': [dict(r) for r in rows]})




@app.route('/api/pg/config')
def api_pg_config():
    return jsonify({'client_key': TOSS_CLIENT_KEY, 'enabled': bool(get_setting('card_payment_enabled'))})


@app.route('/api/pg/create-order', methods=['POST'])
@login_required
@csrf_protect
@rate_limit(10, 300)
def api_pg_create_order():
    uid = current_user_id()
    if not get_setting('card_payment_enabled'):
        return jsonify({'error': '지금은 계좌이체로만 충전할 수 있어요'}), 400
    data = request.get_json(force=True)
    try:
        amount = int(float(data.get('amount')))
    except (TypeError, ValueError):
        return jsonify({'error': '금액이 올바르지 않아요'}), 400
    if amount < 1000 or amount > 5000000:
        return jsonify({'error': '1회 결제는 1,000원 이상 5,000,000원 이하로 요청해주세요'}), 400
    order_id = 'kn' + uuid.uuid4().hex[:20]
    conn = get_db(); c = conn.cursor()
    c.execute('INSERT INTO pg_orders(user_id,order_id,amount,status,created_at) VALUES (?,?,?,?,?)',
              (uid, order_id, amount, 'pending', now_iso()))
    conn.commit(); conn.close()
    return jsonify({
        'ok': True, 'order_id': order_id, 'amount': amount, 'client_key': TOSS_CLIENT_KEY,
        'order_name': '콩나물 잔액 충전',
    })


@app.route('/api/pg/success')
def api_pg_success():
    """토스페이먼츠 결제창의 successUrl. 결제수단 인증까지만 끝난 상태라, 여기서 최종 승인 API를 호출해야 결제가 완료돼요."""
    payment_key = request.args.get('paymentKey')
    order_id = request.args.get('orderId')
    amount = request.args.get('amount')
    conn = get_db(); c = conn.cursor()
    order = c.execute('SELECT * FROM pg_orders WHERE order_id=?', (order_id,)).fetchone()
    if not order or order['status'] != 'pending':
        conn.close()
        return f'<script>location.href="/?pg=fail&message=처리할 수 없는 주문이에요"</script>'
    try:
        if int(float(amount)) != int(order['amount']):
            conn.close()
            return f'<script>location.href="/?pg=fail&message=결제 금액이 일치하지 않아요"</script>'
    except (TypeError, ValueError):
        conn.close()
        return f'<script>location.href="/?pg=fail&message=잘못된 요청이에요"</script>'

    auth = base64.b64encode(f'{TOSS_SECRET_KEY}:'.encode()).decode()
    try:
        resp = requests.post(
            f'{TOSS_API_BASE}/payments/confirm',
            json={'paymentKey': payment_key, 'orderId': order_id, 'amount': order['amount']},
            headers={'Authorization': f'Basic {auth}', 'Content-Type': 'application/json'},
            timeout=10,
        )
    except requests.RequestException as e:
        conn.close()
        return f'<script>location.href="/?pg=fail&message=결제 서버 통신 오류"</script>'

    if resp.status_code == 200:
        c.execute('UPDATE wallet SET balance=balance+? WHERE user_id=?', (order['amount'], order['user_id']))
        c.execute('UPDATE pg_orders SET status=?, paid_at=? WHERE order_id=?', ('paid', now_iso(), order_id))
        conn.commit(); conn.close()
        push_notification(order['user_id'], 'pg_paid', f'카드 결제로 {int(order["amount"]):,}원이 충전됐어요.', {'wallet_changed': True})
        return f'<script>location.href="/?pg=success&amount={order["amount"]}"</script>'
    else:
        c.execute('UPDATE pg_orders SET status=? WHERE order_id=?', ('failed', order_id))
        conn.commit(); conn.close()
        err = resp.json().get('message', '결제 승인에 실패했어요') if resp.headers.get('content-type', '').startswith('application/json') else '결제 승인에 실패했어요'
        return f'<script>location.href="/?pg=fail&message={err}"</script>'


@app.route('/api/pg/fail')
def api_pg_fail():
    order_id = request.args.get('orderId')
    message = request.args.get('message', '결제가 취소됐어요')
    if order_id:
        conn = get_db(); c = conn.cursor()
        c.execute("UPDATE pg_orders SET status='failed' WHERE order_id=? AND status='pending'", (order_id,))
        conn.commit(); conn.close()
    return f'<script>location.href="/?pg=fail&message={message}"</script>'


# ---------------- 매입 신청 (검수 기반 판매, 사용자) ----------------

INSPECTION_ADDRESS = '서울시 강남구 테헤란로 000, 콩나물 검수센터 (가상 주소 - app.py에서 수정)'

@app.route('/api/sell/request', methods=['POST'])
@login_required
@csrf_protect
@rate_limit(20, 300)
def api_sell_request():
    uid = current_user_id()
    data = request.get_json(force=True)
    model_key = data.get('model')
    grade = data.get('grade')
    note = (data.get('note') or '').strip()[:500]
    fast_track = bool(data.get('fast_track'))
    conn = get_db(); c = conn.cursor()
    m = c.execute('SELECT * FROM models WHERE key=?', (model_key,)).fetchone()
    if not m or grade not in GRADES:
        conn.close(); return jsonify({'error': 'invalid model/grade'}), 400
    estimated = price_for(m, grade, 'buy', get_user_tier(uid)['discount'])
    is_plus = get_plus_status(uid)['active']
    ft_fee = 0 if (not fast_track or is_plus) else get_setting('fast_track_fee')
    ts = now_iso()
    c.execute('''INSERT INTO sell_requests(user_id,model_key,self_grade,note,estimated_price,status,created_at,updated_at,is_fast_track,fast_track_fee)
                 VALUES (?,?,?,?,?,?,?,?,?,?)''', (uid, model_key, grade, note, estimated, 'submitted', ts, ts, 1 if fast_track else 0, ft_fee))
    req_id = c.lastrowid
    conn.commit(); conn.close()
    write_audit('user', str(uid), 'sell_request_submit', f'id={req_id} model={model_key} grade={grade} fast_track={fast_track}')
    push_admin_notification('new_sellreq', f'{"🚀 빠른처리 " if fast_track else ""}새 매입신청: {model_key} {grade}급 (예상가 {int(estimated):,}원)')
    return jsonify({'ok': True, 'id': req_id, 'estimated_price': estimated, 'fast_track_fee': ft_fee, 'shipping_address': INSPECTION_ADDRESS})


@app.route('/api/sell/my')
@login_required
def api_sell_my():
    uid = current_user_id()
    conn = get_db()
    rows = [dict(r) for r in conn.execute(
        'SELECT * FROM sell_requests WHERE user_id=? ORDER BY id DESC LIMIT 30', (uid,)).fetchall()]
    conn.close()
    return jsonify({'requests': rows})


@app.route('/api/sell/<int:req_id>/tracking', methods=['POST'])
@login_required
@csrf_protect
def api_sell_tracking(req_id):
    uid = current_user_id()
    data = request.get_json(force=True)
    tracking = (data.get('tracking_note') or '').strip()[:200]
    conn = get_db(); c = conn.cursor()
    row = c.execute('SELECT * FROM sell_requests WHERE id=? AND user_id=?', (req_id, uid)).fetchone()
    if not row:
        conn.close(); return jsonify({'error': 'not found'}), 404
    c.execute('UPDATE sell_requests SET tracking_note=?, updated_at=? WHERE id=?', (tracking, now_iso(), req_id))
    conn.commit(); conn.close()
    return jsonify({'ok': True})


@app.route('/api/sell/<int:req_id>/cancel', methods=['POST'])
@login_required
@csrf_protect
def api_sell_cancel(req_id):
    uid = current_user_id()
    conn = get_db(); c = conn.cursor()
    row = c.execute('SELECT * FROM sell_requests WHERE id=? AND user_id=?', (req_id, uid)).fetchone()
    if not row or row['status'] != 'submitted':
        conn.close(); return jsonify({'error': '취소할 수 없는 상태예요'}), 400
    c.execute('UPDATE sell_requests SET status=?, updated_at=? WHERE id=?', ('cancelled', now_iso(), req_id))
    conn.commit(); conn.close()
    return jsonify({'ok': True})


# ---------------- 예약(관심등록) ----------------
# 드랍(판매 시간창)이 곧 시작되는 상품에 미리 관심 등록을 해두면, 사이트 방문 시
# "예약하신 상품이 곧 판매돼요" 같은 안내를 볼 수 있어요. 예약 자체가 구매를 확정하거나
# 재고를 미리 잡아두지는 않아요 (선착순 원칙은 동일하게 유지).

@app.route('/api/reserve', methods=['POST'])
@login_required
@csrf_protect
def api_reserve():
    uid = current_user_id()
    data = request.get_json(force=True)
    model_key = data.get('model')
    grade = data.get('grade')
    conn = get_db(); c = conn.cursor()
    m = c.execute('SELECT key FROM models WHERE key=?', (model_key,)).fetchone()
    if not m or grade not in GRADES:
        conn.close(); return jsonify({'error': 'invalid model/grade'}), 400
    try:
        c.execute('INSERT INTO reservations(user_id,model_key,grade,created_at) VALUES (?,?,?,?)',
                   (uid, model_key, grade, now_iso()))
        conn.commit()
    except sqlite3.IntegrityError:
        pass  # 이미 예약되어 있으면 조용히 통과
    conn.close()
    return jsonify({'ok': True})


@app.route('/api/reserve/<model_key>/<grade>', methods=['DELETE'])
@login_required
@csrf_protect
def api_unreserve(model_key, grade):
    uid = current_user_id()
    conn = get_db(); c = conn.cursor()
    c.execute('DELETE FROM reservations WHERE user_id=? AND model_key=? AND grade=?', (uid, model_key, grade))
    conn.commit(); conn.close()
    return jsonify({'ok': True})


@app.route('/api/my-reservations')
@login_required
def api_my_reservations():
    uid = current_user_id()
    conn = get_db()
    rows = conn.execute('SELECT model_key, grade FROM reservations WHERE user_id=?', (uid,)).fetchall()
    conn.close()
    return jsonify({'reservations': [dict(r) for r in rows]})


# ---------------- 가격 알림 ----------------

@app.route('/api/price-alerts', methods=['POST'])
@login_required
@csrf_protect
@rate_limit(20, 300)
def api_create_price_alert():
    uid = current_user_id()
    data = request.get_json(force=True)
    model_key = data.get('model')
    grade = data.get('grade')
    try:
        target_price = float(data.get('target_price'))
    except (TypeError, ValueError):
        return jsonify({'error': '목표 가격이 올바르지 않아요'}), 400
    conn = get_db(); c = conn.cursor()
    m = c.execute('SELECT key FROM models WHERE key=?', (model_key,)).fetchone()
    if not m or grade not in GRADES or target_price <= 0:
        conn.close(); return jsonify({'error': 'invalid request'}), 400
    c.execute('INSERT INTO price_alerts(user_id,model_key,grade,target_price,triggered,created_at) VALUES (?,?,?,?,0,?)',
              (uid, model_key, grade, target_price, now_iso()))
    conn.commit(); conn.close()
    return jsonify({'ok': True})


@app.route('/api/my-price-alerts')
@login_required
def api_my_price_alerts():
    uid = current_user_id()
    conn = get_db()
    rows = conn.execute('SELECT * FROM price_alerts WHERE user_id=? ORDER BY id DESC', (uid,)).fetchall()
    conn.close()
    return jsonify({'alerts': [dict(r) for r in rows]})


@app.route('/api/price-alerts/<int:alert_id>', methods=['DELETE'])
@login_required
@csrf_protect
def api_delete_price_alert(alert_id):
    uid = current_user_id()
    conn = get_db(); c = conn.cursor()
    c.execute('DELETE FROM price_alerts WHERE id=? AND user_id=?', (alert_id, uid))
    conn.commit(); conn.close()
    return jsonify({'ok': True})


# ---------------- 찜하기(위시리스트) ----------------

@app.route('/api/wishlist', methods=['POST'])
@login_required
@csrf_protect
def api_add_wishlist():
    uid = current_user_id()
    data = request.get_json(force=True)
    model_key = data.get('model')
    grade = data.get('grade')
    conn = get_db(); c = conn.cursor()
    m = c.execute('SELECT key FROM models WHERE key=?', (model_key,)).fetchone()
    if not m or grade not in GRADES:
        conn.close(); return jsonify({'error': 'invalid request'}), 400
    try:
        c.execute('INSERT INTO wishlist(user_id,model_key,grade,created_at) VALUES (?,?,?,?)',
                   (uid, model_key, grade, now_iso()))
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    conn.close()
    return jsonify({'ok': True})


@app.route('/api/wishlist/<model_key>/<grade>', methods=['DELETE'])
@login_required
@csrf_protect
def api_remove_wishlist(model_key, grade):
    uid = current_user_id()
    conn = get_db(); c = conn.cursor()
    c.execute('DELETE FROM wishlist WHERE user_id=? AND model_key=? AND grade=?', (uid, model_key, grade))
    conn.commit(); conn.close()
    return jsonify({'ok': True})


@app.route('/api/my-wishlist')
@login_required
def api_my_wishlist():
    uid = current_user_id()
    conn = get_db()
    rows = conn.execute('SELECT model_key, grade FROM wishlist WHERE user_id=?', (uid,)).fetchall()
    conn.close()
    return jsonify({'wishlist': [dict(r) for r in rows]})


# ---------------- 인기 상품 랭킹 ----------------

@app.route('/api/popular')
def api_popular():
    days = int(request.args.get('days', 7))
    conn = get_db()
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    rows = conn.execute('''
        SELECT model_key, COUNT(*) as trade_count
        FROM trades WHERE ts >= ?
        GROUP BY model_key ORDER BY trade_count DESC LIMIT 5
    ''', (since,)).fetchall()
    result = []
    for r in rows:
        m = conn.execute('SELECT name, code FROM models WHERE key=?', (r['model_key'],)).fetchone()
        if m:
            result.append({'model_key': r['model_key'], 'name': m['name'], 'code': m['code'], 'trade_count': r['trade_count']})
    conn.close()
    return jsonify({'popular': result, 'days': days})


# ---------------- 리뷰 (실거래 인증 기반) ----------------

@app.route('/api/my-orders')
@login_required
def api_my_orders():
    """내가 구매체결한 거래 목록 + 이미 리뷰를 남겼는지 여부 + 인증서 발급여부"""
    uid = current_user_id()
    conn = get_db()
    rows = conn.execute('''
        SELECT t.id, t.model_key, t.grade, t.price, t.ts,
               (SELECT COUNT(*) FROM reviews r WHERE r.trade_id=t.id) as reviewed,
               (SELECT COUNT(*) FROM certificates ce WHERE ce.trade_id=t.id) as has_certificate
        FROM trades t WHERE t.user_id=? AND t.side='sell_to_user'
        ORDER BY t.id DESC LIMIT 50
    ''', (uid,)).fetchall()
    conn.close()
    return jsonify({'orders': [dict(r) for r in rows]})


@app.route('/api/certificate/<int:trade_id>')
@login_required
def api_certificate(trade_id):
    uid = current_user_id()
    conn = get_db(); c = conn.cursor()
    trade = c.execute('SELECT * FROM trades WHERE id=? AND user_id=? AND side=?',
                       (trade_id, uid, 'sell_to_user')).fetchone()
    if not trade:
        conn.close(); return jsonify({'error': '본인이 구매한 거래만 인증서를 발급할 수 있어요'}), 400

    existing = c.execute('SELECT * FROM certificates WHERE trade_id=?', (trade_id,)).fetchone()
    if not existing:
        is_plus = get_plus_status(uid)['active']
        fee = 0 if is_plus else get_setting('certificate_fee')
        if fee > 0:
            wrow = c.execute('SELECT balance FROM wallet WHERE user_id=?', (uid,)).fetchone()
            if not wrow or wrow['balance'] < fee:
                conn.close(); return jsonify({'error': f'인증서 발급에는 {int(fee):,}원이 필요해요. 잔액이 부족해요'}), 400
            c.execute('UPDATE wallet SET balance=balance-? WHERE user_id=? AND balance>=?', (fee, uid, fee))
            if c.rowcount == 0:
                conn.close(); return jsonify({'error': '잔액이 부족해요'}), 400
            c.execute('INSERT INTO revenue_events(type,amount,user_id,ts) VALUES (?,?,?,?)',
                       ('certificate_fee', fee, uid, now_iso()))
        cert_code = 'KN-' + secrets.token_hex(4).upper()
        c.execute('INSERT INTO certificates(trade_id,user_id,cert_code,fee_paid,issued_at) VALUES (?,?,?,?,?)',
                   (trade_id, uid, cert_code, fee, now_iso()))
        conn.commit()
        write_audit('user', str(uid), 'issue_certificate', f'trade_id={trade_id} fee={fee}')
        existing = c.execute('SELECT * FROM certificates WHERE trade_id=?', (trade_id,)).fetchone()

    m = c.execute('SELECT name FROM models WHERE key=?', (trade['model_key'],)).fetchone()
    user = c.execute('SELECT email FROM users WHERE id=?', (uid,)).fetchone()
    conn.close()

    pdf_bytes = generate_certificate_pdf(
        existing['cert_code'],
        m['name'] if m else trade['model_key'],
        trade['grade'],
        trade['price'],
        user['email'],
        trade['ts'][:10],
        existing['issued_at'][:10],
    )
    return Response(pdf_bytes, mimetype='application/pdf', headers={
        'Content-Disposition': f'inline; filename="kongnamul_certificate_{existing["cert_code"]}.pdf"'
    })


@app.route('/api/verify-certificate/<cert_code>')
def api_verify_certificate(cert_code):
    """누구나(로그인 없이) 인증코드로 진위를 확인할 수 있는 공개 조회 API예요.
    구매자 이메일은 일부만 노출해서 개인정보를 보호해요."""
    cert_code = cert_code.strip().upper()
    conn = get_db()
    row = conn.execute('SELECT * FROM certificates WHERE cert_code=?', (cert_code,)).fetchone()
    if not row:
        conn.close()
        return jsonify({'valid': False})
    trade = conn.execute('SELECT * FROM trades WHERE id=?', (row['trade_id'],)).fetchone()
    m = conn.execute('SELECT name, code FROM models WHERE key=?', (trade['model_key'],)).fetchone() if trade else None
    user = conn.execute('SELECT email FROM users WHERE id=?', (row['user_id'],)).fetchone()
    conn.close()
    if not trade or not user:
        return jsonify({'valid': False})
    email = user['email']
    name, _, domain = email.partition('@')
    masked_email = (name[:2] + '***@' + domain) if len(name) > 2 else ('**@' + domain)
    return jsonify({
        'valid': True,
        'cert_code': row['cert_code'],
        'model_name': m['name'] if m else trade['model_key'],
        'grade': trade['grade'],
        'price': trade['price'],
        'buyer_email_masked': masked_email,
        'trade_date': trade['ts'][:10],
        'issued_at': row['issued_at'][:10],
    })


@app.route('/api/reviews')
def api_reviews():
    model_key = request.args.get('model')
    conn = get_db()
    q = '''SELECT r.*, u.email FROM reviews r JOIN users u ON r.user_id=u.id'''
    params = []
    if model_key:
        q += ' WHERE r.model_key=?'
        params.append(model_key)
    q += ' ORDER BY r.id DESC LIMIT 100'
    rows = conn.execute(q, params).fetchall()
    avg = conn.execute('SELECT AVG(rating) a, COUNT(*) c FROM reviews' + (' WHERE model_key=?' if model_key else ''),
                        params).fetchone()
    conn.close()
    reviews = []
    for r in rows:
        d = dict(r)
        # 이메일은 일부만 노출 (마스킹)
        email = d['email']
        name, _, domain = email.partition('@')
        d['email'] = (name[:2] + '***@' + domain) if len(name) > 2 else ('**@' + domain)
        reviews.append(d)
    return jsonify({'reviews': reviews, 'average': round(avg['a'], 2) if avg['a'] else None, 'count': avg['c']})


@app.route('/api/reviews', methods=['POST'])
@login_required
@csrf_protect
@rate_limit(10, 300)
def api_create_review():
    uid = current_user_id()
    data = request.get_json(force=True)
    trade_id = data.get('trade_id')
    try:
        rating = int(data.get('rating'))
    except (TypeError, ValueError):
        return jsonify({'error': '별점이 올바르지 않아요'}), 400
    comment = (data.get('comment') or '').strip()[:500]
    if rating < 1 or rating > 5:
        return jsonify({'error': '별점은 1~5 사이여야 해요'}), 400
    conn = get_db(); c = conn.cursor()
    trade = c.execute('SELECT * FROM trades WHERE id=? AND user_id=? AND side=?',
                       (trade_id, uid, 'sell_to_user')).fetchone()
    if not trade:
        conn.close(); return jsonify({'error': '본인이 구매한 거래만 리뷰를 남길 수 있어요'}), 400
    existing = c.execute('SELECT id FROM reviews WHERE trade_id=?', (trade_id,)).fetchone()
    if existing:
        conn.close(); return jsonify({'error': '이미 리뷰를 남긴 거래예요'}), 400
    c.execute('INSERT INTO reviews(user_id,trade_id,model_key,grade,rating,comment,created_at) VALUES (?,?,?,?,?,?,?)',
              (uid, trade_id, trade['model_key'], trade['grade'], rating, comment, now_iso()))
    conn.commit(); conn.close()
    return jsonify({'ok': True})


@app.route('/api/admin/reviews/<int:review_id>', methods=['DELETE'])
@admin_required
@csrf_protect
def api_admin_delete_review(review_id):
    conn = get_db(); c = conn.cursor()
    c.execute('DELETE FROM reviews WHERE id=?', (review_id,))
    conn.commit(); conn.close()
    write_audit('admin', session.get('admin_username'), 'delete_review', f'id={review_id}')
    return jsonify({'ok': True})


# ---------------- 관리자 인증 ----------------

@app.route('/api/admin/login', methods=['POST'])
@rate_limit(10, 300)
def api_admin_login():
    data = request.get_json(force=True)
    username = (data.get('username') or '').strip()
    password = data.get('password') or ''
    lock_key = f'admin:{username}'
    if check_login_lock(lock_key):
        return jsonify({'error': f'로그인 시도가 너무 많아 {LOGIN_LOCK_MINUTES}분간 잠겼어요'}), 429
    conn = get_db()
    admin = conn.execute('SELECT * FROM admins WHERE username=?', (username,)).fetchone()
    conn.close()
    if not admin or not check_password_hash(admin['password_hash'], password):
        record_login_failure(lock_key)
        write_audit('admin', username, 'login_failed')
        return jsonify({'error': '아이디 또는 비밀번호가 올바르지 않아요'}), 401
    clear_login_failures(lock_key)
    session['admin_id'] = admin['id']
    session['admin_username'] = admin['username']
    token = issue_csrf_token()
    write_audit('admin', username, 'login')
    return jsonify({'ok': True, 'admin': {'username': admin['username']}, 'csrf_token': token})


@app.route('/api/admin/logout', methods=['POST'])
def api_admin_logout():
    session.pop('admin_id', None)
    session.pop('admin_username', None)
    return jsonify({'ok': True})


@app.route('/api/admin/me')
def api_admin_me():
    aid = current_admin_id()
    if not aid:
        return jsonify({'admin': None})
    return jsonify({'admin': {'username': session.get('admin_username')}, 'csrf_token': session.get('csrf_token') or issue_csrf_token()})


# ---------------- 관리자 대시보드 데이터 ----------------

@app.route('/api/admin/orders')
@admin_required
def api_admin_orders():
    limit = min(int(request.args.get('limit', 50)), 200)
    conn = get_db()
    rows = conn.execute('''
        SELECT t.id, t.ts, t.side, t.label, t.model_key, t.grade, t.price, u.email
        FROM trades t LEFT JOIN users u ON t.user_id = u.id
        ORDER BY t.id DESC LIMIT ?
    ''', (limit,)).fetchall()
    conn.close()
    return jsonify({'orders': [dict(r) for r in rows]})


@app.route('/api/admin/users')
@admin_required
def api_admin_users():
    conn = get_db()
    rows = conn.execute('''
        SELECT u.id, u.email, u.created_at, u.is_suspended, u.phone_number, u.phone_verified,
               COALESCE(w.balance,0) as balance,
               (SELECT COUNT(*) FROM portfolio p WHERE p.user_id=u.id) as item_count,
               (SELECT COUNT(*) FROM trades t WHERE t.user_id=u.id) as trade_count
        FROM users u LEFT JOIN wallet w ON w.user_id=u.id
        ORDER BY u.id DESC
    ''').fetchall()
    conn.close()
    return jsonify({'users': [dict(r) for r in rows]})


@app.route('/api/admin/users/<int:user_id>/suspend', methods=['POST'])
@admin_required
@csrf_protect
def api_admin_suspend_user(user_id):
    conn = get_db(); c = conn.cursor()
    c.execute('UPDATE users SET is_suspended=1 WHERE id=?', (user_id,))
    conn.commit(); conn.close()
    write_audit('admin', session.get('admin_username'), 'suspend_user', f'user_id={user_id}')
    push_notification(user_id, 'account', '계정이 정지됐어요. 관리자에게 문의해주세요.')
    return jsonify({'ok': True})


@app.route('/api/admin/users/<int:user_id>/unsuspend', methods=['POST'])
@admin_required
@csrf_protect
def api_admin_unsuspend_user(user_id):
    conn = get_db(); c = conn.cursor()
    c.execute('UPDATE users SET is_suspended=0 WHERE id=?', (user_id,))
    conn.commit(); conn.close()
    write_audit('admin', session.get('admin_username'), 'unsuspend_user', f'user_id={user_id}')
    push_notification(user_id, 'account', '계정 정지가 해제됐어요.')
    return jsonify({'ok': True})


@app.route('/api/admin/users/<int:user_id>/adjust-balance', methods=['POST'])
@admin_required
@csrf_protect
def api_admin_adjust_balance(user_id):
    data = request.get_json(force=True)
    try:
        amount = float(data.get('amount'))
    except (TypeError, ValueError):
        return jsonify({'error': '금액이 올바르지 않아요'}), 400
    reason = (data.get('reason') or '').strip()[:200]
    if amount == 0:
        return jsonify({'error': '0이 아닌 금액을 입력해주세요'}), 400
    conn = get_db(); c = conn.cursor()
    wrow = c.execute('SELECT balance FROM wallet WHERE user_id=?', (user_id,)).fetchone()
    if not wrow:
        conn.close(); return jsonify({'error': '존재하지 않는 유저예요'}), 400
    new_balance = max(0, wrow['balance'] + amount)
    c.execute('UPDATE wallet SET balance=? WHERE user_id=?', (new_balance, user_id))
    conn.commit(); conn.close()
    write_audit('admin', session.get('admin_username'), 'adjust_balance',
                f'user_id={user_id} amount={amount:+} reason={reason} new_balance={new_balance}')
    push_notification(user_id, 'balance', f'잔액이 {amount:+,.0f}원 조정됐어요.' + (f' ({reason})' if reason else ''), {'wallet_changed': True})
    return jsonify({'ok': True, 'new_balance': new_balance})


@app.route('/api/admin/revenue')
@admin_required
def api_admin_revenue():
    conn = get_db()
    def agg(where=''):
        buy_total = conn.execute(f"SELECT COALESCE(SUM(price),0) v FROM trades WHERE side='buy_from_user' {where}").fetchone()['v']
        sell_total = conn.execute(f"SELECT COALESCE(SUM(price),0) v FROM trades WHERE side='sell_to_user' {where}").fetchone()['v']
        delivery_fee = conn.execute(f"SELECT COALESCE(SUM(amount),0) v FROM revenue_events WHERE type='delivery_fee' {where}").fetchone()['v']
        storage_fee = conn.execute(f"SELECT COALESCE(SUM(amount),0) v FROM revenue_events WHERE type='storage_fee' {where}").fetchone()['v']
        withdraw_fee = conn.execute(f"SELECT COALESCE(SUM(amount),0) v FROM revenue_events WHERE type='withdraw_fee' {where}").fetchone()['v']
        spread = sell_total - buy_total
        return {
            'buy_total': buy_total, 'sell_total': sell_total, 'spread': spread,
            'delivery_fee': delivery_fee, 'storage_fee': storage_fee, 'withdraw_fee': withdraw_fee,
            'total_revenue': spread + delivery_fee + storage_fee + withdraw_fee,
        }
    today = datetime.now(timezone.utc).date().isoformat()
    all_time = agg()
    today_stats = agg(f"AND ts >= '{today}'")
    conn.close()
    return jsonify({'all_time': all_time, 'today': today_stats})


@app.route('/api/admin/deposits')
@admin_required
def api_admin_deposits():
    status = request.args.get('status')
    conn = get_db()
    q = '''SELECT d.*, u.email, b.bank_name, b.account_number, b.holder_name
           FROM deposit_requests d LEFT JOIN users u ON d.user_id=u.id
           LEFT JOIN bank_accounts b ON d.bank_account_id=b.id'''
    params = []
    if status:
        q += ' WHERE d.status=?'
        params.append(status)
    q += ' ORDER BY d.id DESC LIMIT 100'
    rows = conn.execute(q, params).fetchall()
    conn.close()
    return jsonify({'deposits': [dict(r) for r in rows]})


@app.route('/api/admin/deposits/<int:dep_id>/approve', methods=['POST'])
@admin_required
@csrf_protect
def api_admin_deposit_approve(dep_id):
    conn = get_db(); c = conn.cursor()
    dep = c.execute('SELECT * FROM deposit_requests WHERE id=?', (dep_id,)).fetchone()
    if not dep or dep['status'] != 'pending':
        conn.close(); return jsonify({'error': '처리할 수 없는 요청이에요'}), 400
    c.execute('UPDATE wallet SET balance=balance+? WHERE user_id=?', (dep['amount'], dep['user_id']))
    c.execute('UPDATE deposit_requests SET status=?, decided_at=?, decided_by=? WHERE id=?',
              ('approved', now_iso(), session.get('admin_username'), dep_id))
    conn.commit(); conn.close()
    write_audit('admin', session.get('admin_username'), 'approve_deposit', f'id={dep_id} amount={dep["amount"]}')
    push_notification(dep['user_id'], 'deposit', f'충전 {int(dep["amount"]):,}원이 승인됐어요!', {'wallet_changed': True})
    return jsonify({'ok': True})


@app.route('/api/admin/deposits/<int:dep_id>/reject', methods=['POST'])
@admin_required
@csrf_protect
def api_admin_deposit_reject(dep_id):
    conn = get_db(); c = conn.cursor()
    dep = c.execute('SELECT * FROM deposit_requests WHERE id=?', (dep_id,)).fetchone()
    if not dep or dep['status'] != 'pending':
        conn.close(); return jsonify({'error': '처리할 수 없는 요청이에요'}), 400
    c.execute('UPDATE deposit_requests SET status=?, decided_at=?, decided_by=? WHERE id=?',
              ('rejected', now_iso(), session.get('admin_username'), dep_id))
    conn.commit(); conn.close()
    write_audit('admin', session.get('admin_username'), 'reject_deposit', f'id={dep_id}')
    push_notification(dep['user_id'], 'deposit', f'충전 요청 {int(dep["amount"]):,}원이 거절됐어요. 입금 내역을 확인해주세요.')
    return jsonify({'ok': True})


# ---------------- 관리자: 출금 관리 ----------------

@app.route('/api/admin/withdrawals')
@admin_required
def api_admin_withdrawals():
    status = request.args.get('status')
    conn = get_db()
    q = '''SELECT w.*, u.email FROM withdrawal_requests w LEFT JOIN users u ON w.user_id=u.id'''
    params = []
    if status:
        q += ' WHERE w.status=?'
        params.append(status)
    q += ' ORDER BY (w.priority=\'instant\') DESC, w.id ASC LIMIT 100'
    rows = conn.execute(q, params).fetchall()
    conn.close()
    return jsonify({'withdrawals': [dict(r) for r in rows]})


@app.route('/api/admin/withdrawals/batch-complete', methods=['POST'])
@admin_required
@csrf_protect
def api_admin_withdraw_batch_complete():
    """대기중인 출금 요청을 한 번에 전부 완료 처리해요. 실제 송금은 관리자가 은행 대량이체
    기능 등으로 미리 처리했다는 전제예요 (이 앱이 실제로 계좌에 돈을 보내주진 않아요)."""
    conn = get_db(); c = conn.cursor()
    rows = c.execute("SELECT * FROM withdrawal_requests WHERE status='pending'").fetchall()
    if not rows:
        conn.close(); return jsonify({'error': '대기중인 출금 요청이 없어요'}), 400
    ts = now_iso()
    total_net = 0
    for row in rows:
        c.execute("UPDATE withdrawal_requests SET status='completed', decided_at=?, decided_by=? WHERE id=?",
                   (ts, session.get('admin_username'), row['id']))
        if row['fee'] > 0:
            c.execute('INSERT INTO revenue_events(type,amount,user_id,ts) VALUES (?,?,?,?)',
                       ('withdraw_fee', row['fee'], row['user_id'], ts))
        push_notification(row['user_id'], 'withdraw',
            f'출금이 완료됐어요! {row["bank_name"]} {row["account_number"]}로 {int(row["net_amount"]):,}원을 보내드렸어요.')
        total_net += row['net_amount']
    conn.commit(); conn.close()
    write_audit('admin', session.get('admin_username'), 'batch_complete_withdraw', f'{len(rows)}건 총 {total_net}원')
    return jsonify({'ok': True, 'count': len(rows), 'total_net': total_net})


@app.route('/api/admin/withdrawals/<int:req_id>/complete', methods=['POST'])
@admin_required
@csrf_protect
def api_admin_withdraw_complete(req_id):
    conn = get_db(); c = conn.cursor()
    row = c.execute('SELECT * FROM withdrawal_requests WHERE id=?', (req_id,)).fetchone()
    if not row or row['status'] != 'pending':
        conn.close(); return jsonify({'error': '처리할 수 없는 상태예요'}), 400
    c.execute("UPDATE withdrawal_requests SET status='completed', decided_at=?, decided_by=? WHERE id=?",
              (now_iso(), session.get('admin_username'), req_id))
    if row['fee'] > 0:
        c.execute('INSERT INTO revenue_events(type,amount,user_id,ts) VALUES (?,?,?,?)',
                   ('withdraw_fee', row['fee'], row['user_id'], now_iso()))
    conn.commit(); conn.close()
    write_audit('admin', session.get('admin_username'), 'complete_withdraw', f'id={req_id} net={row["net_amount"]}')
    push_notification(row['user_id'], 'withdraw',
        f'출금이 완료됐어요! {row["bank_name"]} {row["account_number"]}로 {int(row["net_amount"]):,}원을 보내드렸어요.')
    return jsonify({'ok': True})


@app.route('/api/admin/withdrawals/<int:req_id>/reject', methods=['POST'])
@admin_required
@csrf_protect
def api_admin_withdraw_reject(req_id):
    conn = get_db(); c = conn.cursor()
    row = c.execute('SELECT * FROM withdrawal_requests WHERE id=?', (req_id,)).fetchone()
    if not row or row['status'] != 'pending':
        conn.close(); return jsonify({'error': '처리할 수 없는 상태예요'}), 400
    c.execute('UPDATE wallet SET balance=balance+? WHERE user_id=?', (row['amount'], row['user_id']))
    c.execute("UPDATE withdrawal_requests SET status='rejected', decided_at=?, decided_by=? WHERE id=?",
              (now_iso(), session.get('admin_username'), req_id))
    conn.commit(); conn.close()
    write_audit('admin', session.get('admin_username'), 'reject_withdraw', f'id={req_id}')
    push_notification(row['user_id'], 'withdraw', f'출금 요청 {int(row["amount"]):,}원이 거절돼서 잔액으로 돌려드렸어요.', {'wallet_changed': True})
    return jsonify({'ok': True})




@app.route('/api/admin/bank-accounts')
@admin_required
def api_admin_bank_accounts():
    conn = get_db()
    rows = conn.execute('SELECT * FROM bank_accounts ORDER BY id DESC').fetchall()
    conn.close()
    return jsonify({'accounts': [dict(r) for r in rows]})


@app.route('/api/admin/bank-accounts', methods=['POST'])
@admin_required
@csrf_protect
def api_admin_add_bank_account():
    data = request.get_json(force=True)
    bank_name = (data.get('bank_name') or '').strip()
    account_number = (data.get('account_number') or '').strip()
    holder_name = (data.get('holder_name') or '').strip()
    if not (bank_name and account_number and holder_name):
        return jsonify({'error': '은행명/계좌번호/예금주를 모두 입력해주세요'}), 400
    conn = get_db(); c = conn.cursor()
    c.execute('INSERT INTO bank_accounts(bank_name,account_number,holder_name,is_active,created_at) VALUES (?,?,?,0,?)',
              (bank_name, account_number, holder_name, now_iso()))
    conn.commit(); conn.close()
    write_audit('admin', session.get('admin_username'), 'add_bank_account', f'{bank_name} {account_number}')
    return jsonify({'ok': True})


@app.route('/api/admin/bank-accounts/<int:acc_id>/activate', methods=['POST'])
@admin_required
@csrf_protect
def api_admin_activate_bank_account(acc_id):
    conn = get_db(); c = conn.cursor()
    c.execute('UPDATE bank_accounts SET is_active=0')
    c.execute('UPDATE bank_accounts SET is_active=1 WHERE id=?', (acc_id,))
    conn.commit(); conn.close()
    write_audit('admin', session.get('admin_username'), 'activate_bank_account', f'id={acc_id}')
    return jsonify({'ok': True})


@app.route('/api/admin/audit-log')
@admin_required
def api_admin_audit_log():
    conn = get_db()
    rows = conn.execute('SELECT * FROM audit_log ORDER BY id DESC LIMIT 100').fetchall()
    conn.close()
    return jsonify({'logs': [dict(r) for r in rows]})


@app.route('/api/admin/system-info')
@admin_required
def api_admin_system_info():
    """지금 이 서버가 정확히 어떤 market.db 파일을 쓰고 있는지 보여줘요.
    "분명히 상품을 추가했는데 사라졌다" 같은 문제는 대부분 zip을 여러 번 풀거나
    다른 폴더에서 서버를 실행해서, 실제로는 서로 다른 market.db를 보고 있는 경우예요."""
    exists = os.path.exists(DB_PATH)
    conn = get_db()
    model_count = conn.execute('SELECT COUNT(*) c FROM models').fetchone()['c']
    user_count = conn.execute('SELECT COUNT(*) c FROM users').fetchone()['c']
    conn.close()
    return jsonify({
        'app_py_path': os.path.abspath(__file__),
        'db_path': os.path.abspath(DB_PATH),
        'db_exists': exists,
        'db_size_kb': round(os.path.getsize(DB_PATH) / 1024, 1) if exists else 0,
        'db_modified_at': datetime.fromtimestamp(os.path.getmtime(DB_PATH), tz=timezone.utc).isoformat() if exists else None,
        'server_started_at': SERVER_STARTED_AT,
        'model_count': model_count,
        'user_count': user_count,
        'old_location_backup_exists': os.path.exists(_OLD_DB_PATH),
        'old_location_path': _OLD_DB_PATH,
    })


# ---------------- 공지사항 ----------------

@app.route('/api/announcements')
def api_announcements():
    conn = get_db()
    rows = conn.execute('SELECT * FROM announcements ORDER BY pinned DESC, id DESC LIMIT 20').fetchall()
    conn.close()
    return jsonify({'announcements': [dict(r) for r in rows]})


# ---------------- 사업자 정보 ----------------

@app.route('/api/business-info')
def api_business_info():
    conn = get_db()
    rows = conn.execute('SELECT * FROM business_info').fetchall()
    conn.close()
    return jsonify({r['key']: r['value'] for r in rows})


@app.route('/api/admin/business-info', methods=['POST'])
@admin_required
@csrf_protect
def api_admin_update_business_info():
    data = request.get_json(force=True)
    conn = get_db(); c = conn.cursor()
    for key, value in data.items():
        c.execute('INSERT OR REPLACE INTO business_info(key,value) VALUES (?,?)', (key, str(value)[:300]))
    conn.commit(); conn.close()
    write_audit('admin', session.get('admin_username'), 'update_business_info', str(list(data.keys())))
    return jsonify({'ok': True})


@app.route('/api/admin/announcements', methods=['POST'])
@admin_required
@csrf_protect
def api_admin_create_announcement():
    data = request.get_json(force=True)
    title = (data.get('title') or '').strip()[:200]
    body = (data.get('body') or '').strip()[:2000]
    pinned = 1 if data.get('pinned') else 0
    if not title:
        return jsonify({'error': '제목을 입력해주세요'}), 400
    conn = get_db(); c = conn.cursor()
    c.execute('INSERT INTO announcements(title,body,pinned,created_at) VALUES (?,?,?,?)',
              (title, body, pinned, now_iso()))
    conn.commit(); conn.close()
    write_audit('admin', session.get('admin_username'), 'create_announcement', title)
    broadcast_notification('announcement', f'새 공지: {title}')
    return jsonify({'ok': True})


@app.route('/api/admin/announcements/<int:ann_id>', methods=['DELETE'])
@admin_required
@csrf_protect
def api_admin_delete_announcement(ann_id):
    conn = get_db(); c = conn.cursor()
    c.execute('DELETE FROM announcements WHERE id=?', (ann_id,))
    conn.commit(); conn.close()
    write_audit('admin', session.get('admin_username'), 'delete_announcement', f'id={ann_id}')
    return jsonify({'ok': True})


# ---------------- 관리자: 재고 관리 ----------------

@app.route('/api/admin/inventory')
@admin_required
def api_admin_inventory():
    conn = get_db()
    models = conn.execute('SELECT * FROM models').fetchall()
    result = []
    for m in models:
        stock = {r['grade']: r['qty'] for r in conn.execute(
            'SELECT grade,qty FROM stock WHERE model_key=?', (m['key'],)).fetchall()}
        result.append({'key': m['key'], 'code': m['code'], 'name': m['name'], 'stock': stock})
    conn.close()
    return jsonify({'inventory': result})


# ---------------- 관리자: 매각위기(보관료 위험) 아이템 관리 ----------------

@app.route('/api/admin/at-risk')
@admin_required
def api_admin_at_risk():
    conn = get_db()
    rows = conn.execute('''
        SELECT p.*, u.email FROM portfolio p JOIN users u ON p.user_id=u.id
        ORDER BY p.ts ASC
    ''').fetchall()
    result = []
    for item in rows:
        m = conn.execute('SELECT * FROM models WHERE key=?', (item['model_key'],)).fetchone()
        if not m:
            continue
        fee = calc_storage_fee(item['ts'], get_plus_status(item['user_id'])['active'])
        value = price_for(m, item['grade'], 'buy')
        ratio = (fee / value) if value > 0 else 0
        level = 2 if ratio >= 1.0 else (1 if ratio >= 0.7 else 0)
        if level > 0:
            days = int((datetime.now(timezone.utc) - datetime.fromisoformat(item['ts']).replace(tzinfo=timezone.utc)).total_seconds() // 86400)
            result.append({
                'id': item['id'], 'email': item['email'], 'model_key': item['model_key'], 'grade': item['grade'],
                'days_stored': days, 'storage_fee': fee, 'current_value': value,
                'net_if_liquidated': max(0, value - fee), 'ratio': round(ratio*100), 'level': level,
                'risk_notified': item['risk_notified'],
            })
    conn.close()
    result.sort(key=lambda x: -x['ratio'])
    return jsonify({'items': result})


@app.route('/api/admin/portfolio/<item_id>/liquidate', methods=['POST'])
@admin_required
@csrf_protect
def api_admin_liquidate(item_id):
    """관리자가 방치된 위험 아이템을 강제로 매도 처리해요. 보관료를 뺀 나머지(0원 이상)만 유저에게 지급돼요."""
    conn = get_db(); c = conn.cursor()
    item = c.execute('SELECT * FROM portfolio WHERE id=?', (item_id,)).fetchone()
    if not item:
        conn.close(); return jsonify({'error': 'not found'}), 404
    m = c.execute('SELECT * FROM models WHERE key=?', (item['model_key'],)).fetchone()
    price = price_for(m, item['grade'], 'buy')
    storage_fee = calc_storage_fee(item['ts'], get_plus_status(item['user_id'])['active'])
    net = max(0, price - storage_fee)
    ts = now_iso()
    c.execute('UPDATE wallet SET balance=balance+? WHERE user_id=?', (net, item['user_id']))
    c.execute('DELETE FROM portfolio WHERE id=?', (item_id,))
    c.execute('INSERT INTO trades(user_id,model_key,grade,side,price,label,ts,pending) VALUES (?,?,?,?,?,?,?,1)',
              (item['user_id'], item['model_key'], item['grade'], 'buy_from_user', price, '관리자 강제매각', ts))
    c.execute('UPDATE models SET internal_trade_count=internal_trade_count+1 WHERE key=?', (item['model_key'],))
    if storage_fee > 0:
        c.execute('INSERT INTO revenue_events(type,amount,user_id,ts) VALUES (?,?,?,?)',
                   ('storage_fee', min(storage_fee, price), item['user_id'], ts))
    conn.commit()
    user_row = c.execute('SELECT email FROM users WHERE id=?', (item['user_id'],)).fetchone()
    conn.close()
    if user_row:
        send_email(user_row['email'], '[콩나물] 보관 중이던 상품이 매각 처리됐어요',
                    f'{item["model_key"]} {item["grade"]}급 상품의 보관료가 물건 가치를 초과해서, 안내드린 대로 매각 처리했어요.\n'
                    f'매도가 {int(price):,}원에서 보관료 {int(storage_fee):,}원을 제외한 {int(net):,}원이 잔액에 입금됐어요.')
    push_notification(item['user_id'], 'liquidate', f'보관 중이던 {item["model_key"]} {item["grade"]}급 상품이 매각 처리됐어요. {int(net):,}원이 입금됐어요.', {'wallet_changed': True})
    write_audit('admin', session.get('admin_username'), 'liquidate_portfolio_item',
                f'id={item_id} user_id={item["user_id"]} net={net}')
    return jsonify({'ok': True, 'net': net})


@app.route('/api/admin/inventory/set', methods=['POST'])
@admin_required
@csrf_protect
def api_admin_inventory_set():
    data = request.get_json(force=True)
    model_key = data.get('model')
    grade = data.get('grade')
    try:
        qty = int(data.get('qty'))
    except (TypeError, ValueError):
        return jsonify({'error': '수량이 올바르지 않아요'}), 400
    if grade not in GRADES or qty < 0:
        return jsonify({'error': '잘못된 요청이에요'}), 400
    conn = get_db(); c = conn.cursor()
    m = c.execute('SELECT key FROM models WHERE key=?', (model_key,)).fetchone()
    if not m:
        conn.close(); return jsonify({'error': '존재하지 않는 모델이에요'}), 400
    prev = c.execute('SELECT qty FROM stock WHERE model_key=? AND grade=?', (model_key, grade)).fetchone()
    c.execute('UPDATE stock SET qty=? WHERE model_key=? AND grade=?', (qty, model_key, grade))
    notify_wishlist_if_restocked(c, model_key, grade, prev and prev['qty'] == 0 and qty > 0)
    conn.commit(); conn.close()
    write_audit('admin', session.get('admin_username'), 'inventory_set', f'{model_key} {grade} -> {qty}')
    return jsonify({'ok': True})


# ---------------- 관리자: 시세(차트) 직접 조작 ----------------

@app.route('/api/admin/models/<model_key>/set-mid', methods=['POST'])
@admin_required
@csrf_protect
def api_admin_set_mid(model_key):
    data = request.get_json(force=True)
    try:
        mid = float(data.get('mid'))
    except (TypeError, ValueError):
        return jsonify({'error': '시세 값이 올바르지 않아요'}), 400
    conn = get_db(); c = conn.cursor()
    m = c.execute('SELECT * FROM models WHERE key=?', (model_key,)).fetchone()
    if not m:
        conn.close(); return jsonify({'error': '존재하지 않는 모델이에요'}), 400
    if mid <= 0:
        conn.close(); return jsonify({'error': '시세는 0보다 커야 해요'}), 400
    # 관리자가 직접 지정하는 값이니 floor/ceil 범위도 필요하면 함께 넓혀줘요
    new_floor = min(m['floor_p'], mid * 0.7)
    new_ceil = max(m['ceil_p'], mid * 1.3)
    c.execute('UPDATE models SET mid=?, floor_p=?, ceil_p=? WHERE key=?', (mid, new_floor, new_ceil, model_key))
    c.execute('INSERT INTO history(model_key,mid,ts) VALUES (?,?,?)', (model_key, mid, now_iso()))
    ids = [r['id'] for r in c.execute('SELECT id FROM history WHERE model_key=? ORDER BY id DESC', (model_key,)).fetchall()]
    if len(ids) > 20:
        c.executemany('DELETE FROM history WHERE id=?', [(i,) for i in ids[20:]])
    conn.commit(); conn.close()
    write_audit('admin', session.get('admin_username'), 'set_mid_price', f'{model_key} -> {mid}')
    return jsonify({'ok': True, 'mid': mid})


# ---------------- 관리자: 상품 관리 (모델 추가/비활성화) ----------------

@app.route('/api/admin/models')
@admin_required
def api_admin_list_models():
    conn = get_db()
    rows = conn.execute('SELECT * FROM models ORDER BY key').fetchall()
    conn.close()
    return jsonify({'models': [dict(r) for r in rows]})


@app.route('/api/admin/models', methods=['POST'])
@admin_required
@csrf_protect
def api_admin_create_model():
    data = request.get_json(force=True)
    key = re.sub(r'[^a-z0-9_]', '', (data.get('key') or '').strip().lower())
    code = (data.get('code') or '').strip()[:20]
    name = (data.get('name') or '').strip()[:100]
    try:
        mid = float(data.get('mid'))
        ratio_s = float(data.get('ratio_s', 1.2))
        ratio_a = float(data.get('ratio_a', 1.0))
        ratio_b = float(data.get('ratio_b', 0.8))
        ratio_c = float(data.get('ratio_c', 0.55))
    except (TypeError, ValueError):
        return jsonify({'error': '숫자 값이 올바르지 않아요'}), 400
    if not key or not code or not name:
        return jsonify({'error': '상품 키/코드/이름을 모두 입력해주세요'}), 400
    if mid <= 0:
        return jsonify({'error': '기준가는 0보다 커야 해요'}), 400
    conn = get_db(); c = conn.cursor()
    existing = c.execute('SELECT key FROM models WHERE key=?', (key,)).fetchone()
    if existing:
        conn.close(); return jsonify({'error': '이미 존재하는 상품 키예요'}), 400
    floor_p, ceil_p = mid * 0.6, mid * 1.5
    c.execute('''INSERT INTO models(key,code,name,ratio_s,ratio_a,ratio_b,ratio_c,mid,floor_p,ceil_p,
                 internal_trade_count,is_active,drop_start,drop_end)
                 VALUES (?,?,?,?,?,?,?,?,?,?,0,1,NULL,NULL)''',
              (key, code, name, ratio_s, ratio_a, ratio_b, ratio_c, mid, floor_p, ceil_p))
    c.execute('INSERT INTO history(model_key,mid,ts) VALUES (?,?,?)', (key, mid, now_iso()))
    for g in GRADES:
        c.execute('INSERT INTO stock VALUES (?,?,0)', (key, g))
    c.execute('INSERT INTO external_ref VALUES (?,?,?,?)', (key, mid, '관리자 등록 초기값', now_iso()))
    conn.commit(); conn.close()
    write_audit('admin', session.get('admin_username'), 'create_model', f'{key} ({name})')
    return jsonify({'ok': True, 'key': key})


@app.route('/api/admin/models/<model_key>/toggle-active', methods=['POST'])
@admin_required
@csrf_protect
def api_admin_toggle_model_active(model_key):
    conn = get_db(); c = conn.cursor()
    m = c.execute('SELECT is_active FROM models WHERE key=?', (model_key,)).fetchone()
    if not m:
        conn.close(); return jsonify({'error': '존재하지 않는 상품이에요'}), 400
    new_state = 0 if m['is_active'] else 1
    c.execute('UPDATE models SET is_active=? WHERE key=?', (new_state, model_key))
    conn.commit(); conn.close()
    write_audit('admin', session.get('admin_username'), 'toggle_model_active', f'{model_key} -> {"활성" if new_state else "비활성"}')
    return jsonify({'ok': True, 'is_active': bool(new_state)})


@app.route('/api/admin/models/<model_key>/set-drop', methods=['POST'])
@admin_required
@csrf_protect
def api_admin_set_drop(model_key):
    data = request.get_json(force=True)
    conn = get_db(); c = conn.cursor()
    m = c.execute('SELECT key FROM models WHERE key=?', (model_key,)).fetchone()
    if not m:
        conn.close(); return jsonify({'error': '존재하지 않는 상품이에요'}), 400
    if data.get('clear'):
        c.execute('UPDATE models SET drop_start=NULL, drop_end=NULL WHERE key=?', (model_key,))
        conn.commit(); conn.close()
        write_audit('admin', session.get('admin_username'), 'clear_drop', model_key)
        return jsonify({'ok': True})
    drop_start = data.get('drop_start')
    drop_end = data.get('drop_end')
    try:
        if drop_start: datetime.fromisoformat(drop_start)
        if drop_end: datetime.fromisoformat(drop_end)
    except ValueError:
        conn.close(); return jsonify({'error': '시간 형식이 올바르지 않아요 (ISO 형식 필요)'}), 400
    c.execute('UPDATE models SET drop_start=?, drop_end=? WHERE key=?', (drop_start, drop_end, model_key))
    conn.commit(); conn.close()
    write_audit('admin', session.get('admin_username'), 'set_drop', f'{model_key} {drop_start} ~ {drop_end}')
    return jsonify({'ok': True})


# ---------------- 관리자: 운영 설정 (스프레드/수수료/갱신주기) ----------------

SETTINGS_BOUNDS = {
    'buy_spread': (0, 0.5), 'sell_markup': (0, 0.5),
    'storage_free_days': (0, 365), 'storage_fee_per_month': (0, 1000000),
    'delivery_base_fee': (0, 1000000), 'remote_surcharge': (0, 1000000),
    'tick_seconds': (5, 86400), 'instant_withdraw_fee': (0, 0.2),
    'plus_monthly_fee': (0, 100000), 'fast_track_fee': (0, 100000),
    'certificate_fee': (0, 50000), 'gift_service_fee': (0, 50000),
    'card_payment_enabled': (0, 1), 'instant_withdraw_enabled': (0, 1),
}

@app.route('/api/admin/settings')
@admin_required
def api_admin_settings():
    return jsonify({'settings': {k: get_setting(k) for k in DEFAULT_SETTINGS}})


@app.route('/api/admin/settings', methods=['POST'])
@admin_required
@csrf_protect
def api_admin_settings_update():
    data = request.get_json(force=True)
    updated = {}
    for key, value in data.items():
        if key not in DEFAULT_SETTINGS:
            continue
        try:
            v = float(value)
        except (TypeError, ValueError):
            return jsonify({'error': f'{key} 값이 올바르지 않아요'}), 400
        lo, hi = SETTINGS_BOUNDS[key]
        if not (lo <= v <= hi):
            return jsonify({'error': f'{key} 값은 {lo}~{hi} 범위여야 해요'}), 400
        set_setting(key, v)
        updated[key] = v
    write_audit('admin', session.get('admin_username'), 'update_settings', str(updated))
    return jsonify({'ok': True, 'settings': {k: get_setting(k) for k in DEFAULT_SETTINGS}})


# ---------------- 관리자: 매입 신청(검수) 관리 ----------------

@app.route('/api/admin/sell-requests')
@admin_required
def api_admin_sell_requests():
    status = request.args.get('status')
    conn = get_db()
    q = '''SELECT s.*, u.email FROM sell_requests s LEFT JOIN users u ON s.user_id=u.id'''
    params = []
    if status:
        q += ' WHERE s.status=?'
        params.append(status)
    q += ' ORDER BY s.is_fast_track DESC, s.id DESC LIMIT 100'
    rows = conn.execute(q, params).fetchall()
    conn.close()
    return jsonify({'requests': [dict(r) for r in rows]})


@app.route('/api/admin/sell-requests/<int:req_id>/receive', methods=['POST'])
@admin_required
@csrf_protect
def api_admin_sell_receive(req_id):
    conn = get_db(); c = conn.cursor()
    row = c.execute('SELECT * FROM sell_requests WHERE id=?', (req_id,)).fetchone()
    if not row or row['status'] != 'submitted':
        conn.close(); return jsonify({'error': '처리할 수 없는 상태예요'}), 400
    c.execute('UPDATE sell_requests SET status=?, updated_at=? WHERE id=?', ('received', now_iso(), req_id))
    conn.commit(); conn.close()
    write_audit('admin', session.get('admin_username'), 'sell_receive', f'id={req_id}')
    push_notification(row['user_id'], 'sell_status', f'매입 신청하신 {row["model_key"]} {row["self_grade"]}급 상품을 수령했어요. 검수를 시작할게요.')
    return jsonify({'ok': True})


@app.route('/api/admin/sell-requests/<int:req_id>/inspect', methods=['POST'])
@admin_required
@csrf_protect
def api_admin_sell_inspect(req_id):
    data = request.get_json(force=True)
    final_grade = data.get('final_grade')
    admin_note = (data.get('admin_note') or '').strip()[:500]
    if final_grade not in GRADES:
        return jsonify({'error': '등급을 선택해주세요'}), 400
    conn = get_db(); c = conn.cursor()
    row = c.execute('SELECT * FROM sell_requests WHERE id=?', (req_id,)).fetchone()
    if not row or row['status'] != 'received':
        conn.close(); return jsonify({'error': '처리할 수 없는 상태예요'}), 400
    m = c.execute('SELECT * FROM models WHERE key=?', (row['model_key'],)).fetchone()
    final_price = price_for(m, final_grade, 'buy', get_user_tier(row["user_id"])['discount'])
    c.execute('UPDATE sell_requests SET status=?, final_grade=?, final_price=?, admin_note=?, updated_at=? WHERE id=?',
              ('inspected', final_grade, final_price, admin_note, now_iso(), req_id))
    conn.commit(); conn.close()
    write_audit('admin', session.get('admin_username'), 'sell_inspect', f'id={req_id} grade={final_grade} price={final_price}')
    push_notification(row['user_id'], 'sell_status', f'검수가 끝났어요! 최종 {final_grade}급, {int(final_price):,}원 정산 예정이에요.')
    return jsonify({'ok': True, 'final_price': final_price})


@app.route('/api/admin/sell-requests/<int:req_id>/payout', methods=['POST'])
@admin_required
@csrf_protect
def api_admin_sell_payout(req_id):
    conn = get_db(); c = conn.cursor()
    row = c.execute('SELECT * FROM sell_requests WHERE id=?', (req_id,)).fetchone()
    if not row or row['status'] != 'inspected':
        conn.close(); return jsonify({'error': '처리할 수 없는 상태예요'}), 400
    ts = now_iso()
    ft_fee = row['fast_track_fee'] or 0
    net_paid = max(0, row['final_price'] - ft_fee)
    c.execute('UPDATE wallet SET balance=balance+? WHERE user_id=?', (net_paid, row['user_id']))
    c.execute('INSERT INTO trades(user_id,model_key,grade,side,price,label,ts,pending) VALUES (?,?,?,?,?,?,?,1)',
              (row['user_id'], row['model_key'], row['final_grade'], 'buy_from_user', row['final_price'], '매입체결(검수완료)', ts))
    c.execute('UPDATE models SET internal_trade_count=internal_trade_count+1 WHERE key=?', (row['model_key'],))
    c.execute('UPDATE stats SET today_trades=today_trades+1, total_inspected=total_inspected+1 WHERE id=1')
    c.execute('UPDATE sell_requests SET status=?, updated_at=? WHERE id=?', ('paid', ts, req_id))
    if ft_fee > 0:
        c.execute('INSERT INTO revenue_events(type,amount,user_id,ts) VALUES (?,?,?,?)',
                   ('fast_track_fee', ft_fee, row['user_id'], ts))
    # 매입 완료된 실물은 클리닝/재포장 후 판매 가능 재고로 편입돼요
    prev_stock = c.execute('SELECT qty FROM stock WHERE model_key=? AND grade=?', (row['model_key'], row['final_grade'])).fetchone()
    c.execute('UPDATE stock SET qty=qty+1 WHERE model_key=? AND grade=?', (row['model_key'], row['final_grade']))
    notify_wishlist_if_restocked(c, row['model_key'], row['final_grade'], prev_stock and prev_stock['qty'] == 0)
    conn.commit(); conn.close()
    write_audit('admin', session.get('admin_username'), 'sell_payout', f'id={req_id} amount={row["final_price"]} fast_track_fee={ft_fee}')
    write_audit('admin', session.get('admin_username'), 'inventory_auto_add', f'{row["model_key"]} {row["final_grade"]} +1 (from sell_request #{req_id})')
    push_notification(row['user_id'], 'sell_status',
        f'정산 완료! {int(net_paid):,}원이 잔액에 입금됐어요.' + (f' (빠른처리 수수료 {int(ft_fee):,}원 차감)' if ft_fee else ''), {'wallet_changed': True})
    return jsonify({'ok': True, 'paid': net_paid})


@app.route('/api/admin/sell-requests/<int:req_id>/reject', methods=['POST'])
@admin_required
@csrf_protect
def api_admin_sell_reject(req_id):
    data = request.get_json(force=True, silent=True) or {}
    reason = (data.get('reason') or '').strip()[:500]
    conn = get_db(); c = conn.cursor()
    row = c.execute('SELECT * FROM sell_requests WHERE id=?', (req_id,)).fetchone()
    if not row or row['status'] in ('paid', 'rejected', 'cancelled'):
        conn.close(); return jsonify({'error': '처리할 수 없는 상태예요'}), 400
    c.execute('UPDATE sell_requests SET status=?, admin_note=?, updated_at=? WHERE id=?',
              ('rejected', reason, now_iso(), req_id))
    conn.commit(); conn.close()
    write_audit('admin', session.get('admin_username'), 'sell_reject', f'id={req_id} reason={reason}')
    push_notification(row['user_id'], 'sell_status', f'매입 신청이 반려됐어요.' + (f' 사유: {reason}' if reason else ''))
    return jsonify({'ok': True})


@app.route('/admin')
def admin_page():
    return app.send_static_file('admin.html')


@app.route('/')
def index():
    return app.send_static_file('index.html')


# ---------------- 앱 초기화 ----------------
# 예전엔 이 블록이 `if __name__ == '__main__':` 안에 있었는데, 그러면 `python3 app.py`로
# 직접 실행할 때만 동작하고 gunicorn처럼 모듈을 import해서 쓰는 프로덕션 서버에서는
# init_db()도, 시세갱신 스케줄러도 아예 실행되지 않는 문제가 있었어요. 그래서 모듈이
# import되는 시점에 항상 실행되도록 옮겼습니다 (개발 서버 실행이든 gunicorn이든 동일하게 동작).
init_db()
load_settings_cache()

# gunicorn을 여러 워커(-w 2 이상)로 띄우면 워커마다 이 스케줄러 스레드가 각각 떠서 시세가
# 중복으로 갱신돼요. 이 앱은 SQLite를 쓰기 때문에 애초에 워커를 1개(-w 1 --threads N)로
# 띄우는 걸 권장하고, 정말 워커를 늘려야 한다면 AIRMRKT_ENABLE_SCHEDULER=0으로 나머지
# 워커의 스케줄러를 꺼서 딱 한 곳에서만 돌게 하세요.
if os.environ.get('AIRMRKT_ENABLE_SCHEDULER', '1') == '1':
    threading.Thread(target=scheduler_loop, daemon=True).start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
