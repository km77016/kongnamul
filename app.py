"""
AIRMRKT 백엔드 API 서버
- Flask + SQLite (둘 다 별도 설치 없이 동작)
- 30초마다(운영시엔 원하는 주기로 조정) 자체 거래 + 외부 참고 시세를 블렌딩해 시세를 갱신
- 프론트엔드(static/index.html)를 같은 서버에서 서빙

실행: python app.py  ->  http://localhost:5000
"""
import sqlite3, time, random, threading, os, uuid, re, smtplib, secrets
from datetime import datetime, timezone, timedelta
from email.mime.text import MIMEText
from flask import Flask, jsonify, request, session
from werkzeug.security import generate_password_hash, check_password_hash

DB_PATH = os.path.join(os.path.dirname(__file__), 'market.db')


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


def calc_storage_fee(ts_iso):
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
    if fresh:
        for k, m in MODEL_DEFAULTS.items():
            c.execute('INSERT INTO models VALUES (?,?,?,?,?,?,?,?,?,?,0)',
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
    conn.commit()
    conn.close()


def send_verification_email(to_email, code):
    """
    SMTP_HOST/SMTP_USER/SMTP_PASS 환경변수가 설정되어 있으면 실제로 이메일을 발송합니다.
    설정되어 있지 않으면 DEV_MODE로 서버 콘솔에만 출력합니다 (로컬 테스트용).
    반환값: (sent: bool, mode: 'sent'|'dev_mode'|'error')
    """
    if DEV_MODE:
        print(f'[DEV MODE] {to_email} 로 보낼 인증코드: {code}  (실제 발송하려면 SMTP_HOST/SMTP_USER/SMTP_PASS 환경변수를 설정하세요)')
        return False, 'dev_mode'
    try:
        msg = MIMEText(f'콩나물 인증코드: {code}\n{CODE_TTL_MINUTES}분 이내에 입력해주세요.')
        msg['Subject'] = '[콩나물] 이메일 인증코드'
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


def price_for(model_row, grade, side):
    ratio = {'S': model_row['ratio_s'], 'A': model_row['ratio_a'],
             'B': model_row['ratio_b'], 'C': model_row['ratio_c']}[grade]
    base = model_row['mid'] * ratio
    if side == 'buy':   # 우리가 매입
        return round(base * (1 - get_setting('buy_spread')) / 500) * 500
    return round(base * (1 + get_setting('sell_markup')) / 500) * 500  # 우리가 판매


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
        c.execute('UPDATE stock SET qty=qty+1 WHERE model_key=? AND grade=?', (key, grade))
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
    models = {}
    for m in c.execute('SELECT * FROM models').fetchall():
        hist = [r['mid'] for r in c.execute(
            'SELECT mid FROM history WHERE model_key=? ORDER BY id ASC', (m['key'],)).fetchall()]
        stock = {r['grade']: r['qty'] for r in c.execute(
            'SELECT grade,qty FROM stock WHERE model_key=?', (m['key'],)).fetchall()}
        ref = c.execute('SELECT * FROM external_ref WHERE model_key=?', (m['key'],)).fetchone()
        prices = {g: {'buy': price_for(m, g, 'buy'), 'sell': price_for(m, g, 'sell')} for g in GRADES}
        models[m['key']] = {
            'code': m['code'], 'name': m['name'], 'mid': m['mid'], 'history': hist,
            'stock': stock, 'prices': prices,
            'external_ref': {'avg': ref['avg'], 'note': ref['note'], 'updated_at': ref['updated_at']},
            'internal_weight_pct': round((1 - external_weight(m['internal_trade_count'])) * 100),
        }
    feed = [dict(r) for r in c.execute(
        'SELECT model_key,grade,side,price,label,ts FROM trades ORDER BY id DESC LIMIT 10').fetchall()]

    uid = current_user_id()
    user_info = None
    wallet_balance = None
    portfolio = []
    csrf_token = None
    if uid:
        urow = c.execute('SELECT email, is_suspended FROM users WHERE id=?', (uid,)).fetchone()
        if urow and urow['is_suspended']:
            session.pop('user_id', None)
            urow = None
        if urow:
            user_info = {'email': urow['email']}
            csrf_token = session.get('csrf_token') or issue_csrf_token()
            wrow = c.execute('SELECT balance FROM wallet WHERE user_id=?', (uid,)).fetchone()
            wallet_balance = wrow['balance'] if wrow else 0
            for r in c.execute('SELECT * FROM portfolio WHERE user_id=? ORDER BY ts DESC', (uid,)).fetchall():
                item = dict(r)
                item['storage_fee'] = calc_storage_fee(item['ts'])
                stored_at = datetime.fromisoformat(item['ts'])
                if stored_at.tzinfo is None:
                    stored_at = stored_at.replace(tzinfo=timezone.utc)
                item['days_stored'] = int((datetime.now(timezone.utc) - stored_at).total_seconds() // 86400)
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
    conn.commit(); conn.close()

    session['user_id'] = user_id
    token = issue_csrf_token()
    write_audit('user', email, 'signup')
    return jsonify({'ok': True, 'user': {'email': email}, 'csrf_token': token})


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


@app.route('/api/trade', methods=['POST'])
@login_required
@csrf_protect
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
        stockrow = c.execute('SELECT qty FROM stock WHERE model_key=? AND grade=?', (model_key, grade)).fetchone()
        if not stockrow or stockrow['qty'] <= 0:
            conn.close(); return jsonify({'error': '품절'}), 400
        price = price_for(m, grade, 'sell')
        delivery_fee = 0
        if delivery == 'delivery':
            delivery_fee = get_setting('delivery_base_fee') + (get_setting('remote_surcharge') if region == 'remote' else 0)
        total = price + delivery_fee
        wallet_row = c.execute('SELECT balance FROM wallet WHERE user_id=?', (uid,)).fetchone()
        if not wallet_row or wallet_row['balance'] < total:
            conn.close(); return jsonify({'error': '잔액 부족'}), 400
        c.execute('UPDATE wallet SET balance=balance-? WHERE user_id=?', (total, uid))
        c.execute('UPDATE stock SET qty=qty-1 WHERE model_key=? AND grade=?', (model_key, grade))
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
    price = price_for(m, item['grade'], 'buy')
    storage_fee = calc_storage_fee(item['ts'])
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
    storage_fee = calc_storage_fee(item['ts'])
    delivery_base = get_setting('delivery_base_fee')
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
    conn = get_db(); c = conn.cursor()
    m = c.execute('SELECT * FROM models WHERE key=?', (model_key,)).fetchone()
    if not m or grade not in GRADES:
        conn.close(); return jsonify({'error': 'invalid model/grade'}), 400
    estimated = price_for(m, grade, 'buy')
    ts = now_iso()
    c.execute('''INSERT INTO sell_requests(user_id,model_key,self_grade,note,estimated_price,status,created_at,updated_at)
                 VALUES (?,?,?,?,?,?,?,?)''', (uid, model_key, grade, note, estimated, 'submitted', ts, ts))
    req_id = c.lastrowid
    conn.commit(); conn.close()
    write_audit('user', str(uid), 'sell_request_submit', f'id={req_id} model={model_key} grade={grade}')
    return jsonify({'ok': True, 'id': req_id, 'estimated_price': estimated, 'shipping_address': INSPECTION_ADDRESS})


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
        SELECT u.id, u.email, u.created_at, u.is_suspended,
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
    return jsonify({'ok': True})


@app.route('/api/admin/users/<int:user_id>/unsuspend', methods=['POST'])
@admin_required
@csrf_protect
def api_admin_unsuspend_user(user_id):
    conn = get_db(); c = conn.cursor()
    c.execute('UPDATE users SET is_suspended=0 WHERE id=?', (user_id,))
    conn.commit(); conn.close()
    write_audit('admin', session.get('admin_username'), 'unsuspend_user', f'user_id={user_id}')
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
        spread = sell_total - buy_total
        return {
            'buy_total': buy_total, 'sell_total': sell_total, 'spread': spread,
            'delivery_fee': delivery_fee, 'storage_fee': storage_fee,
            'total_revenue': spread + delivery_fee + storage_fee,
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


# ---------------- 공지사항 ----------------

@app.route('/api/announcements')
def api_announcements():
    conn = get_db()
    rows = conn.execute('SELECT * FROM announcements ORDER BY pinned DESC, id DESC LIMIT 20').fetchall()
    conn.close()
    return jsonify({'announcements': [dict(r) for r in rows]})


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
    c.execute('UPDATE stock SET qty=? WHERE model_key=? AND grade=?', (qty, model_key, grade))
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


# ---------------- 관리자: 운영 설정 (스프레드/수수료/갱신주기) ----------------

SETTINGS_BOUNDS = {
    'buy_spread': (0, 0.5), 'sell_markup': (0, 0.5),
    'storage_free_days': (0, 365), 'storage_fee_per_month': (0, 1000000),
    'delivery_base_fee': (0, 1000000), 'remote_surcharge': (0, 1000000),
    'tick_seconds': (5, 86400),
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
    q += ' ORDER BY s.id DESC LIMIT 100'
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
    final_price = price_for(m, final_grade, 'buy')
    c.execute('UPDATE sell_requests SET status=?, final_grade=?, final_price=?, admin_note=?, updated_at=? WHERE id=?',
              ('inspected', final_grade, final_price, admin_note, now_iso(), req_id))
    conn.commit(); conn.close()
    write_audit('admin', session.get('admin_username'), 'sell_inspect', f'id={req_id} grade={final_grade} price={final_price}')
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
    c.execute('UPDATE wallet SET balance=balance+? WHERE user_id=?', (row['final_price'], row['user_id']))
    c.execute('INSERT INTO trades(user_id,model_key,grade,side,price,label,ts,pending) VALUES (?,?,?,?,?,?,?,1)',
              (row['user_id'], row['model_key'], row['final_grade'], 'buy_from_user', row['final_price'], '매입체결(검수완료)', ts))
    c.execute('UPDATE models SET internal_trade_count=internal_trade_count+1 WHERE key=?', (row['model_key'],))
    c.execute('UPDATE stats SET today_trades=today_trades+1, total_inspected=total_inspected+1 WHERE id=1')
    c.execute('UPDATE sell_requests SET status=?, updated_at=? WHERE id=?', ('paid', ts, req_id))
    # 매입 완료된 실물은 클리닝/재포장 후 판매 가능 재고로 편입돼요
    c.execute('UPDATE stock SET qty=qty+1 WHERE model_key=? AND grade=?', (row['model_key'], row['final_grade']))
    conn.commit(); conn.close()
    write_audit('admin', session.get('admin_username'), 'sell_payout', f'id={req_id} amount={row["final_price"]}')
    write_audit('admin', session.get('admin_username'), 'inventory_auto_add', f'{row["model_key"]} {row["final_grade"]} +1 (from sell_request #{req_id})')
    return jsonify({'ok': True, 'paid': row['final_price']})


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
    return jsonify({'ok': True})


@app.route('/admin')
def admin_page():
    return app.send_static_file('admin.html')


@app.route('/')
def index():
    return app.send_static_file('index.html')


if __name__ == '__main__':
    init_db()
    load_settings_cache()
    threading.Thread(target=scheduler_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=5000, debug=False)
