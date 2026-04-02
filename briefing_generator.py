"""
재테크 트렌드 브리핑 자동 생성기 v2
=================================
기존 풀 피처 HTML (briefing_template.html) 을 베이스로 사용하여
오늘의 핫 콘텐츠(오늘 탭) · 키워드 TOP5 · 뉴스 · 시세만 매일 자동 갱신.
이번주/이번달/3달/6달/1년 패널은 수동 큐레이션 유지.

사용법:
  python3 briefing_generator.py

출력:
  daily_briefing_YYYY-MM-DD.html  ← 로컬 저장
  GitHub Pages: latest.html + briefings/daily_briefing_YYYY-MM-DD.html

쿼터 소모:
  search.list × 15회 = 1,500 units  (하루 한도 10,000)
  videos.list × ~30회 =    30 units
  총 ~1,530 units / 10,000 한도
"""

import urllib.request
import urllib.parse
import json
import re
import base64
import os
from datetime import datetime, timedelta, timezone

# ─── yfinance 선택적 로드 ──────────────────────────
try:
    import yfinance as yf
    HAS_YF = True
except ImportError:
    HAS_YF = False
    print("[경고] yfinance 미설치 — pip install yfinance 후 재실행하면 실시간 시세 반영됨")

# ─── 설정 ──────────────────────────────────────────
GITHUB_API_BASE = 'https://api.github.com'

def _load_config():
    """API 키를 config.json에서 로드 (GitHub에 올라가지 않는 별도 파일)"""
    candidates = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.json'),
        os.path.expanduser('~/Documents/Claude/Briefings/config.json'),
    ]
    for path in candidates:
        if os.path.exists(path):
            with open(path, encoding='utf-8') as f:
                cfg = json.load(f)
            return cfg.get('YOUTUBE_API_KEY', ''), cfg.get('GITHUB_TOKEN', ''), cfg.get('GITHUB_REPO', 'leeho-spec/weolbu-briefing')
    raise RuntimeError('config.json 없음 — ~/Documents/Claude/Briefings/config.json 생성 필요')

YOUTUBE_API_KEY, GITHUB_TOKEN, GITHUB_REPO = _load_config()

# 템플릿 파일 경로 (스크립트와 같은 디렉터리 우선, 없으면 GitHub에서 다운로드)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_CANDIDATES = [
    os.path.join(SCRIPT_DIR, 'briefing_template.html'),
    os.path.expanduser('~/Documents/Claude/Briefings/briefing_template.html'),
]

# 순위 히스토리 JSON (전일대비 비교용)
RANKING_HISTORY_PATH   = os.path.join(SCRIPT_DIR, 'ranking_history.json')
VIDEO_CACHE_PATH       = os.path.join(SCRIPT_DIR, 'video_details_cache.json')
CHANNELS_JSON_PATH     = os.path.join(SCRIPT_DIR, 'channels.json')

# ─── channels.json 로드 (없으면 내장 기본값 사용) ───
def load_channels():
    candidates = [
        CHANNELS_JSON_PATH,
        os.path.expanduser('~/Documents/Claude/Briefings/channels.json'),
    ]
    for path in candidates:
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                raw = json.load(f)
            # _comment 키 제거, Python bool 변환
            return {k: v for k, v in raw.items() if not k.startswith('_')}
    # fallback: 하드코딩 기본값
    print('[경고] channels.json 없음 — 내장 기본값 사용')
    return {
        '소수몽키':         {'id': 'UCC3yfxS5qC6PCwDzetUuEWg', 'weight': 1.5},
        '삼프로TV':         {'id': 'UChlv4GSd7OQl3js-jkLOnFA', 'weight': 1.5},
        '월급쟁이부자들TV': {'id': 'UCDSj40X9FFUAnx1nv7gQhcA', 'weight': 1.0},
    }

CHANNELS = load_channels()

# 재테크 cat_hint 채널 집합 (카테고리 fallback 적용 대상)
FINTECH_HINT_CHANNELS = {ch for ch, info in CHANNELS.items() if info.get('cat_hint') == 'fintech'}

KEYWORDS = [
    {'label': '환율 · 달러',    'query': '환율 달러',                                  'emoji': '💱', 'cat': 'macro'},
    {'label': 'AI · 반도체',    'query': 'AI 반도체',                                  'emoji': '🤖', 'cat': 'stock'},
    {'label': '부동산 규제완화', 'query': '부동산 규제',                                'emoji': '🏠', 'cat': 'realestate'},
    {'label': '미국 경기침체',  'query': '미국 경기침체',                              'emoji': '📉', 'cat': 'macro'},
    {'label': '금리 인하 기대', 'query': '금리 인하',                                  'emoji': '🏦', 'cat': 'macro'},
    {'label': '예적금 · 재테크', 'query': '예금 적금 재테크 ISA IRP ETF 연금저축 청년도약 발행어음 절세', 'emoji': '💰', 'cat': 'fintech'},
]

# 카테고리 레이블 매핑 (뱃지 텍스트)
CAT_LABEL = {
    'stock':       '주식·증시',
    'realestate':  '부동산',
    'macro':       '거시경제',
    'fintech':     '재테크',
    'general':     '일반',
}

MAX_RESULTS_PER_CHANNEL = 2
RECENCY_DAYS = 30

NEWS_SOURCES = [
    # 직접 RSS (안정)
    {'name': '한국경제',  'tag': '한경', 'rss': 'https://www.hankyung.com/feed/economy'},
    {'name': '매일경제',  'tag': '매경', 'rss': 'https://www.mk.co.kr/rss/30000001/'},
    # Google News RSS 경유 (직접 RSS 차단된 소스)
    {'name': '머니투데이', 'tag': '머투', 'rss': 'https://news.google.com/rss/search?q=site%3Anews.mt.co.kr+%EA%B2%BD%EC%A0%9C&hl=ko&gl=KR&ceid=KR%3Ako', 'gnews': True},
    {'name': '조선비즈',   'tag': '조비', 'rss': 'https://news.google.com/rss/search?q=site%3Abiz.chosun.com+%EA%B2%BD%EC%A0%9C&hl=ko&gl=KR&ceid=KR%3Ako', 'gnews': True},
    {'name': '이데일리',   'tag': '이데', 'rss': 'https://news.google.com/rss/search?q=site%3Awww.edaily.co.kr+%EA%B2%BD%EC%A0%9C&hl=ko&gl=KR&ceid=KR%3Ako', 'gnews': True},
]
NEWS_FILTER_KEYWORDS = [
    '환율', '금리', '주식', '코스피', '나스닥', '달러', '부동산', '아파트',
    '반도체', 'AI', '투자', '경제', '증시', '재테크', 'ETF', '펀드',
    '물가', '인플레', '기준금리', '연준', '한은', '채권',
]

MARKET_SYMBOLS = [
    ('KOSPI',    '^KS11',     2, 0.5),
    ('KOSDAQ',   '^KQ11',     2, 0.2),
    ('NASDAQ',   '^IXIC',     1, 8.0),
    ('S&P500',   '^GSPC',     1, 3.0),
    ('DOW',      '^DJI',      1, 20.0),
    ('USD/KRW',  'KRW=X',     2, 0.3),
    ('JPY/KRW',  'JPYKRW=X', 2, 0.02),
    ('CNY/KRW',  'CNYKRW=X', 2, 0.1),
    ('NIKKEI',   '^N225',     1, 15.0),
    ('SHANGHAI', '000001.SS', 1, 5.0),
]


# ─── 날짜 포맷 ──────────────────────────────────────

def korean_date_str(dt=None):
    if dt is None:
        dt = datetime.now()
    weekdays = ['월요일', '화요일', '수요일', '목요일', '금요일', '토요일', '일요일']
    return f"{dt.year}년 {dt.month:02d}월 {dt.day:02d}일 {weekdays[dt.weekday()]}"


# ─── 템플릿 로드 ────────────────────────────────────

def load_template():
    """로컬 파일 우선, 없으면 GitHub에서 다운로드"""
    for path in TEMPLATE_CANDIDATES:
        if os.path.exists(path):
            print(f'[템플릿] 로컬 파일 로드: {path}')
            with open(path, 'r', encoding='utf-8') as f:
                return f.read()

    # GitHub에서 다운로드
    print('[템플릿] GitHub에서 다운로드 중...')
    url = f'{GITHUB_API_BASE}/repos/{GITHUB_REPO}/contents/briefing_template.html'
    req = urllib.request.Request(url, headers={
        'Authorization': f'token {GITHUB_TOKEN}',
        'Accept': 'application/vnd.github.v3+json',
    })
    try:
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read())
            content = base64.b64decode(data['content']).decode('utf-8')
            # 로컬에 캐시
            cache_path = TEMPLATE_CANDIDATES[0]
            os.makedirs(os.path.dirname(cache_path), exist_ok=True)
            with open(cache_path, 'w', encoding='utf-8') as f:
                f.write(content)
            return content
    except Exception as e:
        raise RuntimeError(f'템플릿 로드 실패: {e}')


# ─── YouTube 수집 (RSS 방식 — 쿼터 절감) ──────────────
# search.list 100유닛 × N회 대신 RSS(0유닛) + videos.list(1유닛/50개)만 사용
# 쿼터 소모: 기존 ~5,000유닛 → ~10유닛 (99% 절감)

def yt_get(endpoint, params):
    """YouTube Data API 호출 — videos.list 등 최소한으로만 사용"""
    params['key'] = YOUTUBE_API_KEY
    url = f'https://www.googleapis.com/youtube/v3/{endpoint}?' + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=15) as r:
        return json.loads(r.read())


def fetch_channel_rss(channel_id):
    """YouTube RSS 피드에서 최신 영상 최대 15개 수집 — API 쿼터 0"""
    import xml.etree.ElementTree as ET
    url = f'https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}'
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            root = ET.fromstring(r.read())
    except Exception:
        return []
    ns = {
        'atom': 'http://www.w3.org/2005/Atom',
        'yt':   'http://www.youtube.com/xml/schemas/2015',
    }
    videos = []
    for entry in root.findall('atom:entry', ns):
        vid_id    = entry.findtext('yt:videoId', namespaces=ns)
        title     = entry.findtext('atom:title', namespaces=ns) or ''
        published = (entry.findtext('atom:published', namespaces=ns) or '')[:10]
        if vid_id:
            videos.append({'vid': vid_id, 'title': title, 'published': published})
    return videos


def parse_duration_sec(iso):
    """ISO 8601 duration → 초 변환 (PT1M30S → 90)"""
    m = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', iso or '')
    if not m:
        return 0
    h  = int(m.group(1) or 0)
    mi = int(m.group(2) or 0)
    s  = int(m.group(3) or 0)
    return h * 3600 + mi * 60 + s


def fmt_duration(dur_sec):
    """초 → 간결한 길이 문자열 (예: 23분, 1시간 5분)"""
    if not dur_sec or dur_sec <= 0:
        return ''
    h  = dur_sec // 3600
    mi = (dur_sec % 3600) // 60
    s  = dur_sec % 60
    if h > 0:
        return f'{h}시간 {mi}분' if mi > 0 else f'{h}시간'
    if mi > 0:
        return f'{mi}분'
    return f'{s}초'


def build_meta_html(ch_name, views, days_str, dur_sec=0):
    """hot-row 메타 2줄 구조: 채널명 / 뷰(날짜) ⏱시간"""
    views_str = fmt_views(views) + '뷰' if views and views > 0 else ''
    dur_str   = fmt_duration(dur_sec)
    dur_tag   = f'<span class="hot-dur">⏱ {dur_str}</span>' if dur_str else ''
    date_tag  = f'<span class="hot-date">({days_str})</span>' if days_str else ''
    views_tag = f'<span class="hot-views-num">{views_str}</span>' if views_str else ''
    return (
        f'<div class="hot-ch-name">{ch_name}</div>'
        f'<div class="hot-stats">{views_tag}{date_tag}{dur_tag}</div>'
    )


def get_video_stats(video_ids):
    """videos.list로 조회수·채널명·duration 일괄 조회 — 50개당 1유닛"""
    if not video_ids:
        return {}
    result = {}
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        try:
            data = yt_get('videos', {
                'part': 'snippet,statistics,contentDetails',
                'id':   ','.join(batch),
            })
            for item in data.get('items', []):
                vid      = item['id']
                dur_iso  = item.get('contentDetails', {}).get('duration', '')
                dur_sec  = parse_duration_sec(dur_iso)
                is_short = 15 <= dur_sec < 90   # 숏폼: 15초 이상 ~ 1분30초 미만
                is_live  = item['snippet'].get('liveBroadcastContent', 'none') in ('live', 'upcoming')
                url      = (f'https://www.youtube.com/shorts/{vid}'
                            if is_short
                            else f'https://www.youtube.com/watch?v={vid}')
                result[vid] = {
                    'title':    item['snippet']['title'],
                    'channel':  item['snippet']['channelTitle'],
                    'date':     item['snippet']['publishedAt'][:10],
                    'views':    int(item['statistics'].get('viewCount', 0)),
                    'url':      url,
                    'vid':      vid,
                    'dur_sec':  dur_sec,
                    'is_short': is_short,
                    'is_live':  is_live,
                }
        except Exception as e:
            print(f'  [videos.list] 오류: {e}')
    return result


def collect_shorts_data():
    """모든 채널 RSS → duration ≤60초 Shorts만 추려 스코어 정렬 — 쿼터 ~1유닛"""
    all_ids = []  # (vid, ch_name, weight, published)
    for ch_name, ch_info in CHANNELS.items():
        for v in fetch_channel_rss(ch_info['id']):
            all_ids.append((v['vid'], ch_name, ch_info['weight'], v['published']))

    if not all_ids:
        return []

    # 중복 제거
    seen, unique = set(), []
    for vid, ch_name, weight, pub in all_ids:
        if vid not in seen:
            seen.add(vid)
            unique.append((vid, ch_name, weight, pub))

    # videos.list (contentDetails 포함)
    stats = get_video_stats([v[0] for v in unique])
    ch_map = {vid: (ch_name, weight) for vid, ch_name, weight, pub in unique}

    today = datetime.now(timezone.utc).date()
    scored = []
    for vid, info in stats.items():
        if not info.get('is_short'):
            continue
        if is_live_video(info):           # 라이브/예정/리플레이 제외
            continue
        ch_name, weight = ch_map.get(vid, ('?', 1.0))
        days_old = (today - datetime.fromisoformat(info['date']).date()).days
        if days_old > RECENCY_DAYS:
            continue
        recency = max(0, 1 - days_old / 30)
        score   = info['views'] * weight * (1 + recency)
        scored.append({**info, 'ch_name': ch_name, 'ch_weight': weight, 'score': score})

    return sorted(scored, key=lambda x: x['score'], reverse=True)


# 라이브 방송 리플레이 제목 패턴 (API liveBroadcastContent 우회 케이스 대응)
LIVE_TITLE_PATTERNS = [
    '전체보기', '오전 방송', '오후 방송', '방송 전체', '생방송', '생방',
    '[LIVE]', '(LIVE)', 'LIVE방송', '라이브방송', '실시간방송',
]

def is_live_video(info):
    """라이브 방송 / 라이브 리플레이 여부 판정"""
    if info.get('is_live'):
        return True
    # completed 리플레이: title 패턴으로 감지
    title = info.get('title', '')
    return any(p in title for p in LIVE_TITLE_PATTERNS)


# 키워드 필터링용 — 제목에 이 중 하나 이상 포함되면 관련 영상으로 판단
KEYWORD_TITLE_MAP = {
    '환율 달러':     ['환율', '달러', '원화', '환헤지', '강달러', '약달러', '외환'],
    'AI 반도체':     ['AI', 'ai', '인공지능', '반도체', '엔비디아', 'GPU', '칩', 'HBM', '소부장'],
    '부동산 규제':   ['부동산', '아파트', '청약', '규제', '재건축', '재개발', '전세', '분양', '주택'],
    '미국 경기침체': ['경기침체', '침체', '리세션', '미국 경제', '연준', 'Fed', '관세', '트럼프'],
    '금리 인하':     ['금리', '기준금리', '인하', '인상', '한은', '연준', '채권', '이자'],
    '예금 적금 재테크': ['예금', '적금', '재테크', 'ISA', 'IRP', '연금저축', 'CMA', 'MMF', '이자율', '저축', '파킹통장', '고금리'],
}


def collect_keyword_data(keyword_cfg):
    """RSS로 채널별 최신 영상 수집 → 키워드 필터 → 스코어 정렬"""
    query    = keyword_cfg['query']
    kw_words = KEYWORD_TITLE_MAP.get(query, query.split())
    is_realestate = '부동산' in query or '청약' in query or '전세' in query

    candidate_ids = []  # (vid_id, ch_name, weight)
    for ch_name, ch_info in CHANNELS.items():
        if ch_info.get('finance_only') and is_realestate:
            continue
        rss_videos = fetch_channel_rss(ch_info['id'])
        for v in rss_videos:
            # 제목에 키워드 단어 하나라도 포함되면 후보 추가
            title_upper = v['title'].upper()
            if any(kw.upper() in title_upper for kw in kw_words):
                # 최근 N일 이내만
                if RECENCY_DAYS:
                    try:
                        pub = datetime.fromisoformat(v['published']).date()
                        if (datetime.now().date() - pub).days > RECENCY_DAYS:
                            continue
                    except Exception:
                        pass
                candidate_ids.append((v['vid'], ch_name, ch_info['weight']))

    if not candidate_ids:
        return []

    # 중복 제거 (같은 vid가 여러 채널에서 걸릴 수 있음)
    seen, unique = set(), []
    for vid, ch_name, weight in candidate_ids:
        if vid not in seen:
            seen.add(vid)
            unique.append((vid, ch_name, weight))

    # videos.list로 조회수 일괄 조회 (1유닛/50개)
    stats = get_video_stats([v[0] for v in unique])
    ch_map = {vid: (ch_name, weight) for vid, ch_name, weight in unique}

    today = datetime.now(timezone.utc).date()
    scored = []
    for vid, info in stats.items():
        if info.get('is_short'):           # 쇼츠는 롱폼 키워드 풀에서 제외
            continue
        if is_live_video(info):            # 라이브/예정/리플레이 제외
            continue
        if 0 < info.get('dur_sec', 0) < 120:  # 롱폼 최소 2분 미만 제외 (회색지대 영상)
            continue
        ch_name, weight = ch_map.get(vid, ('?', 1.0))
        days_old = (today - datetime.fromisoformat(info['date']).date()).days
        recency  = max(0, 1 - days_old / 30)   # 30일 지나면 0, 오늘 업로드면 1
        score    = info['views'] * weight * (1 + recency)
        scored.append({**info, 'ch_name': ch_name, 'ch_weight': weight, 'score': score})

    return sorted(scored, key=lambda x: x['score'], reverse=True)


# ─── 시세 수집 ──────────────────────────────────────

def fetch_market_data():
    if not HAS_YF:
        return None
    rows = []
    for name, sym, dec, step in MARKET_SYMBOLS:
        try:
            hist = yf.Ticker(sym).history(period='2d')
            price = round(float(hist['Close'].iloc[-1]), dec)
            prev  = round(float(hist['Close'].iloc[-2]), dec) if len(hist) > 1 else price
            chg   = round(price - prev, dec)
            auto_step = round(max(abs(chg) * 0.05, step * 0.1), dec)
            rows.append(f"    {{ name:'{name}', cur:{price}, base:{price}, dec:{dec}, step:{auto_step} }}")
            print(f'  {name:10s} {price:>12.2f}  ({chg:+.2f})')
        except Exception as e:
            print(f'  [!] {name} 오류: {e}')
            rows.append(f"    {{ name:'{name}', cur:0, base:0, dec:{dec}, step:{step} }}")
    return 'const stocks = [\n' + ',\n'.join(rows) + '\n  ];'


# ─── 뉴스 수집 ──────────────────────────────────────

def _parse_rss_date(pub_date_str):
    """RSS pubDate → 날짜 문자열 YYYY-MM-DD. 실패 시 오늘 날짜."""
    today = datetime.now().strftime('%Y-%m-%d')
    if not pub_date_str:
        return today
    # RFC 2822 예: "Mon, 30 Mar 2026 09:00:00 +0000" or "Mon, 30 Mar 2026 09:00:00 GMT"
    for fmt in ('%a, %d %b %Y %H:%M:%S %z', '%a, %d %b %Y %H:%M:%S GMT',
                '%a, %d %b %Y %H:%M:%S +0000', '%Y-%m-%dT%H:%M:%S%z'):
        try:
            return datetime.strptime(pub_date_str.strip(), fmt).strftime('%Y-%m-%d')
        except Exception:
            pass
    # fallback: extract YYYY-MM-DD-like pattern
    m = re.search(r'(\d{1,2})\s+(\w{3})\s+(\d{4})', pub_date_str)
    if m:
        try:
            return datetime.strptime(f'{m.group(1)} {m.group(2)} {m.group(3)}', '%d %b %Y').strftime('%Y-%m-%d')
        except Exception:
            pass
    return today

def fetch_news(max_per_source=3):
    import xml.etree.ElementTree as ET
    import ssl
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    results = []
    today_str = datetime.now().strftime('%Y-%m-%d')
    for src in NEWS_SOURCES:
        is_gnews = src.get('gnews', False)
        try:
            req = urllib.request.Request(
                src['rss'],
                headers={'User-Agent': 'Mozilla/5.0 (compatible; briefing-bot/1.0)'}
            )
            with urllib.request.urlopen(req, timeout=12, context=ctx) as r:
                root = ET.fromstring(r.read())
            count = 0
            for item in root.findall('.//item'):
                if count >= max_per_source:
                    break
                raw_title = (item.findtext('title') or '').strip()
                # Google News titles end with " - source.domain" — strip it
                title = re.sub(r'\s*-\s*[\w.]+\.(co\.kr|com|kr)\s*$', '', raw_title).strip() if is_gnews else raw_title
                link  = (item.findtext('link') or '').strip()
                desc  = (item.findtext('description') or '').strip()
                desc  = re.sub(r'<[^>]+>', '', desc)[:120].strip()
                pub_raw = (item.findtext('pubDate') or item.findtext(
                    '{http://purl.org/dc/elements/1.1/}date') or '').strip()
                pub_date = _parse_rss_date(pub_raw)
                # Google News 소스는 쿼리 자체가 경제 키워드 포함 → 필터 생략
                passes = is_gnews or any(kw in title for kw in NEWS_FILTER_KEYWORDS)
                if passes:
                    results.append({
                        'source': src['name'], 'tag': src['tag'],
                        'title': title, 'link': link, 'desc': desc,
                        'pub_date': pub_date,
                        'is_today': pub_date == today_str,
                    })
                    count += 1
        except Exception as e:
            print(f'  [뉴스] {src["name"]} 오류: {e}')
    return results[:15]


# ─── HTML 조각 빌더 ─────────────────────────────────

def fmt_views(n):
    if n >= 10000:
        return f'{n//10000}만{(n % 10000)//1000}천'
    if n >= 1000:
        return f'{n//1000}천'
    return str(n)


def make_why(v, rank):
    """스코어 구성요소(조회수·최신성·채널가중치)를 풀어서 설명하는 WHY 텍스트"""
    from datetime import datetime as _dt
    try:
        days_old = (_dt.now().date() - _dt.fromisoformat(v['date']).date()).days
    except Exception:
        days_old = 0
    recency_pct = max(0, round((1 - days_old / 30) * 100))
    score_int   = int(v['score'])

    lines = {
        1: [
            f'→ 조회수 {v["views"]:,}뷰 × 채널 가중치 ×{v["ch_weight"]} × 최신성 {recency_pct}% 적용',
            f'→ 업로드 {v["date"]} ({days_old}일 전) — 동일 기간 수집 영상 중 복합 스코어 1위',
            f'→ 지금 시장 불안과 제목 키워드가 정확히 맞아 클릭·조회 집중',
        ],
        2: [
            f'→ 조회수 {v["views"]:,}뷰 × 채널 가중치 ×{v["ch_weight"]} × 최신성 {recency_pct}% 적용',
            f'→ 업로드 {v["date"]} — 구체적 수치·종목 제목이 클릭 의향을 높였어요',
            f'→ 복합 스코어 {score_int:,}점으로 2위 선정',
        ],
        3: [
            f'→ 조회수 {v["views"]:,}뷰 × 채널 가중치 ×{v["ch_weight"]} × 최신성 {recency_pct}% 적용',
            f'→ 업로드 {v["date"]} — 경쟁 영상 대비 최신성 보정에서 유리',
            f'→ 복합 스코어 {score_int:,}점으로 3위 선정',
        ],
    }
    row = lines.get(rank, lines[3])
    return '<br>\n            '.join(row)


WHY_TEMPLATES = [
    lambda v: make_why(v, 1),
    lambda v: make_why(v, 2),
    lambda v: make_why(v, 3),
]


# ─── 뉴스 키워드 매핑 (키워드별 기본 뉴스 링크) ──────
NEWS_KW_DEFAULTS = {
    '환율 달러':         ('https://www.hankyung.com/finance/exchange',        '한경 — 환율 최신 기사'),
    'AI 반도체':         ('https://www.hankyung.com/it/semiconductor',         '한경 — 반도체 최신 기사'),
    '부동산 규제':       ('https://land.hankyung.com/',                       '한경 — 부동산 최신 기사'),
    '미국 경기침체':     ('https://www.hankyung.com/international/us-economy', '한경 — 미국 경제 기사'),
    '금리 인하':         ('https://www.hankyung.com/economy/bond',             '한경 — 금리·채권 기사'),
    '예금 적금 재테크':  ('https://www.hankyung.com/economy/savings',          '한경 — 예적금·재테크 기사'),
}

def get_news_for_keyword(query, news_items):
    """수집된 뉴스 중 키워드 관련 기사 찾기, 없으면 기본 URL 반환"""
    kw_words = KEYWORD_TITLE_MAP.get(query, query.split())
    for n in news_items:
        if any(kw in n['title'] for kw in kw_words):
            label = f'{n["title"][:55]} — {n["source"]}'
            return n['link'], label, n.get('desc', '')
    url, label = NEWS_KW_DEFAULTS.get(query, ('https://www.hankyung.com/economy', '한경 — 경제 최신 기사'))
    return url, label, ''

def get_news_list_for_keyword(query, news_items, max_count=3):
    """키워드 관련 뉴스 최대 max_count개 반환 — [(link, title, source), ...]"""
    kw_words = KEYWORD_TITLE_MAP.get(query, query.split())
    matched = []
    for n in news_items:
        if any(kw in n['title'] for kw in kw_words):
            matched.append((n['link'], n['title'], n['source']))
        if len(matched) >= max_count:
            break
    if not matched:
        url, label = NEWS_KW_DEFAULTS.get(query, ('https://www.hankyung.com/economy', '한경 — 경제 최신 기사'))
        matched = [(url, label, '한경')]
    return matched


# ─── 숏폼 공통 인사이트 빌더 ────────────────────────
def build_shorts_insight_text(top5_videos):
    titles = ' '.join(v['title'] for v in top5_videos)
    themes = []
    if any(kw in titles for kw in ['환율', '달러', '원화']): themes.append('환율 불안')
    if any(kw in titles for kw in ['반도체', 'AI', '엔비디아']): themes.append('AI·반도체 흐름')
    if any(kw in titles for kw in ['부동산', '아파트', '청약']): themes.append('부동산 동향')
    if any(kw in titles for kw in ['금리', '채권', '한은']): themes.append('금리 변화')
    if any(kw in titles for kw in ['미국', '경기침체', '관세', '트럼프']): themes.append('미국 경제')
    themes = themes[:3] if themes else ['재테크 종합']
    ch_set = list(dict.fromkeys(v['ch_name'] for v in top5_videos))
    top_ch = ch_set[0] if ch_set else '주요 채널'
    theme_str = ' · '.join(themes)
    return (f'이번주 숏폼 공통 테마: {theme_str}. '
            f'{top_ch} 등이 주도하며 높은 조회수를 기록했어요. '
            f'스코어 상위 영상들이 같은 시장 불안을 서로 다른 앵글로 다루는 구조예요.')


# ─── 핫콘텐츠 카드 빌더 (기간별 필터 지원) ──────────────
def build_hot_cards_by_period(top_videos, shorts_videos, max_days=None, prev_ranking=None):
    """롱폼 WHY 카드 3개 + 더보기(4~10위) + 실제 Shorts 5카드 / max_days=None이면 전체"""

    today = datetime.now().date()

    def within(v):
        if max_days is None:
            return True
        try:
            pub = datetime.fromisoformat(v['date']).date()
            return (today - pub).days <= max_days
        except Exception:
            return True

    # 기간 필터 적용
    filtered = [v for v in top_videos if within(v)]
    shorts_filtered = [v for v in shorts_videos if within(v)] if shorts_videos else []

    # ── 롱폼: 숏폼(is_short=True) 완전 제외 ──
    longform = [v for v in filtered if not v.get('is_short')][:3]

    # ── 롱폼 compact rows (TOP 3) ──
    rank_cls  = ['r1', 'r2', 'r3']
    # 1·2·3위: 금은동 메달 (Tossface)
    rank_sym  = [
        '<span class="tossface">🥇</span>',
        '<span class="tossface">🥈</span>',
        '<span class="tossface">🥉</span>',
    ]
    top3_html = ''
    for i, v in enumerate(longform):
        views_str  = fmt_views(v['views']) + '뷰' if v['views'] > 0 else ''
        dur_str    = fmt_duration(v.get('dur_sec', 0))
        delta_html = get_rank_delta_html(v['vid'], i + 1, prev_ranking)
        cat        = v.get('cat', 'general')
        cat_label  = CAT_LABEL.get(cat, '일반')
        try:
            days_old = (today - datetime.fromisoformat(v['date']).date()).days
            days_str = f'{days_old}일 전' if days_old > 0 else '오늘'
        except Exception:
            days_str = v['date']
        meta_html = build_meta_html(v['ch_name'], v['views'], days_str, v.get('dur_sec', 0))
        top3_html += f'''  <a class="hot-row hot-row-top3" href="{v['url']}" target="_blank" data-cat="{cat}" data-rank="{i+1}">
    <span class="hot-rank-num {rank_cls[i]}">{rank_sym[i]}</span>
    <img class="hot-thumb-sm" src="https://img.youtube.com/vi/{v['vid']}/hqdefault.jpg" alt="" loading="lazy">
    <div class="hot-body">
      <div class="hot-title">{v['title'][:60]}</div>
      <div class="hot-meta">{meta_html}</div>
    </div>
    <div class="hot-right">
      {delta_html}
      <span class="cat-badge cat-{cat}">{cat_label}</span>
    </div>
    <div class="thumb-preview">
      <img src="https://img.youtube.com/vi/{v['vid']}/maxresdefault.jpg" onerror="this.src='https://img.youtube.com/vi/{v['vid']}/hqdefault.jpg'" alt="">
      <div class="thumb-preview-title">{v['title'][:55]}</div>
    </div>
  </a>\n'''

    # ── 4~10위 더보기 아코디언 (compact rows) ──
    more_items = [v for v in filtered if not v.get('is_short')][3:10]
    more_rows  = ''
    for j, v in enumerate(more_items, 4):
        views_str  = fmt_views(v['views']) + '뷰' if v['views'] > 0 else ''
        dur_str    = fmt_duration(v.get('dur_sec', 0))
        delta_html = get_rank_delta_html(v['vid'], j, prev_ranking)
        cat        = v.get('cat', 'general')
        cat_label  = CAT_LABEL.get(cat, '일반')
        try:
            days_old = (today - datetime.fromisoformat(v['date']).date()).days
            days_str = f'{days_old}일 전' if days_old > 0 else '오늘'
        except Exception:
            days_str = v['date']
        meta_html = build_meta_html(v['ch_name'], v['views'], days_str, v.get('dur_sec', 0))
        more_rows += f'''  <a class="hot-row" href="{v['url']}" target="_blank" data-cat="{cat}" data-rank="{j}">
    <span class="hot-rank-num rn">{j}위</span>
    <img class="hot-thumb-sm" src="https://img.youtube.com/vi/{v['vid']}/hqdefault.jpg" alt="" loading="lazy">
    <div class="hot-body">
      <div class="hot-title">{v['title'][:60]}</div>
      <div class="hot-meta">{meta_html}</div>
    </div>
    <div class="hot-right">
      {delta_html}
      <span class="cat-badge cat-{cat}">{cat_label}</span>
    </div>
  </a>\n'''

    # ── 급상승 스트립: 어제 대비 3위 이상 상승한 영상 ──
    surge_html = ''
    if prev_ranking:
        surge_vids = []
        all_longform_ranked = [v for v in filtered if not v.get('is_short')][:10]
        for rank_i, v in enumerate(all_longform_ranked):
            try:
                prev_rank = prev_ranking.index(v['vid']) + 1
                delta = prev_rank - (rank_i + 1)
                if delta >= 3:
                    surge_vids.append((v, delta))
            except ValueError:
                pass  # NEW 영상 (어제 없던 영상)은 제외
        if surge_vids:
            surge_items = ''
            for v, delta in surge_vids[:3]:
                dur_str   = fmt_duration(v.get('dur_sec', 0))
                dur_tag   = f'<span class="surge-dur">⏱ {dur_str}</span>' if dur_str else ''
                views_str = fmt_views(v['views']) + '뷰' if v['views'] > 0 else ''
                surge_items += f'''    <a class="surge-item" href="{v['url']}" target="_blank">
      <span class="surge-delta">▲{delta}</span>
      <img class="surge-thumb" src="https://img.youtube.com/vi/{v['vid']}/hqdefault.jpg" alt="">
      <div class="surge-body">
        <div class="surge-title">{v['title'][:50]}</div>
        <div class="surge-meta">{v['ch_name']} · {views_str}{dur_tag}</div>
      </div>
    </a>\n'''
            surge_html = f'''  <div class="surge-strip">
    <div class="surge-header"><span class="tossface">🔥</span> 이번 주 급상승</div>
{surge_items}  </div>\n'''

    more_section = ''
    if more_rows:
        more_section = f'''  <button class="more-toggle" onclick="toggleMore(this)">
    <span class="toggle-label">4~10위 더보기</span>
    <span class="toggle-arrow">▼</span>
  </button>
  <div class="more-list">
{more_rows}  </div>'''

    # ── 카테고리별 보장 아이템 (전체 TOP10 밖에 있는 카테고리 탑 영상) ──
    top10_vids = {v['vid'] for v in [v for v in filtered if not v.get('is_short')][:10]}
    all_longform_full = [v for v in filtered if not v.get('is_short')]
    cat_extra_rows = ''
    for cat_key in ['stock', 'realestate', 'macro', 'fintech']:
        cat_vids = [v for v in all_longform_full if v.get('cat') == cat_key and v['vid'] not in top10_vids]
        for k, v in enumerate(cat_vids[:3]):
            views_str  = fmt_views(v['views']) + '뷰' if v['views'] > 0 else ''
            cat_label  = CAT_LABEL.get(cat_key, '일반')
            try:
                days_old = (today - datetime.fromisoformat(v['date']).date()).days
                days_str = f'{days_old}일 전' if days_old > 0 else '오늘'
            except Exception:
                days_str = v['date']
            meta_html = build_meta_html(v['ch_name'], v['views'], days_str, v.get('dur_sec', 0))
            cat_extra_rows += f'''  <a class="hot-row" href="{v['url']}" target="_blank" data-cat="{cat_key}" data-rank="99" data-cat-extra="true">
    <span class="hot-rank-num rn">—</span>
    <img class="hot-thumb-sm" src="https://img.youtube.com/vi/{v['vid']}/hqdefault.jpg" alt="" loading="lazy">
    <div class="hot-body">
      <div class="hot-title">{v['title'][:60]}</div>
      <div class="hot-meta">{meta_html}</div>
    </div>
    <div class="hot-right">
      <span class="cat-badge cat-{cat_key}">{cat_label}</span>
    </div>
  </a>\n'''
    cat_extra_section = ''
    if cat_extra_rows:
        cat_extra_section = f'''  <div class="cat-extra-list">
{cat_extra_rows}  </div>'''

    # ── 숏폼: 실제 Shorts 데이터(기간 필터)만 사용 — 롱폼 fallback 없음 ──
    shorts_src = shorts_filtered[:5]
    rank_badges = ['1위', '2위', '3위', '4위', '5위']
    shorts_cards = ''
    for i, v in enumerate(shorts_src):
        views_str = fmt_views(v['views']) + '뷰' if v['views'] > 0 else '—'
        try:
            days_old = (datetime.now().date() - datetime.fromisoformat(v['date']).date()).days
        except Exception:
            days_old = 0
        dur_label = f'{v["dur_sec"]}초' if v.get('dur_sec') and v['dur_sec'] < 90 else ''
        why_short = ' · '.join(filter(None, [views_str, dur_label, f'{days_old}일 전']))
        shorts_cards += f'''        <a class="shorts-card" href="{v["url"]}" target="_blank">
          <div class="shorts-thumb-wrap">
            <img class="shorts-thumb-v" src="https://img.youtube.com/vi/{v["vid"]}/maxresdefault.jpg" onerror="this.src='https://img.youtube.com/vi/{v[\"vid\"]}/hqdefault.jpg'" alt="">
            <div class="shorts-rank-badge">{rank_badges[i]}</div>
          </div>
          <div class="shorts-body">
            <div class="shorts-title">{v["title"][:60]}</div>
            <div class="shorts-ch">{v["ch_name"]} · {v["date"]}</div>
            <div class="shorts-why">{why_short}</div>
          </div>
        </a>\n'''

    shorts_notice = '' if shorts_src else '<div style="font-size:10px;color:#ccc;margin-bottom:6px;">※ 해당 기간 Shorts 없음</div>'

    return f'''  <div class="hot-section-label"><span class="tossface">📹</span> 롱폼 TOP 10</div>
  <div class="hot-list">
{top3_html}  </div>
{surge_html}{more_section}
{cat_extra_section}
  <div class="cat-view-list"></div>
  <div class="hot-section-label shorts-label" style="margin-top:22px;">
    <span><span class="tossface">📱</span> 숏폼 TOP 5</span>
  </div>
  {shorts_notice}<div class="shorts-list">
{shorts_cards}  </div>'''


# ─── 스탯 바 빌더 ────────────────────────────────────
def build_stats_bar_html(kw_results, top_video, shorts_count, news_count):
    """상단 요약 스탯 4칸: 1위 키워드 / 최다 조회 / Shorts 수집 / 뉴스"""
    kw_label = kw_results[0][0]['label'] if kw_results else '—'
    top_views = fmt_views(top_video['views']) + '뷰' if top_video and top_video.get('views', 0) > 0 else '—'
    top_ch    = top_video['ch_name'] if top_video else '—'
    return f'''  <div class="stats-strip">
    <div class="stat-item">
      <span class="stat-icon">🔥</span>
      <div>
        <div class="stat-label">1위 키워드</div>
        <div class="stat-value">{kw_label}</div>
      </div>
    </div>
    <div class="stat-item">
      <span class="stat-icon">📹</span>
      <div>
        <div class="stat-label">최다 조회</div>
        <div class="stat-value">{top_views}</div>
        <div class="stat-sub">{top_ch}</div>
      </div>
    </div>
    <div class="stat-item">
      <span class="stat-icon">📱</span>
      <div>
        <div class="stat-label">Shorts 수집</div>
        <div class="stat-value">{shorts_count}개</div>
      </div>
    </div>
    <div class="stat-item">
      <span class="stat-icon">📰</span>
      <div>
        <div class="stat-label">뉴스</div>
        <div class="stat-value">{news_count}건</div>
      </div>
    </div>
  </div>'''


# ─── 순위 히스토리 저장/로드 ─────────────────────────
def load_ranking_history():
    """어제까지의 순위 히스토리 로드: {date_str: [vid_id, ...]}"""
    try:
        with open(RANKING_HISTORY_PATH, encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}

def save_ranking_history(history, today_str, video_ids):
    """오늘 순위 저장 (최근 400일 보관 — 6개월/12개월 패널용)"""
    history[today_str] = video_ids
    cutoff = (datetime.now() - timedelta(days=400)).strftime('%Y-%m-%d')
    history = {k: v for k, v in history.items() if k >= cutoff}
    try:
        with open(RANKING_HISTORY_PATH, 'w', encoding='utf-8') as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f'[경고] 순위 히스토리 저장 실패: {e}')
    return history


def load_video_cache():
    """누적 영상 메타데이터 캐시 로드"""
    try:
        with open(VIDEO_CACHE_PATH, encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def save_video_cache(cache, all_vids_full, today_str):
    """오늘 수집된 영상 정보를 캐시에 누적 저장"""
    for v in all_vids_full:
        vid = v['vid']
        if vid not in cache:
            cache[vid] = {
                'title':      v['title'],
                'url':        v['url'],
                'ch_name':    v.get('ch_name', ''),
                'cat':        v.get('cat', 'general'),
                'dur_sec':    v.get('dur_sec', 0),
                'pub_date':   v.get('date', today_str),
                'first_seen': today_str,
                'last_seen':  today_str,
                'best_views': v.get('views', 0),
                'best_score': v.get('score', 0),
            }
        else:
            # 기존 항목 업데이트: 최고 기록 갱신
            cache[vid]['last_seen']  = today_str
            if v.get('views', 0) > cache[vid].get('best_views', 0):
                cache[vid]['best_views'] = v['views']
            if v.get('score', 0) > cache[vid].get('best_score', 0):
                cache[vid]['best_score'] = v['score']
            # 카테고리·채널명 최신화
            cache[vid]['cat']     = v.get('cat', cache[vid]['cat'])
            cache[vid]['ch_name'] = v.get('ch_name', cache[vid]['ch_name'])
    try:
        with open(VIDEO_CACHE_PATH, 'w', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
        print(f'✅ 영상 캐시 저장: {len(cache)}개 누적')
    except Exception as e:
        print(f'[경고] 영상 캐시 저장 실패: {e}')
    return cache


def build_longterm_panel_html(video_cache, max_days, today_str):
    """video_details_cache 기반 장기 패널 HTML (6개월/12개월)"""
    cutoff = (datetime.strptime(today_str, '%Y-%m-%d') - timedelta(days=max_days)).strftime('%Y-%m-%d')
    today_dt = datetime.strptime(today_str, '%Y-%m-%d').date()

    # 기간 내 first_seen 영상만, 롱폼(dur≥120)만, best_score 정렬
    candidates = [
        v for v in video_cache.values()
        if v.get('first_seen', '9999') >= cutoff
        and v.get('dur_sec', 0) >= 120
    ]
    if not candidates:
        months = max_days // 30
        return f'  <div class="period-placeholder">📦 데이터 누적 중<br>브리핑이 매일 실행되면 {months}개월 후 자동 활성화됩니다.</div>'

    candidates.sort(key=lambda x: x.get('best_score', 0), reverse=True)
    top = candidates[:10]
    longform3 = top[:3]
    more_items = top[3:]

    rank_cls = ['r1', 'r2', 'r3']
    rank_sym = [
        '<span class="tossface">🥇</span>',
        '<span class="tossface">🥈</span>',
        '<span class="tossface">🥉</span>',
    ]
    top3_html = ''
    for i, v in enumerate(longform3):
        vid_id    = [k for k, val in video_cache.items() if val is v]
        vid_id    = vid_id[0] if vid_id else ''
        views_str = fmt_views(v.get('best_views', 0)) + '뷰' if v.get('best_views', 0) > 0 else ''
        dur_str   = fmt_duration(v.get('dur_sec', 0))
        cat       = v.get('cat', 'general')
        cat_label = CAT_LABEL.get(cat, '일반')
        try:
            days_old = (today_dt - datetime.strptime(v['pub_date'], '%Y-%m-%d').date()).days
            days_str = f'{days_old}일 전' if days_old > 0 else '오늘'
        except Exception:
            days_str = v.get('pub_date', '')
        meta_html = build_meta_html(v['ch_name'], v.get('best_views', 0), days_str, v.get('dur_sec', 0))
        url = v.get('url', f'https://www.youtube.com/watch?v={vid_id}')
        top3_html += f'''  <a class="hot-row hot-row-top3" href="{url}" target="_blank" data-cat="{cat}" data-rank="{i+1}">
    <span class="hot-rank-num {rank_cls[i]}">{rank_sym[i]}</span>
    <img class="hot-thumb-sm" src="https://img.youtube.com/vi/{vid_id}/hqdefault.jpg" alt="" loading="lazy">
    <div class="hot-body">
      <div class="hot-title">{v['title'][:60]}</div>
      <div class="hot-meta">{meta_html}</div>
    </div>
    <div class="hot-right">
      <span class="cat-badge cat-{cat}">{cat_label}</span>
    </div>
  </a>\n'''

    more_rows = ''
    for j, v in enumerate(more_items, 4):
        vid_id    = [k for k, val in video_cache.items() if val is v]
        vid_id    = vid_id[0] if vid_id else ''
        views_str = fmt_views(v.get('best_views', 0)) + '뷰' if v.get('best_views', 0) > 0 else ''
        dur_str   = fmt_duration(v.get('dur_sec', 0))
        cat       = v.get('cat', 'general')
        cat_label = CAT_LABEL.get(cat, '일반')
        try:
            days_old = (today_dt - datetime.strptime(v['pub_date'], '%Y-%m-%d').date()).days
            days_str = f'{days_old}일 전' if days_old > 0 else '오늘'
        except Exception:
            days_str = v.get('pub_date', '')
        meta_html = build_meta_html(v['ch_name'], v.get('best_views', 0), days_str, v.get('dur_sec', 0))
        url = v.get('url', f'https://www.youtube.com/watch?v={vid_id}')
        more_rows += f'''  <a class="hot-row" href="{url}" target="_blank" data-cat="{cat}" data-rank="{j}">
    <span class="hot-rank-num rn">{j}위</span>
    <img class="hot-thumb-sm" src="https://img.youtube.com/vi/{vid_id}/hqdefault.jpg" alt="" loading="lazy">
    <div class="hot-body">
      <div class="hot-title">{v['title'][:60]}</div>
      <div class="hot-meta">{meta_html}</div>
    </div>
    <div class="hot-right">
      <span class="cat-badge cat-{cat}">{cat_label}</span>
    </div>
  </a>\n'''

    more_section = ''
    if more_rows:
        more_section = f'''  <button class="more-toggle" onclick="toggleMore(this)">
    <span class="toggle-label">4~10위 더보기</span>
    <span class="toggle-arrow">▼</span>
  </button>
  <div class="more-list">
{more_rows}  </div>'''

    data_label = f'{max_days // 30}개월'
    return f'''  <div class="hot-section-label"><span class="tossface">📹</span> {data_label} 롱폼 TOP 10</div>
  <div class="hot-list">
{top3_html}  </div>
{more_section}
  <div class="cat-view-list"></div>'''

def get_rank_delta_html(vid_id, today_rank, prev_ranking):
    """전일 대비 순위 변동 뱃지 HTML"""
    if not prev_ranking:
        return '<span class="rank-delta rank-new">NEW</span>'
    try:
        prev_rank = prev_ranking.index(vid_id) + 1
    except ValueError:
        return '<span class="rank-delta rank-new">NEW</span>'
    delta = prev_rank - today_rank
    if delta > 0:
        return f'<span class="rank-delta rank-up">▲{delta}</span>'
    elif delta < 0:
        return f'<span class="rank-delta rank-down">▼{abs(delta)}</span>'
    else:
        return '<span class="rank-delta rank-same">—</span>'


# ─── TODAY'S BRIEF 자동 생성 ──────────────────────────
CAT_EMOJI = {'stock': '📈', 'realestate': '🏠', 'fintech': '💰', 'macro': '🌐', 'general': '📊'}
CAT_KR    = {'stock': '주식', 'realestate': '부동산', 'fintech': '재테크', 'macro': '경제', 'general': '일반'}

def build_summary_card_html(kw_results, all_vids_full, news_items):
    """TODAY'S 3-LINE BRIEF + CATEGORY BRIEF HTML 자동 생성"""

    # ── TOP 3 브리프 (키워드 기준) ──
    brief_items_html = ''
    rank_colors = ['#CC0000', '#cc4400', '#cc7700']
    for rank, (kw_cfg, videos) in enumerate(kw_results[:3], 1):
        top_vid = videos[0] if videos else None
        ch_names = ' · '.join(dict.fromkeys(v['ch_name'] for v in videos[:4])) if videos else '—'
        vid_count = len(videos)
        views_str = fmt_views(top_vid['views']) + '뷰' if top_vid and top_vid.get('views', 0) > 0 else ''

        # 브리프 설명 자동 생성
        if vid_count >= 3:
            desc = f'{ch_names[:40]} 등 {vid_count}개 영상 언급'
        elif vid_count > 0:
            desc = f'{ch_names[:50]} — {views_str}' if views_str else ch_names[:50]
        else:
            desc = '데이터 수집 중...'

        # 링크 블록 (YT 상위 3개 + 뉴스 1개)
        links_html = ''
        for v in videos[:3]:
            links_html += (
                f'<a class="bl-item" href="{v["url"]}" target="_blank">'
                f'<span class="bl-lbl yt">YT</span>'
                f'<span class="bl-text">{v["title"][:55]} — {v["ch_name"]}</span></a>'
            )
        news_url, news_label, _ = get_news_for_keyword(kw_cfg['query'], news_items or [])
        links_html += (
            f'<a class="bl-item" href="{news_url}" target="_blank">'
            f'<span class="bl-lbl news">뉴스</span>'
            f'<span class="bl-text">{news_label[:60]}</span></a>'
        )

        brief_items_html += f'''      <div class="brief-item-wrap">
        <div class="brief-item">
          <span class="brief-num" style="color:{rank_colors[rank-1]};">{rank}</span>
          <div class="brief-body">
            <div class="brief-title">{kw_cfg["label"]}</div>
            <div class="brief-desc">{desc}</div>
          </div>
          <span class="brief-hint">링크 ›</span>
        </div>
        <div class="brief-links">{links_html}</div>
      </div>\n'''

    # ── CATEGORY BRIEF ──
    cat_order = ['stock', 'realestate', 'fintech', 'macro']
    cat_vids = {}
    for v in all_vids_full:
        cat = v.get('cat', 'general')
        if cat not in cat_vids:
            cat_vids[cat] = v

    cat_items_html = ''
    shown_cats = [c for c in cat_order if c in cat_vids][:3]
    for i, cat in enumerate(shown_cats):
        top = cat_vids[cat]
        emoji = CAT_EMOJI.get(cat, '📊')
        label = CAT_KR.get(cat, '일반')
        title = top['title'][:52]
        views_str = fmt_views(top['views']) + '뷰' if top.get('views', 0) > 0 else ''
        desc = f'{top["ch_name"]} — {views_str}' if views_str else top['ch_name']
        is_last = (i == len(shown_cats) - 1)
        cat_items_html += f'''      <div class="brief-item-wrap"{' style="border-bottom:none;"' if is_last else ''}>
        <div class="brief-item">
          <div style="display:flex;flex-direction:column;align-items:center;width:36px;flex-shrink:0;gap:2px;">
            <span class="tossface" style="font-size:20px;line-height:1;">{emoji}</span>
            <span style="font-size:8px;color:#aaa;font-weight:600;letter-spacing:0.3px;">{label}</span>
          </div>
          <div class="brief-body">
            <div class="brief-title">{title}</div>
            <div class="brief-desc">{desc}</div>
          </div>
          <span class="brief-hint">링크 ›</span>
        </div>
        <div class="brief-links">
          <a class="bl-item" href="{top["url"]}" target="_blank">
            <span class="bl-lbl yt">YT</span>
            <span class="bl-text">{top["title"][:55]} — {top["ch_name"]}</span>
          </a>
        </div>
      </div>\n'''

    # 구분선 + CATEGORY BRIEF 헤더
    cat_section = ''
    if cat_items_html:
        cat_section = (
            '      <div style="height:1px;background:#f0f0f0;margin:4px 0 6px;"></div>\n'
            '      <div class="summary-card-title" style="margin-bottom:6px;">YOUTUBE BRIEF</div>\n'
            + cat_items_html
        )

    return brief_items_html + cat_section


# ─── 오늘 뉴스 패널 빌더 ──────────────────────────────
def build_hot_news_html(news_items):
    """오늘 뉴스 탭 — RSS 수집 뉴스 목록"""
    news_cards = ''
    for idx, n in enumerate(news_items[:10], 1):
        desc = n.get('desc', '')
        desc_html = f'<div class="news-summary">{desc}</div>' if desc else ''
        news_cards += f'''        <a class="news-item" href="{n["link"]}" target="_blank">
          <div class="news-num">{idx:02d}</div>
          <div>
            <div class="news-source-tag">{n["source"]}</div>
            <div class="news-headline">{n["title"][:80]}</div>
            {desc_html}
          </div>
        </a>\n'''
    return f'      <div class="news-list">\n{news_cards}      </div>'


# ─── 종합 키워드 TOP5 빌더 (스크린샷 디자인 완전 복원) ──
def build_kw_rows_html(kw_results, news_items=None):
    """스파크라인 · 트렌드 배지 · 뉴스링크 · 스코어바 포함 전체 디자인"""
    rank_cls = {1: 'top1', 2: 'top2', 3: 'top3'}

    # 스코어 계산
    top_scores = [max((v['score'] for v in vids), default=0) for _, vids in kw_results]
    max_score  = max(top_scores) if max(top_scores) > 0 else 1
    avg_score  = sum(top_scores) / len(top_scores) if top_scores else 1

    # 스파크라인 패턴 (1~5위)
    SPARKS = [
        [(4,'#f8c0c0'),(6,'#f8c0c0'),(9,'#f08080'),(11,'#f08080'),(13,'#e04040'),(16,'#e04040'),(20,'#CC0000')],
        [(8,'#f08080'),(10,'#f08080'),(12,'#e04040'),(14,'#e04040'),(15,'#e04040'),(17,'#CC0000'),(18,'#CC0000')],
        [(14,'#e04040'),(12,'#e04040'),(15,'#CC0000'),(13,'#e04040'),(14,'#e04040'),(12,'#e04040'),(14,'#e04040')],
        [(16,'#e04040'),(15,'#e04040'),(13,'#f08080'),(12,'#f08080'),(11,'#f08080'),(10,'#f8c0c0'),(9,'#f8c0c0')],
        [(18,'#CC0000'),(15,'#e04040'),(13,'#f08080'),(10,'#f08080'),(8,'#f8c0c0'),(7,'#f8c0c0'),(6,'#f8c0c0')],
    ]

    rows = ''
    for rank, (kw_cfg, videos) in enumerate(kw_results, 1):
        label = kw_cfg['label']
        emoji = kw_cfg['emoji']
        query = kw_cfg['query']
        top   = videos[0] if videos else None
        rest  = videos[1:4]
        score = top_scores[rank - 1]
        score_pct = round(score / max_score * 100) if max_score > 0 else 0
        bar_pct = max(score_pct, 4) if score > 0 else 0  # 최소 4% 표시
        pct_vs_avg = round((score - avg_score) / avg_score * 100) if avg_score > 0 else 0
        cls = rank_cls.get(rank, '')
        ch_names = ' · '.join(dict.fromkeys(v['ch_name'] for v in videos[:4])) if videos else '—'

        # 트렌드 배지 (TOP2만 스파크라인 + 배지, 나머지는 텍스트만)
        pattern_html = ''.join(
            f'<div class="spark-bar" style="height:{h}px; background:{c};"></div>'
            for h, c in SPARKS[min(rank-1, 4)]
        )
        if rank == 1:
            badge_html = '<span class="kw-badge rising">급상승</span>'
            trend_html = (f'<span class="kw-trend up">↑ +{abs(pct_vs_avg)}%</span>'
                         f'<div class="kw-sparkline">{pattern_html}</div>{badge_html}')
        elif rank == 2:
            pct_label = f'↑ +{abs(pct_vs_avg)}% 상승' if pct_vs_avg > 0 else '→ 보합 유지'
            trend_cls2 = 'up' if pct_vs_avg > 0 else 'stable'
            trend_html = (f'<span class="kw-trend {trend_cls2}">{pct_label}</span>'
                         f'<div class="kw-sparkline">{pattern_html}</div>')
        elif pct_vs_avg < -15:
            trend_html = f'<span class="kw-trend down">↓ {pct_vs_avg}% 하락</span>'
        else:
            trend_html = f'<span class="kw-trend stable">→ 보합 유지</span>'

        # YT 링크 블록
        yt_block = ''
        if top:
            exp_html = ''
            if rest:
                exp_items = ''.join(
                    f'<a class="kw-exp-item" href="{v["url"]}" target="_blank">'
                    f'<span class="kw-link-icon yt">YT</span>'
                    f'<span class="kw-exp-text">{v["title"][:50]} — {v["ch_name"]}</span></a>'
                    for v in rest
                )
                exp_html = f'<span class="kw-link-more">+{len(rest)} ›</span><div class="kw-expand">{exp_items}</div>'
            yt_block = (f'<div class="kw-link-wrap">'
                       f'<a class="kw-link-base yt" href="{top["url"]}" target="_blank">'
                       f'<span class="kw-link-icon yt">YT</span>'
                       f'<span class="kw-link-text">{top["title"][:55]} — {top["ch_name"]}</span>'
                       f'{exp_html}</a></div>')

        # 키워드별 관련 뉴스 (2열에 기존처럼)
        news_url, news_label, _ = get_news_for_keyword(query, news_items or [])
        news_block = (f'<div class="kw-link-wrap">'
                     f'<a class="kw-link-base news" href="{news_url}" target="_blank">'
                     f'<span class="kw-link-icon news">뉴스</span>'
                     f'<span class="kw-link-text">{news_label}</span>'
                     f'</a></div>')

        # 1위 행에만 오늘의 뉴스 패널 (rowspan=len(kw_results))
        today_news_td = ''
        if rank == 1:
            _today = datetime.now().strftime('%Y-%m-%d')
            _yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            all_news = news_items or []
            today_news    = [n for n in all_news if n.get('pub_date', _today) == _today]
            yesterday_news = [n for n in all_news if n.get('pub_date', '') == _yesterday]
            other_news    = [n for n in all_news if n not in today_news and n not in yesterday_news]

            def _news_rows(items, limit=6):
                html = ''
                for n in items[:limit]:
                    src_badge = f'<span class="kw-news-src-badge">{n["tag"]}</span>' if n.get('tag') else ''
                    html += (
                        f'<a class="kw-news-row" href="{n["link"]}" target="_blank">'
                        f'{src_badge}'
                        f'<span class="kw-news-title">{n["title"][:50]}</span>'
                        f'</a>'
                    )
                return html

            sections_html = ''
            if today_news:
                sections_html += '<div class="kw-news-day-label">오늘</div>' + _news_rows(today_news, 6)
            if yesterday_news:
                sections_html += '<div class="kw-news-day-label">어제</div>' + _news_rows(yesterday_news, 4)
            if not today_news and not yesterday_news:
                sections_html = _news_rows(other_news, 8)
            if not sections_html:
                sections_html = '<div style="color:#ccc;font-size:12px;padding:12px 0;">뉴스 수집 중...</div>'

            today_news_td = f'''
          <td class="kw-news-col" rowspan="{len(kw_results)}">
            <div class="kw-news-label">최신뉴스</div>
            {sections_html}
          </td>'''

        rows += f'''
        <!-- {rank}위 -->
        <tr>
          <td class="rank-num {cls}">{rank}</td>
          <td>
            <div class="kw-trend-row">
              <span class="kw-main"><span class="tossface">{emoji}</span> {label}</span>
              {trend_html}
            </div>
            <div class="kw-sub">{ch_names}</div>
            <div class="kw-default-links">
              {yt_block}
              {news_block}
            </div>
          </td>{today_news_td}
        </tr>'''

    return rows


# ─── GitHub Pages 업로드 ────────────────────────────

def gh_get_sha(path):
    url = f'{GITHUB_API_BASE}/repos/{GITHUB_REPO}/contents/{path}'
    req = urllib.request.Request(url, headers={
        'Authorization': f'token {GITHUB_TOKEN}',
        'Accept': 'application/vnd.github.v3+json',
    })
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read()).get('sha')
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def gh_push(path, content_bytes, message):
    sha = gh_get_sha(path)
    payload = {'message': message, 'content': base64.b64encode(content_bytes).decode()}
    if sha:
        payload['sha'] = sha
    url = f'{GITHUB_API_BASE}/repos/{GITHUB_REPO}/contents/{path}'
    req = urllib.request.Request(url, data=json.dumps(payload).encode(), method='PUT', headers={
        'Authorization': f'token {GITHUB_TOKEN}',
        'Accept': 'application/vnd.github.v3+json',
        'Content-Type': 'application/json',
    })
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())['content']['html_url']


# ─── 메인 ──────────────────────────────────────────

def main():
    print('=== 재테크 트렌드 브리핑 v2 생성 시작 ===\n')

    # 1. 템플릿 로드
    html = load_template()
    today = datetime.now()
    today_str = today.strftime('%Y-%m-%d')
    print(f'날짜: {korean_date_str(today)}\n')

    # 2. 날짜 업데이트
    html = re.sub(
        r'<!-- INJECT_DATE_START -->.*?<!-- INJECT_DATE_END -->',
        f'<!-- INJECT_DATE_START --><div class="masthead-date">{korean_date_str(today)}</div><!-- INJECT_DATE_END -->',
        html, flags=re.DOTALL
    )

    # 2-c. 날짜 아카이브 네비게이션
    base_url = 'https://leeho-spec.github.io/weolbu-briefing'
    script_dir = os.path.dirname(os.path.abspath(__file__))
    yesterday  = today - timedelta(days=1)
    yesterday_str2 = yesterday.strftime('%Y-%m-%d')
    today_label    = today.strftime('%Y. %m. %d')
    # 어제 파일이 로컬에 있을 때만 링크 활성
    prev_file = os.path.join(script_dir, f'daily_briefing_{yesterday_str2}.html')
    if os.path.exists(prev_file):
        prev_btn = f'<a class="date-nav-btn" href="{base_url}/briefings/daily_briefing_{yesterday_str2}.html">← {yesterday_str2}</a>'
    else:
        prev_btn = f'<span class="date-nav-btn disabled">← 이전 기록 없음</span>'
    # 오늘 브리핑에서 다음날은 항상 비활성
    next_btn = f'<span class="date-nav-btn disabled">최신 브리핑</span>'
    date_nav_html = (
        f'<div class="date-nav">'
        f'{prev_btn}'
        f'<span class="date-nav-center">📅 {today_label}</span>'
        f'{next_btn}'
        f'</div>'
    )
    html = re.sub(
        r'<!-- INJECT_DATE_NAV_START -->.*?<!-- INJECT_DATE_NAV_END -->',
        f'<!-- INJECT_DATE_NAV_START -->{date_nav_html}<!-- INJECT_DATE_NAV_END -->',
        html, flags=re.DOTALL
    )

    # 2-b. 소스바 — 채널명만, 가중치 표시 없음, CSS overflow:hidden 으로 1줄 제한
    source_tags = ''.join(
        f'  <span class="source-tag">{name}</span>\n'
        for name in CHANNELS.keys()
    )
    new_source_bar = (
        '<div class="source-bar">\n'
        '  <span>수집 채널</span>\n'
        + source_tags +
        '</div>'
    )
    html = re.sub(
        r'<div class="source-bar">.*?</div>',
        new_source_bar,
        html, count=1, flags=re.DOTALL
    )

    # 3. YouTube 데이터 수집
    print('[YouTube] 키워드별 영상 수집 중...')
    kw_results = []
    for kw_cfg in KEYWORDS:
        print(f'  [{kw_cfg["label"]}] 검색 중...')
        videos = collect_keyword_data(kw_cfg)
        kw_results.append((kw_cfg, videos))
        if videos:
            print(f'    → {len(videos)}개, 1위: {videos[0]["title"][:40]} ({videos[0]["ch_name"]}, {videos[0]["views"]:,}뷰)')
        else:
            print('    → 결과 없음')

    # 4. 오늘의 핫 콘텐츠 TOP5 선정 (롱폼 3 + 숏폼 5 공유)
    all_vids = []
    for _, vids in kw_results:
        all_vids.extend(vids)
    seen, top5 = set(), []
    for v in sorted(all_vids, key=lambda x: x['score'], reverse=True):
        if v['vid'] not in seen:
            seen.add(v['vid'])
            top5.append(v)
        if len(top5) == 5:
            break

    # 5. 뉴스 수집
    print('\n[뉴스] RSS 수집 중...')
    news = fetch_news(max_per_source=3)
    print(f'  → {len(news)}건 수집')

    # 5-b. 실제 Shorts 수집 (전 채널 RSS → duration ≤60초 필터)
    print('\n[Shorts] 수집 중...')
    shorts_list = collect_shorts_data()
    print(f'  → {len(shorts_list)}개 Shorts 수집')
    if shorts_list:
        print(f'  1위: {shorts_list[0]["title"][:50]} ({shorts_list[0]["ch_name"]}, {shorts_list[0]["views"]:,}뷰, {shorts_list[0]["dur_sec"]}초)')

    # 6a-1. 전체 영상 목록 (스코어 정렬, 중복 제거 + 카테고리 태깅)
    # vid → cat 매핑: 각 영상이 처음 발견된 키워드의 cat 사용
    vid_cat_map = {}
    for kw_cfg, vids in kw_results:
        cat = kw_cfg.get('cat', 'general')
        for v in vids:
            if v['vid'] not in vid_cat_map:
                vid_cat_map[v['vid']] = cat

    # 채널별 카테고리 맵
    ch_cat_hint  = {ch: info['cat_hint']  for ch, info in CHANNELS.items() if info.get('cat_hint')}
    ch_force_cat = {ch: info['force_cat'] for ch, info in CHANNELS.items() if info.get('force_cat')}

    all_vids_full = []
    seen_all = set()
    for v in sorted(all_vids, key=lambda x: x['score'], reverse=True):
        if v['vid'] not in seen_all:
            seen_all.add(v['vid'])
            ch = v.get('ch_name', '')
            if ch in ch_force_cat:
                # force_cat: 키워드 매칭 무관하게 강제 지정 (채널 성격이 명확한 경우)
                v['cat'] = ch_force_cat[ch]
            else:
                kw_cat = vid_cat_map.get(v['vid'])
                # 1) 키워드 매칭 카테고리 우선, 2) 채널 cat_hint fallback, 3) general
                v['cat'] = kw_cat or ch_cat_hint.get(ch, 'general')
            all_vids_full.append(v)

    # 6a-2. 순위 히스토리 로드 + 전일 순위 가져오기
    ranking_history = load_ranking_history()
    yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    prev_ranking = ranking_history.get(yesterday_str, [])
    if prev_ranking:
        print(f'  → 전일({yesterday_str}) 순위 {len(prev_ranking)}개 로드')
    else:
        print('  → 전일 순위 없음 (첫 실행 또는 당일 비교 불가) → NEW 표시')

    # 오늘 롱폼 순위 저장 (is_short=False 기준 상위 10개 vid ID)
    today_longform_ids = [v['vid'] for v in all_vids_full if not v.get('is_short')][:10]
    ranking_history = save_ranking_history(ranking_history, today_str, today_longform_ids)

    # 영상 메타데이터 캐시 누적 저장 (6개월/12개월 패널 데이터 원본)
    print('\n[캐시] 영상 메타데이터 누적 저장 중...')
    video_cache = load_video_cache()
    video_cache = save_video_cache(video_cache, all_vids_full, today_str)

    # 6a-3. 기간별 핫 콘텐츠 패널 주입 (이번주/1달)
    period_specs = [
        ('WEEK',  7,  'INJECT_HOT_CARDS_WEEK'),
        ('MONTH', 30, 'INJECT_HOT_CARDS_MONTH'),
    ]
    for _label, max_days, marker in period_specs:
        panel_html = build_hot_cards_by_period(all_vids_full, shorts_list, max_days=max_days, prev_ranking=prev_ranking)
        html = re.sub(
            rf'<!-- {marker}_START -->.*?<!-- {marker}_END -->',
            f'<!-- {marker}_START -->\n{panel_html}\n<!-- {marker}_END -->',
            html, flags=re.DOTALL
        )

    # 6a-4. 장기 패널 주입 (6개월/12개월) — video_details_cache 기반
    longterm_specs = [
        (180, 'INJECT_HOT_CARDS_6MONTH'),
        (365, 'INJECT_HOT_CARDS_12MONTH'),
    ]
    for max_days, marker in longterm_specs:
        panel_html = build_longterm_panel_html(video_cache, max_days, today_str)
        html = re.sub(
            rf'<!-- {marker}_START -->.*?<!-- {marker}_END -->',
            f'<!-- {marker}_START -->\n{panel_html}\n<!-- {marker}_END -->',
            html, flags=re.DOTALL
        )

    # 6b. TODAY'S BRIEF 자동 생성 & 주입
    brief_html = build_summary_card_html(kw_results, all_vids_full, news)
    html = re.sub(
        r'<!-- INJECT_BRIEF_START -->.*?<!-- INJECT_BRIEF_END -->',
        f'<!-- INJECT_BRIEF_START -->\n{brief_html}<!-- INJECT_BRIEF_END -->',
        html, flags=re.DOTALL
    )

    # 6c. 오늘 뉴스 탭 주입
    hot_news_html = build_hot_news_html(news)
    html = re.sub(
        r'<!-- INJECT_HOT_TODAY_START -->.*?<!-- INJECT_HOT_TODAY_END -->',
        f'<!-- INJECT_HOT_TODAY_START -->\n{hot_news_html}\n<!-- INJECT_HOT_TODAY_END -->',
        html, flags=re.DOTALL
    )

    # 7. 키워드 테이블 주입 (뉴스 링크 포함)
    kw_rows = build_kw_rows_html(kw_results, news_items=news)
    html = re.sub(
        r'<!-- INJECT_KW_ROWS_START -->.*?<!-- INJECT_KW_ROWS_END -->',
        f'<!-- INJECT_KW_ROWS_START -->{kw_rows}\n<!-- INJECT_KW_ROWS_END -->',
        html, flags=re.DOTALL
    )

    # 8. 시세 수집 & 주입
    print('\n[시세] Yahoo Finance 수집 중...')
    market_js = fetch_market_data()
    if market_js:
        html = re.sub(r'const stocks = \[.*?\];', market_js, html, flags=re.DOTALL)
        print('✓ 실시간 시세 반영 완료')
    else:
        print('⚠ 시세 데이터 없음 — 기존 값 유지')

    # 9. 로컬 저장
    out_filename = f'daily_briefing_{today_str}.html'
    out_path = os.path.join(SCRIPT_DIR, out_filename)
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'\n✅ 로컬 저장: {out_path}')

    # 10. GitHub Pages 업로드
    print('\n[GitHub] 업로드 중...')
    content_bytes = html.encode('utf-8')
    url1 = gh_push(f'briefings/{out_filename}', content_bytes, f'Auto-update briefing {today_str}')
    print(f'✅ {url1}')
    url2 = gh_push('latest.html', content_bytes, f'Update latest.html → {today_str}')
    print(f'✅ {url2}')

    pages_url = f'https://leeho-spec.github.io/weolbu-briefing/latest.html'
    print(f'\n🚀 배포 완료: {pages_url}')
    print(f'쿼터: ~{len(KEYWORDS) * len(CHANNELS) * 100 + 30} units / 10,000')

    # 11. Slack 발송용 요약 JSON 저장
    top3_longform = [v for v in all_vids_full if not v.get('is_short')][:3]
    slack_summary = {
        'date': today_str,
        'date_label': korean_date_str(today),
        'pages_url': pages_url,
        'top3': [
            {
                'rank': i + 1,
                'title': v['title'],
                'url': v['url'],
                'ch_name': v['ch_name'],
                'views': v.get('views', 0),
                'cat': v.get('cat', 'general'),
            }
            for i, v in enumerate(top3_longform)
        ],
        'keywords': [
            {
                'label': kw_cfg['label'],
                'emoji': kw_cfg.get('emoji', '📌'),
                'top_title': videos[0]['title'] if videos else '',
                'top_ch': videos[0]['ch_name'] if videos else '',
                'count': len(videos),
            }
            for kw_cfg, videos in kw_results[:3]
        ],
    }
    slack_json_path = os.path.join(SCRIPT_DIR, 'slack_summary.json')
    with open(slack_json_path, 'w', encoding='utf-8') as f:
        json.dump(slack_summary, f, ensure_ascii=False, indent=2)
    print(f'✅ Slack 요약 저장: {slack_json_path}')

    return pages_url


if __name__ == '__main__':
    main()
