// 콩나물 서비스워커
// 이 사이트는 시세·잔액처럼 계속 바뀌는 데이터가 많아서, 캐시는 최소한으로만 써요.
// /api/ 로 가는 요청은 절대 캐시하지 않고 항상 네트워크로 보내요 (오래된 잔액/시세가 보이면 안 되니까요).
// 캐시하는 건 오직 "껍데기"(HTML/아이콘) 뿐이라, 오프라인일 때 완전히 빈 화면 대신
// 최소한의 안내라도 보여줄 수 있어요.

const CACHE_NAME = 'kongnamul-shell-v1';
const SHELL_URLS = ['/', '/static/manifest.json'];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(SHELL_URLS)).catch(() => {})
  );
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((names) =>
      Promise.all(names.filter((n) => n !== CACHE_NAME).map((n) => caches.delete(n)))
    )
  );
  self.clients.claim();
});

self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);

  // API 요청은 무조건 네트워크로 (절대 캐시 사용 안 함)
  if (url.pathname.startsWith('/api/')) {
    return; // 브라우저 기본 동작(네트워크 요청)에 맡김
  }

  // 그 외(HTML/정적 파일)는 네트워크 우선, 실패하면 캐시, 그것도 없으면 오프라인 안내
  event.respondWith(
    fetch(event.request)
      .then((res) => {
        const resClone = res.clone();
        caches.open(CACHE_NAME).then((cache) => cache.put(event.request, resClone)).catch(() => {});
        return res;
      })
      .catch(() =>
        caches.match(event.request).then(
          (cached) =>
            cached ||
            new Response('오프라인 상태예요. 인터넷 연결을 확인해주세요.', {
              headers: { 'Content-Type': 'text/plain; charset=utf-8' },
            })
        )
      )
  );
});
