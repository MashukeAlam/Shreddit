const express = require('express');
const fetch = globalThis.fetch || require('node-fetch');

const REDDIT_UA = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
const MAX_PAGES = 10;           // 10 pages × 100 = up to 1 000 posts
const MAX_IMG_BYTES = 10 * 1024 * 1024; // 10 MB
const CONCURRENT = 3;

// Track in‑flight scrapes so we don't double-start
const activeScrapes = new Set();

/* ---------- helpers ---------- */
function esc(str = '') {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function extractImageUrls(d) {
  if (!d) return [];
  const out = [];

  // 1. Direct image link
  if (d.url && /\.(jpe?g|png|gif|webp)(\?.*)?$/i.test(d.url)) {
    out.push({ url: d.url, title: d.title, postId: d.id });
  }

  // 2. Gallery posts
  if (d.is_gallery && d.media_metadata) {
    for (const meta of Object.values(d.media_metadata)) {
      if (meta.status !== 'valid') continue;
      const u = (meta.s && (meta.s.u || meta.s.gif)) || null;
      if (u) out.push({ url: u.replace(/&amp;/g, '&'), title: d.title, postId: d.id });
    }
  }

  // 3. Imgur page link (not direct) → try direct
  if (out.length === 0 && d.url && /imgur\.com\/\w+$/i.test(d.url) && !/\.\w{3,4}$/.test(d.url)) {
    out.push({ url: d.url + '.jpg', title: d.title, postId: d.id });
  }

  // 4. Preview fallback
  if (out.length === 0 && d.preview && d.preview.images) {
    for (const img of d.preview.images) {
      if (img.source && img.source.url) {
        out.push({ url: img.source.url.replace(/&amp;/g, '&'), title: d.title, postId: d.id });
      }
    }
  }

  return out;
}

async function downloadImage(url) {
  try {
    const res = await fetch(url, { headers: { 'User-Agent': REDDIT_UA }, redirect: 'follow' });
    if (!res.ok) return null;
    const ct = res.headers.get('content-type') || '';
    if (!ct.startsWith('image/')) return null;
    const len = parseInt(res.headers.get('content-length') || '0', 10);
    if (len > MAX_IMG_BYTES) return null;
    const buf = Buffer.from(await res.arrayBuffer());
    if (buf.length > MAX_IMG_BYTES || buf.length === 0) return null;
    return { data: buf, contentType: ct.split(';')[0] };
  } catch { return null; }
}

/* ---------- module export ---------- */
module.exports = function createSubredditRouter(getDb) {
  const router = express.Router();
  let tablesReady = false;

  async function ensureTables() {
    if (tablesReady) return;
    const db = getDb();
    if (!db) return;
    await db.exec(`
      CREATE TABLE IF NOT EXISTS subreddits (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        name        TEXT UNIQUE COLLATE NOCASE,
        status      TEXT DEFAULT 'pending',
        total_images INTEGER DEFAULT 0,
        pages_fetched INTEGER DEFAULT 0,
        created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
      );
    `);
    await db.exec(`
      CREATE TABLE IF NOT EXISTS subreddit_images (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        subreddit_id  INTEGER,
        reddit_post_id TEXT,
        url           TEXT,
        title         TEXT,
        image_data    BLOB,
        content_type  TEXT,
        created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(subreddit_id) REFERENCES subreddits(id),
        UNIQUE(subreddit_id, url)
      );
    `);
    tablesReady = true;
  }

  /* ---- background scraper ---- */
  async function scrapeSubreddit(name) {
    const key = name.toLowerCase();
    if (activeScrapes.has(key)) {
      console.log(`⏳ r/${key} — scrape already in progress, skipping`);
      return;
    }
    activeScrapes.add(key);
    console.log(`🚀 r/${key} — starting scrape (up to ${MAX_PAGES} pages)`);
    const startTime = Date.now();

    const db = getDb();
    if (!db) { activeScrapes.delete(key); return; }
    await ensureTables();

    let sub = await db.get('SELECT * FROM subreddits WHERE name = ? COLLATE NOCASE', key);
    if (!sub) {
      await db.run('INSERT INTO subreddits (name, status) VALUES (?, ?)', key, 'downloading');
      sub = await db.get('SELECT * FROM subreddits WHERE name = ? COLLATE NOCASE', key);
    } else {
      await db.run('UPDATE subreddits SET status = ?, pages_fetched = 0 WHERE id = ?', 'downloading', sub.id);
    }

    let after = null;
    let total = sub.total_images || 0;

    try {
      for (let page = 0; page < MAX_PAGES; page++) {
        let apiUrl = `https://www.reddit.com/r/${encodeURIComponent(key)}/top.json?t=all&limit=100&raw_json=1`;
        if (after) apiUrl += `&after=${after}`;

        console.log(`📄 r/${key} — fetching page ${page + 1}/${MAX_PAGES}${after ? ` (after=${after.slice(0,12)}…)` : ''}`);
        const headers = { 'User-Agent': REDDIT_UA, 'Accept': 'application/json' };
        let res = await fetch(apiUrl, { headers });

        // Fallback to old.reddit.com if www returns 403
        if (res.status === 403) {
          const fallbackUrl = apiUrl.replace('www.reddit.com', 'old.reddit.com');
          console.log(`⚠️  r/${key} — got 403 from www, retrying via old.reddit.com…`);
          res = await fetch(fallbackUrl, { headers });
        }

        if (res.status === 429) {
          console.log(`⚠️  r/${key} — rate limited (429), waiting 5s…`);
          await new Promise(r => setTimeout(r, 5000));
          continue;
        }
        if (!res.ok) { console.error(`❌ r/${key} — Reddit API returned ${res.status}, stopping`); break; }

        const json = await res.json();
        const listing = json && json.data;
        if (!listing || !listing.children || !listing.children.length) break;

        // Collect image urls from this page
        const images = [];
        for (const child of listing.children) {
          if (child.data) images.push(...extractImageUrls(child.data));
        }
        console.log(`   └─ ${listing.children.length} posts scanned → ${images.length} image(s) found`);

        // Download in batches
        let pageSaved = 0, pageSkipped = 0, pageFailed = 0;
        for (let i = 0; i < images.length; i += CONCURRENT) {
          const batch = images.slice(i, i + CONCURRENT);
          const results = await Promise.all(batch.map(async img => {
            const exists = await db.get(
              'SELECT id FROM subreddit_images WHERE subreddit_id = ? AND url = ?', sub.id, img.url
            );
            if (exists) { pageSkipped++; return null; }
            const dl = await downloadImage(img.url);
            if (!dl) { pageFailed++; return null; }
            return { ...img, ...dl };
          }));

          for (const r of results) {
            if (!r) continue;
            try {
              await db.run(
                `INSERT OR IGNORE INTO subreddit_images
                   (subreddit_id, reddit_post_id, url, title, image_data, content_type)
                 VALUES (?, ?, ?, ?, ?, ?)`,
                sub.id, r.postId, r.url, r.title, r.data, r.contentType
              );
              total++;
              pageSaved++;
            } catch { /* dup */ }
          }
          await db.run('UPDATE subreddits SET total_images = ? WHERE id = ?', total, sub.id);
        }
        console.log(`   └─ 💾 ${pageSaved} saved, ⏭ ${pageSkipped} skipped (dup), ❌ ${pageFailed} failed  |  total: ${total}`);

        await db.run('UPDATE subreddits SET pages_fetched = ? WHERE id = ?', page + 1, sub.id);
        after = listing.after;
        if (!after) {
          console.log(`📭 r/${key} — no more pages (reached end of subreddit)`);
          break;
        }
        console.log(`⏳ r/${key} — waiting 1.2s before next page…`);
        await new Promise(r => setTimeout(r, 1200));
      }

      const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
      await db.run('UPDATE subreddits SET status = ?, total_images = ? WHERE id = ?', 'complete', total, sub.id);
      console.log(`✅ r/${key} — done! ${total} images saved in ${elapsed}s`);
    } catch (err) {
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
      console.error(`❌ r/${key} — scrape failed after ${elapsed}s:`, err.message || err);
      await db.run('UPDATE subreddits SET status = ? WHERE id = ?', 'error', sub.id).catch(() => {});
    } finally {
      activeScrapes.delete(key);
    }
  }

  /* ================= ROUTES ================= */

  // ---- Landing page ----
  router.get('/', async (_req, res) => {
    try {
      const db = getDb();
      if (!db) return res.status(503).send('DB not ready');
      await ensureTables();
      const subs = await db.all('SELECT * FROM subreddits ORDER BY created_at DESC');

      res.send(/* html */`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Reddit Image Scraper</title>
<script src="https://cdn.tailwindcss.com"></script>
<style>
  @keyframes blink{0%,100%{opacity:1}50%{opacity:.35}}
  .blink{animation:blink 1.4s ease-in-out infinite}
</style>
</head>
<body class="bg-gray-950 text-gray-100 min-h-screen flex flex-col">
<div class="max-w-3xl w-full mx-auto px-4 py-14 flex-1">

  <!-- header -->
  <div class="flex items-center gap-3 mb-1">
    <div class="size-10 rounded-lg bg-orange-600 flex items-center justify-center text-white font-bold text-lg select-none">R</div>
    <h1 class="text-3xl font-bold tracking-tight">Reddit Image Scraper</h1>
  </div>
  <p class="text-gray-500 text-sm mb-10">Enter a subreddit to download every image and browse them here.</p>

  <!-- form -->
  <form method="POST" action="/scrape" class="flex gap-3 mb-12">
    <div class="relative flex-1">
      <span class="absolute left-4 top-1/2 -translate-y-1/2 text-gray-500 font-semibold select-none">r/</span>
      <input name="subreddit" required autocomplete="off" spellcheck="false"
        placeholder="earthporn" 
        class="w-full pl-10 pr-4 py-3 rounded-lg bg-gray-900 border border-gray-700 text-gray-100
               placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-orange-500
               focus:border-transparent transition">
    </div>
    <button class="px-6 py-3 bg-orange-600 hover:bg-orange-500 font-semibold rounded-lg transition whitespace-nowrap">
      Scrape Images
    </button>
  </form>

  <!-- list -->
  <h2 class="text-lg font-semibold text-gray-400 mb-4">Scraped Subreddits</h2>
  ${subs.length === 0
    ? '<p class="text-gray-600">Nothing here yet — enter a subreddit above.</p>'
    : `<div class="grid gap-3">${subs.map(s => `
    <a href="/r/${esc(s.name)}"
       class="flex items-center justify-between bg-gray-900 hover:bg-gray-800 border border-gray-800
              rounded-lg p-4 transition group">
      <div class="flex items-center gap-3">
        <div class="size-9 rounded-full bg-gray-800 group-hover:bg-gray-700 flex items-center
                    justify-center text-orange-400 font-bold">${esc(s.name[0].toUpperCase())}</div>
        <div>
          <span class="font-medium">r/${esc(s.name)}</span>
          <span class="ml-3 text-sm text-gray-500">${s.total_images} image${s.total_images === 1 ? '' : 's'}</span>
        </div>
      </div>
      <div class="flex items-center gap-2 text-sm">
        ${s.status === 'downloading'
          ? '<span class="size-2 rounded-full bg-orange-400 blink"></span><span class="text-orange-400">Downloading…</span>'
          : s.status === 'complete'
          ? '<span class="size-2 rounded-full bg-green-400"></span><span class="text-green-400">Complete</span>'
          : s.status === 'error'
          ? '<span class="size-2 rounded-full bg-red-400"></span><span class="text-red-400">Error</span>'
          : '<span class="size-2 rounded-full bg-gray-600"></span><span class="text-gray-500">Pending</span>'}
      </div>
    </a>`).join('')}</div>`}
</div>

<footer class="flex items-center justify-center gap-4 text-xs text-gray-700 py-4 border-t border-gray-900">
  <a href="/posts" class="hover:text-gray-400">Saved Posts</a>
  <a href="/admin/queues" class="hover:text-gray-400">Bull Board</a>
  <button onclick="killAll(this)" class="px-3 py-1.5 bg-red-600 hover:bg-red-500 text-white font-semibold rounded-md transition">Kill All Jobs</button>
  <span id="qstats" class="text-gray-600"></span>
</footer>
<script>
async function killAll(btn){
  if(!confirm('Kill ALL queued & active BullMQ jobs?')) return;
  btn.disabled=true; btn.textContent='Killing…';
  try{
    const r=await fetch('/api/kill-all-jobs',{method:'POST'});
    const d=await r.json();
    btn.textContent=d.ok?'✓ Done':'✗ Failed';
    btn.classList.replace('bg-red-600',d.ok?'bg-green-600':'bg-red-800');
    loadStats();
    setTimeout(()=>{btn.textContent='Kill All Jobs';btn.disabled=false;
      btn.classList.replace(d.ok?'bg-green-600':'bg-red-800','bg-red-600');},2000);
  }catch{btn.textContent='✗ Error';btn.disabled=false;}
}
async function loadStats(){
  try{
    const r=await fetch('/api/queue-stats');
    const d=await r.json();
    document.getElementById('qstats').textContent=
      'Queue: '+d.waiting+' waiting, '+d.active+' active, '+d.completed+' done, '+d.failed+' failed';
  }catch{}
}
loadStats(); setInterval(loadStats,5000);
</script>
</body></html>`);
    } catch (err) {
      console.error('GET / error', err);
      res.status(500).send('Server error');
    }
  });

  // ---- Submit subreddit ----
  router.post('/scrape', async (req, res) => {
    const name = (req.body.subreddit || '').trim().replace(/^\/?(r\/)?/, '').replace(/[^a-zA-Z0-9_]/g, '');
    if (!name) return res.redirect('/');
    scrapeSubreddit(name).catch(e => console.error('scrapeSubreddit fatal:', e));
    res.redirect(`/r/${encodeURIComponent(name)}`);
  });

  // ---- API: polling endpoint ----
  router.get('/api/r/:name', async (req, res) => {
    try {
      const db = getDb();
      if (!db) return res.json({ status: 'error', count: 0 });
      await ensureTables();
      const name = req.params.name.toLowerCase();
      const sub = await db.get('SELECT * FROM subreddits WHERE name = ? COLLATE NOCASE', name);
      if (!sub) return res.json({ status: 'not_found', count: 0 });
      const row = await db.get('SELECT COUNT(*) as cnt FROM subreddit_images WHERE subreddit_id = ?', sub.id);
      res.json({ status: sub.status, count: row.cnt, pages: sub.pages_fetched });
    } catch { res.json({ status: 'error', count: 0 }); }
  });

  // ---- Gallery page ----
  router.get('/r/:name', async (req, res) => {
    try {
      const db = getDb();
      if (!db) return res.status(503).send('DB not ready');
      await ensureTables();

      const name = req.params.name.toLowerCase();
      const sub = await db.get('SELECT * FROM subreddits WHERE name = ? COLLATE NOCASE', name);
      if (!sub) return res.redirect('/');

      const images = await db.all(
        'SELECT id, title, content_type FROM subreddit_images WHERE subreddit_id = ? ORDER BY id', sub.id
      );
      const downloading = sub.status === 'downloading';

      res.send(/* html */`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>r/${esc(sub.name)} — Images</title>
<script src="https://cdn.tailwindcss.com"></script>
<style>
  @keyframes blink{0%,100%{opacity:1}50%{opacity:.35}}
  .blink{animation:blink 1.4s ease-in-out infinite}
  .card img{transition:transform .2s ease}
  .card:hover img{transform:scale(1.03)}
  /* lightbox */
  #lightbox{display:none;position:fixed;inset:0;z-index:50;background:rgba(0,0,0,.92);
    align-items:center;justify-content:center;cursor:zoom-out}
  #lightbox.open{display:flex}
  #lightbox img{max-width:92vw;max-height:92vh;border-radius:.5rem;box-shadow:0 0 60px rgba(0,0,0,.6)}
</style>
</head>
<body class="bg-gray-950 text-gray-100 min-h-screen">

<!-- lightbox -->
<div id="lightbox" onclick="this.classList.remove('open')">
  <img id="lb-img" src="" alt="">
</div>

<div class="max-w-[1400px] mx-auto px-4 py-8">
  <header class="flex flex-wrap items-start justify-between gap-4 mb-8">
    <div>
      <a href="/" class="text-sm text-gray-500 hover:text-gray-300">&larr; Back</a>
      <h1 class="text-3xl font-bold tracking-tight mt-1">r/${esc(sub.name)}</h1>
      <div class="flex items-center gap-3 mt-1">
        ${downloading
          ? '<span class="size-2 rounded-full bg-orange-400 blink"></span><span class="text-sm text-orange-400">Downloading…</span>'
          : sub.status === 'complete'
          ? '<span class="size-2 rounded-full bg-green-400"></span><span class="text-sm text-green-400">Complete</span>'
          : '<span class="size-2 rounded-full bg-red-400"></span><span class="text-sm text-red-400">' + esc(sub.status) + '</span>'}
        <span class="text-sm text-gray-500" id="cnt">${images.length} image${images.length === 1 ? '' : 's'}</span>
        ${sub.pages_fetched ? `<span class="text-sm text-gray-600">(${sub.pages_fetched} pages fetched)</span>` : ''}
      </div>
    </div>
    <form method="POST" action="/scrape" class="flex gap-2">
      <input type="hidden" name="subreddit" value="${esc(sub.name)}">
      <button class="px-4 py-2 bg-orange-600 hover:bg-orange-500 text-sm font-medium rounded-lg transition">
        Re‑scrape
      </button>
    </form>
  </header>

  ${images.length === 0 && downloading ? '<p class="text-center text-gray-500 mt-20">Downloading images — the gallery will appear automatically…</p>' : ''}
  ${images.length === 0 && !downloading ? '<p class="text-center text-gray-600 mt-20">No images found in this subreddit.</p>' : ''}

  <div id="gallery" class="columns-2 sm:columns-3 lg:columns-4 xl:columns-5 gap-3 [&>div]:mb-3">
    ${images.map(img => `
    <div class="card break-inside-avoid">
      <div class="rounded-lg overflow-hidden bg-gray-900 border border-gray-800 cursor-pointer"
           onclick="openLb('/img/${img.id}')">
        <img src="/img/${img.id}" alt="${esc(img.title || '')}" loading="lazy" class="w-full block">
        ${img.title ? `<div class="px-2 py-1.5 text-xs text-gray-400 truncate">${esc(img.title)}</div>` : ''}
      </div>
    </div>`).join('')}
  </div>
</div>

<script>
function openLb(src){
  const lb=document.getElementById('lightbox'),img=document.getElementById('lb-img');
  img.src=src; lb.classList.add('open');
}
document.addEventListener('keydown',e=>{if(e.key==='Escape')document.getElementById('lightbox').classList.remove('open')});

${downloading ? `
// poll while downloading
let lastCount=${images.length};
setInterval(async()=>{
  try{
    const r=await fetch('/api/r/${sub.name}');
    const d=await r.json();
    if(d.count!==lastCount||d.status!=='downloading') location.reload();
  }catch{}
},3500);` : ''}
</script>
</body></html>`);
    } catch (err) {
      console.error('GET /r/:name error', err);
      res.status(500).send('Server error');
    }
  });

  // ---- Serve image blob ----
  router.get('/img/:id', async (req, res) => {
    try {
      const db = getDb();
      if (!db) return res.status(503).end();
      const row = await db.get(
        'SELECT image_data, content_type FROM subreddit_images WHERE id = ?', Number(req.params.id)
      );
      if (!row || !row.image_data) return res.status(404).end();
      res.set({
        'Content-Type': row.content_type || 'image/jpeg',
        'Cache-Control': 'public, max-age=604800, immutable',
      });
      res.send(row.image_data);
    } catch {
      res.status(500).end();
    }
  });

  return router;
};
