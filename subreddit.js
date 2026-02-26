const express = require('express');
const path = require('path');
const { Queue, Worker } = require('bullmq');
const fetch = globalThis.fetch || require('node-fetch');

const REDDIT_UA = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
const MAX_PAGES_PER_FEED = 10;
const MAX_IMG_BYTES = 10 * 1024 * 1024; // 10 MB
const MAX_VIDEO_BYTES = 100 * 1024 * 1024; // 100 MB
const CONCURRENT = 3;
const REDIS_CONN = { host: 'localhost', port: 6379 };

// BullMQ queue for video downloads
const videoQueue = new Queue('videoDownloadQueue', { connection: REDIS_CONN });

// Multiple feeds to exhaust more of the subreddit
const FEEDS = [
  { sort: 'top', t: 'all' },
  { sort: 'top', t: 'year' },
  { sort: 'top', t: 'month' },
  { sort: 'top', t: 'week' },
  { sort: 'hot' },
  { sort: 'new' },
  { sort: 'rising' },
];

// Track in‑flight scrapes so we don't double-start
const activeScrapes = new Set();

/* ---------- helpers ---------- */
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

function extractVideoUrls(d) {
  if (!d) return [];
  const out = [];

  // 1. Reddit hosted video (v.redd.it)
  const rv = (d.media && d.media.reddit_video) || (d.secure_media && d.secure_media.reddit_video);
  if (rv && rv.fallback_url) {
    out.push({
      url: rv.fallback_url.replace(/\?.*$/, ''),
      audioUrl: rv.fallback_url.replace(/DASH_\d+\.mp4.*$/, 'DASH_AUDIO_128.mp4').replace(/DASH_\d+\.mp4.*$/, 'DASH_AUDIO_128.mp4'),
      title: d.title,
      postId: d.id,
      duration: rv.duration || null,
    });
  }

  // 2. Direct video link
  if (out.length === 0 && d.url && /\.(mp4|webm|mov)(\?.*)?$/i.test(d.url)) {
    out.push({ url: d.url, title: d.title, postId: d.id });
  }

  // 3. Preview video (reddit_video_preview)
  if (out.length === 0 && d.preview && d.preview.reddit_video_preview) {
    const pv = d.preview.reddit_video_preview;
    if (pv.fallback_url) {
      out.push({ url: pv.fallback_url.replace(/\?.*$/, ''), title: d.title, postId: d.id });
    }
  }

  return out;
}

async function downloadVideo(url) {
  try {
    const res = await fetch(url, { headers: { 'User-Agent': REDDIT_UA }, redirect: 'follow' });
    if (!res.ok) return null;
    const ct = res.headers.get('content-type') || '';
    const len = parseInt(res.headers.get('content-length') || '0', 10);
    if (len > MAX_VIDEO_BYTES) return null;
    const buf = Buffer.from(await res.arrayBuffer());
    if (buf.length > MAX_VIDEO_BYTES || buf.length === 0) return null;
    const contentType = ct.startsWith('video/') ? ct.split(';')[0] : 'video/mp4';
    return { data: buf, contentType };
  } catch { return null; }
}

/* ---------- module export ---------- */
module.exports = function createSubredditRouter(getDb) {
  const router = express.Router();
  let tablesReady = false;

  // Configure EJS view engine on the app (idempotent)
  router.use((req, _res, next) => {
    if (!req.app.get('view engine')) {
      req.app.set('view engine', 'ejs');
      req.app.set('views', path.join(__dirname, 'views'));
    }
    next();
  });

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
    await db.exec(`
      CREATE TABLE IF NOT EXISTS subreddit_videos (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        subreddit_id  INTEGER,
        reddit_post_id TEXT,
        url           TEXT,
        title         TEXT,
        video_data    BLOB,
        content_type  TEXT,
        status        TEXT DEFAULT 'pending',
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
    console.log(`🚀 r/${key} — starting scrape (${FEEDS.length} feeds × up to ${MAX_PAGES_PER_FEED} pages each)`);
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

    let total = sub.total_images || 0;
    let totalPages = 0;

    try {
      for (const feed of FEEDS) {
        const feedLabel = feed.t ? `${feed.sort}/${feed.t}` : feed.sort;
        console.log(`\n📂 r/${key} — scraping feed: ${feedLabel}`);
        let after = null;

        for (let page = 0; page < MAX_PAGES_PER_FEED; page++) {
          let apiUrl = `https://www.reddit.com/r/${encodeURIComponent(key)}/${feed.sort}.json?limit=100&raw_json=1`;
          if (feed.t) apiUrl += `&t=${feed.t}`;
          if (after) apiUrl += `&after=${after}`;

          console.log(`📄 r/${key} [${feedLabel}] — page ${page + 1}/${MAX_PAGES_PER_FEED}${after ? ` (after=${after.slice(0,12)}…)` : ''}`);
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

        // Collect image + video urls from this page
        const images = [];
        const videos = [];
        for (const child of listing.children) {
          if (child.data) {
            images.push(...extractImageUrls(child.data));
            videos.push(...extractVideoUrls(child.data));
          }
        }
        console.log(`   └─ ${listing.children.length} posts scanned → ${images.length} image(s), ${videos.length} video(s) found`);

        // Queue video downloads via BullMQ
        for (const vid of videos) {
          try {
            const exists = await db.get(
              'SELECT id FROM subreddit_videos WHERE subreddit_id = ? AND url = ?', sub.id, vid.url
            );
            if (!exists) {
              await db.run(
                `INSERT OR IGNORE INTO subreddit_videos (subreddit_id, reddit_post_id, url, title, status) VALUES (?, ?, ?, ?, ?)`,
                sub.id, vid.postId, vid.url, vid.title, 'queued'
              );
              const row = await db.get('SELECT id FROM subreddit_videos WHERE subreddit_id = ? AND url = ?', sub.id, vid.url);
              if (row) {
                await videoQueue.add('downloadVideo', {
                  videoDbId: row.id,
                  url: vid.url,
                  subredditName: key,
                  title: vid.title,
                }, { attempts: 3, backoff: { type: 'exponential', delay: 3000 } });
              }
            }
          } catch { /* dup */ }
        }

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

        totalPages++;
        await db.run('UPDATE subreddits SET pages_fetched = ? WHERE id = ?', totalPages, sub.id);
        after = listing.after;
        if (!after) {
          console.log(`📭 r/${key} [${feedLabel}] — no more pages in this feed`);
          break;
        }
        console.log(`⏳ r/${key} — waiting 1.2s before next page…`);
        await new Promise(r => setTimeout(r, 1200));
      }
      // small pause between feeds
      await new Promise(r => setTimeout(r, 1500));
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
      res.render('scraper/index', { subs });
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
      const imgRow = await db.get('SELECT COUNT(*) as cnt FROM subreddit_images WHERE subreddit_id = ?', sub.id);
      const vidRow = await db.get('SELECT COUNT(*) as cnt FROM subreddit_videos WHERE subreddit_id = ? AND status = ?', sub.id, 'done');
      const vidPending = await db.get('SELECT COUNT(*) as cnt FROM subreddit_videos WHERE subreddit_id = ? AND status != ?', sub.id, 'done');
      res.json({ status: sub.status, count: imgRow.cnt, videos: vidRow.cnt, videosPending: vidPending.cnt, pages: sub.pages_fetched });
    } catch { res.json({ status: 'error', count: 0, videos: 0 }); }
  });

  // ---- API: paginated image list ----
  router.get('/api/r/:name/images', async (req, res) => {
    try {
      const db = getDb();
      if (!db) return res.json({ images: [] });
      await ensureTables();
      const name = req.params.name.toLowerCase();
      const sub = await db.get('SELECT id FROM subreddits WHERE name = ? COLLATE NOCASE', name);
      if (!sub) return res.json({ images: [] });
      const offset = Math.max(0, parseInt(req.query.offset) || 0);
      const limit = Math.min(200, Math.max(1, parseInt(req.query.limit) || 60));
      const images = await db.all(
        'SELECT id, title FROM subreddit_images WHERE subreddit_id = ? ORDER BY id LIMIT ? OFFSET ?',
        sub.id, limit, offset
      );
      const total = (await db.get('SELECT COUNT(*) as cnt FROM subreddit_images WHERE subreddit_id = ?', sub.id)).cnt;
      res.json({ images, total, offset, limit, hasMore: offset + images.length < total });
    } catch { res.json({ images: [], total: 0, hasMore: false }); }
  });

  // ---- API: paginated media list (images + videos) ----
  router.get('/api/r/:name/media', async (req, res) => {
    try {
      const db = getDb();
      if (!db) return res.json({ items: [] });
      await ensureTables();
      const name = req.params.name.toLowerCase();
      const sub = await db.get('SELECT id FROM subreddits WHERE name = ? COLLATE NOCASE', name);
      if (!sub) return res.json({ items: [] });
      const offset = Math.max(0, parseInt(req.query.offset) || 0);
      const limit = Math.min(200, Math.max(1, parseInt(req.query.limit) || 20));
      // Union images and videos ordered by creation time
      const items = await db.all(`
        SELECT id, title, 'image' as type FROM subreddit_images WHERE subreddit_id = ?
        UNION ALL
        SELECT id, title, 'video' as type FROM subreddit_videos WHERE subreddit_id = ? AND status = 'done'
        ORDER BY id
        LIMIT ? OFFSET ?
      `, sub.id, sub.id, limit, offset);
      const totalImg = (await db.get('SELECT COUNT(*) as cnt FROM subreddit_images WHERE subreddit_id = ?', sub.id)).cnt;
      const totalVid = (await db.get('SELECT COUNT(*) as cnt FROM subreddit_videos WHERE subreddit_id = ? AND status = ?', sub.id, 'done')).cnt;
      const total = totalImg + totalVid;
      res.json({ items, total, offset, limit, hasMore: offset + items.length < total });
    } catch { res.json({ items: [], total: 0, hasMore: false }); }
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

      const totalImg = (await db.get('SELECT COUNT(*) as cnt FROM subreddit_images WHERE subreddit_id = ?', sub.id)).cnt;
      const totalVid = (await db.get('SELECT COUNT(*) as cnt FROM subreddit_videos WHERE subreddit_id = ? AND status = ?', sub.id, 'done')).cnt;
      const totalVidPending = (await db.get('SELECT COUNT(*) as cnt FROM subreddit_videos WHERE subreddit_id = ? AND status != ?', sub.id, 'done')).cnt;
      const totalMedia = totalImg + totalVid;
      const downloading = sub.status === 'downloading';

      res.render('scraper/gallery', { sub, totalImages: totalImg, totalVideos: totalVid, totalVideoPending: totalVidPending, totalMedia, downloading });
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

  // ---- Serve video blob ----
  router.get('/vid/:id', async (req, res) => {
    try {
      const db = getDb();
      if (!db) return res.status(503).end();
      const row = await db.get(
        'SELECT video_data, content_type FROM subreddit_videos WHERE id = ? AND status = ?', Number(req.params.id), 'done'
      );
      if (!row || !row.video_data) return res.status(404).end();
      const ct = row.content_type || 'video/mp4';
      const buf = row.video_data;

      // Support range requests for video seeking
      const range = req.headers.range;
      if (range) {
        const parts = range.replace(/bytes=/, '').split('-');
        const start = parseInt(parts[0], 10);
        const end = parts[1] ? parseInt(parts[1], 10) : buf.length - 1;
        const chunk = buf.slice(start, end + 1);
        res.writeHead(206, {
          'Content-Range': `bytes ${start}-${end}/${buf.length}`,
          'Accept-Ranges': 'bytes',
          'Content-Length': chunk.length,
          'Content-Type': ct,
          'Cache-Control': 'public, max-age=604800, immutable',
        });
        res.end(chunk);
      } else {
        res.set({
          'Content-Type': ct,
          'Content-Length': buf.length,
          'Accept-Ranges': 'bytes',
          'Cache-Control': 'public, max-age=604800, immutable',
        });
        res.send(buf);
      }
    } catch {
      res.status(500).end();
    }
  });

  // ---- BullMQ Video Download Worker ----
  const videoWorker = new Worker('videoDownloadQueue', async (job) => {
    const { videoDbId, url, subredditName, title } = job.data;
    const db = getDb();
    if (!db) throw new Error('DB not ready');

    console.log(`🎬 Downloading video: ${title || url}`);
    await db.run('UPDATE subreddit_videos SET status = ? WHERE id = ?', 'downloading', videoDbId);

    const result = await downloadVideo(url);
    if (!result) {
      await db.run('UPDATE subreddit_videos SET status = ? WHERE id = ?', 'failed', videoDbId);
      console.log(`❌ Video download failed: ${title || url}`);
      throw new Error(`Failed to download video: ${url}`);
    }

    await db.run(
      'UPDATE subreddit_videos SET video_data = ?, content_type = ?, status = ? WHERE id = ?',
      result.data, result.contentType, 'done', videoDbId
    );
    const sizeMB = (result.data.length / 1024 / 1024).toFixed(1);
    console.log(`✅ Video saved (${sizeMB} MB): ${title || url}`);
  }, {
    connection: REDIS_CONN,
    concurrency: 2,
  });

  videoWorker.on('failed', (job, err) => {
    console.error(`❌ Video job ${job.id} failed: ${err.message}`);
  });

  return router;
};

// Export so index.js can register it on Bull Board
module.exports.videoQueue = videoQueue;
