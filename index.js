const { Queue } = require('bullmq');
const clipboard = require('clipboardy');
const express = require('express');
const { createBullBoard } = require('@bull-board/api');
const { ExpressAdapter } = require('@bull-board/express');
const { Worker } = require('bullmq');
const puppeteer = require('puppeteer');
const sqlite3 = require('sqlite3');
const { open } = require('sqlite');
const fs = require('fs').promises;
const fsSync = require('fs');
const path = require('path');
const fetch = globalThis.fetch || require('node-fetch');

// ---- Tee console output to history.log ----
const logStream = fsSync.createWriteStream(path.join(__dirname, 'history.log'), { flags: 'a' });
const origLog = console.log.bind(console);
const origErr = console.error.bind(console);
function stamp() { return new Date().toISOString(); }
console.log = (...args) => {
  origLog(...args);
  logStream.write(`[${stamp()}] ${args.map(a => typeof a === 'string' ? a : JSON.stringify(a)).join(' ')}\n`);
};
console.error = (...args) => {
  origErr(...args);
  logStream.write(`[${stamp()}] ERROR ${args.map(a => typeof a === 'string' ? a : (a instanceof Error ? a.stack || a.message : JSON.stringify(a))).join(' ')}\n`);
};
// --------------------------------------------

require('dotenv').config();

const redditQueue = new Queue('redditQueue', {
  connection: {
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT) || 6379,
  },
});

const processedUrls = new Set();
const RUN_CLIPBOARD_MONITOR = process.env.RUN_CLIPBOARD_MONITOR !== 'false';
const INTERVAL = 3000;

if (RUN_CLIPBOARD_MONITOR) {
  console.log('Clipboard Monitor running.');
  
  setInterval(async () => {
    try {
      let clipboardContent = await clipboard.default.read();
      // console.log(clipboardContent);
          
      clipboardContent = (clipboardContent || '').trim();

      const redditUrlRegex = /^(?:https?:\/\/)?(?:(?:www|old|np|i)\.)?(?:reddit\.com|redd\.it)\/[^\s?#]+(?:[^\s]*)?$/i;

      if (redditUrlRegex.test(clipboardContent)) {
        if (!processedUrls.has(clipboardContent)) {
          await redditQueue.add('redditPost', { url: clipboardContent });        
          processedUrls.add(clipboardContent);
          // console.log(`Added new Reddit URL to queue: ${clipboardContent}`);
        }
      }
    } catch (error) {
      console.error('Error reading clipboard or adding to queue:', error);
    }
  }, INTERVAL);
} else {
  console.log('Clipboard monitoring is disabled.');
}

// =================== Bull-Board setup ===================
const app = express();
const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath('/admin/queues');

const { BullMQAdapter } = require('@bull-board/api/bullMQAdapter');
const { videoQueue } = require('./subreddit');

createBullBoard({
  queues: [
    new BullMQAdapter(redditQueue),
    new BullMQAdapter(videoQueue),
  ],
  serverAdapter,
});

app.use('/admin/queues', serverAdapter.getRouter());
// ========================================================

// ---- Body parsing + Subreddit image scraper ----
app.use(express.urlencoded({ extended: true }));
app.use(express.json());

// ---- Kill all BullMQ jobs ----
app.post('/api/kill-all-jobs', async (_req, res) => {
  try {
    await Promise.all([
      redditQueue.obliterate({ force: true }),
      videoQueue.obliterate({ force: true }),
    ]);
    console.log('🗑️  All BullMQ jobs obliterated (reddit + video queues)');
    res.json({ ok: true, message: 'All jobs killed' });
  } catch (err) {
    console.error('Kill all jobs error:', err);
    res.status(500).json({ ok: false, message: err.message });
  }
});

// ---- Get queue stats ----
app.get('/api/queue-stats', async (_req, res) => {
  try {
    const [waiting, active, delayed, completed, failed] = await Promise.all([
      redditQueue.getWaitingCount(),
      redditQueue.getActiveCount(),
      redditQueue.getDelayedCount(),
      redditQueue.getCompletedCount(),
      redditQueue.getFailedCount(),
    ]);
    const [vWaiting, vActive, vFailed, vCompleted] = await Promise.all([
      videoQueue.getWaitingCount(),
      videoQueue.getActiveCount(),
      videoQueue.getFailedCount(),
      videoQueue.getCompletedCount(),
    ]);
    res.json({ waiting, active, delayed, completed, failed, video: { waiting: vWaiting, active: vActive, failed: vFailed, completed: vCompleted } });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const createSubredditRouter = require('./subreddit');
app.use(createSubredditRouter(() => db));
// -------------------------------------------------

function escapeHtml(str = '') {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

async function waitForDbReady(timeoutMs = 5000) {
  const start = Date.now();
  while (!db && Date.now() - start < timeoutMs) {
    await new Promise(r => setTimeout(r, 100));
  }
  if (!db) throw new Error('Database not ready');
  return db;
}

app.get('/posts', async (req, res) => {
  try {
    await waitForDbReady();
    const rows = await db.all('SELECT id, url, title, created_at FROM posts ORDER BY created_at DESC');

    res.send(`
      <!doctype html>
      <html>
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width,initial-scale=1">
        <title>Shreddit — Saved Posts</title>
        <script src="https://cdn.tailwindcss.com"></script>
      </head>
      <body class="bg-slate-50 text-slate-800 antialiased">
        <div class="max-w-5xl mx-auto px-4 py-10">
          <header class="flex items-center justify-between mb-8">
            <div class="flex items-center gap-3">
              <div class="w-10 h-10 rounded-md bg-amber-500 text-white flex items-center justify-center font-semibold">S</div>
              <h1 class="text-2xl font-semibold tracking-tight">Shreddit</h1>
            </div>
            <nav class="flex items-center gap-4 text-sm">
              <a href="/admin/queues" class="text-slate-600 hover:text-slate-900">Bull Board</a>
              <button onclick="killAll(this)" class="px-3 py-1.5 bg-red-600 hover:bg-red-500 text-white text-xs font-semibold rounded-md transition">Kill All Jobs</button>
            </nav>
          </header>

          <main>
            <div class="grid gap-4">
              ${rows.length ? rows.map(r => `
                <a href="/posts/${r.id}" class="block bg-white rounded-lg shadow-sm hover:shadow-md transition p-4 border border-slate-100">
                  <div class="flex items-start justify-between">
                    <div>
                      <h2 class="text-lg font-medium text-slate-900">${escapeHtml(r.title || r.url || 'Untitled')}</h2>
                      <p class="mt-1 text-sm text-slate-500">${escapeHtml(r.url)}</p>
                    </div>
                    <div class="text-xs text-slate-400">${escapeHtml(r.created_at)}</div>
                  </div>
                </a>
              `).join('') : '<div class="p-6 bg-white rounded-lg shadow-sm text-slate-500">No posts yet</div>'}
            </div>
          </main>

          <footer class="mt-8 text-sm text-slate-400">
            <p>Built with care — for the upcoming <span class="font-semibold">apocalypse.</span></p>
          </footer>
        </div>
      <script>
      async function killAll(btn){
        if(!confirm('Kill ALL queued & active jobs?')) return;
        btn.disabled=true; btn.textContent='Killing…';
        try{
          const r=await fetch('/api/kill-all-jobs',{method:'POST'});
          const d=await r.json();
          btn.textContent=d.ok?'✓ Done':'✗ Failed';
          btn.classList.replace('bg-red-600',d.ok?'bg-green-600':'bg-red-800');
          setTimeout(()=>{btn.textContent='Kill All Jobs';btn.disabled=false;
            btn.classList.replace(d.ok?'bg-green-600':'bg-red-800','bg-red-600');},2000);
        }catch{btn.textContent='✗ Error';btn.disabled=false;}
      }
      </script>
      </body>
      </html>
    `);
  } catch (err) {
    console.error('GET /posts error', err);
    res.status(500).send('Failed to load posts');
  }
});

app.get('/posts/:id', async (req, res) => {
  try {
    await waitForDbReady();
    const postId = Number(req.params.id);
    const post = await db.get('SELECT id, url, title, content, created_at FROM posts WHERE id = ?', postId);
    if (!post) return res.status(404).send('Post not found');

    const commentsRows = await db.all(
      'SELECT id, thingId, parent_thingId, author, content, created_at FROM comments WHERE post_id = ? ORDER BY id',
      postId
    );

    const nodeMap = {};
    commentsRows.forEach(r => {
      const key = r.thingId || `__id_${r.id}`;
      nodeMap[key] = { ...r, key, children: [] };
    });

    const roots = [];
    commentsRows.forEach(r => {
      const key = r.thingId || `__id_${r.id}`;
      const parentKey = r.parent_thingId || null;
      if (parentKey && nodeMap[parentKey]) {
        nodeMap[parentKey].children.push(nodeMap[key]);
      } else {
        roots.push(nodeMap[key]);
      }
    });

    function renderComments(nodes) {
      if (!nodes || !nodes.length) return '';
      return `<ul class="space-y-4">${nodes.map(n => `
        <li class="bg-white rounded-lg p-4 border border-slate-100 shadow-sm">
          <div class="flex gap-3">
            <div class="flex-shrink-0">
              <div class="w-9 h-9 rounded-full bg-slate-100 flex items-center justify-center text-slate-700 font-medium">${escapeHtml((n.author || 'u').charAt(0).toUpperCase())}</div>
            </div>
            <div class="flex-1">
              <div class="flex items-baseline justify-between">
                <div class="text-sm font-semibold text-slate-700">${escapeHtml(n.author || 'unknown')}</div>
                <div class="text-xs text-slate-400 ml-4">${escapeHtml(n.created_at)}</div>
              </div>
              <div class="mt-2 text-slate-700 whitespace-pre-wrap">${escapeHtml(n.content || '')}</div>
              ${n.children && n.children.length ? `<div class="mt-4 ml-4">${renderComments(n.children)}</div>` : ''}
            </div>
          </div>
        </li>
      `).join('')}</ul>`;
    }

    res.send(`
      <!doctype html>
      <html>
      <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width,initial-scale=1">
        <title>${escapeHtml(post.title || post.url)}</title>
        <script src="https://cdn.tailwindcss.com"></script>
      </head>
      <body class="bg-slate-50 text-slate-800 antialiased">
        <div class="max-w-4xl mx-auto px-4 py-10">
          <p class="mb-4 text-sm">
            <a href="/posts" class="text-slate-600 hover:underline">&larr; Back to posts</a>
            <span class="mx-2 text-slate-300">•</span>
            <a href="/admin/queues" class="text-slate-600 hover:underline">Bull Board</a>
          </p>

          <article class="bg-white p-6 rounded-lg shadow-sm border border-slate-100">
            <h1 class="text-2xl font-semibold text-slate-900">${escapeHtml(post.title || post.url)}</h1>
            <p class="mt-2 text-sm text-slate-500"><a class="text-amber-600 hover:underline" href="${escapeHtml(post.url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(post.url)}</a></p>
            <div class="prose max-w-none mt-4 text-slate-700 whitespace-pre-wrap">${escapeHtml(post.content || '')}</div>
          </article>

          <section class="mt-8">
            <h2 class="text-lg font-semibold text-slate-900 mb-4">Comments</h2>
            ${renderComments(roots) || '<div class="text-slate-500">No comments yet</div>'}
          </section>
        </div>
      </body>
      </html>
    `);
  } catch (err) {
    console.error('GET /posts/:id error', err);
    res.status(500).send('Failed to load post details');
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Bull-Board is running on http://localhost:${PORT}/admin/queues`);
});

// ------------------- SQLite initialization -------------------
let db;
(async () => {
  db = await open({
    filename: process.env.DB_PATH || './data.db',
    driver: sqlite3.Database,
  });

  await db.exec(`
    CREATE TABLE IF NOT EXISTS posts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      url TEXT UNIQUE,
      title TEXT,
      content TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
  `);

  await db.exec(`
    CREATE TABLE IF NOT EXISTS comments (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      thingId TEXT,
      post_id INTEGER,
      parent_thingId TEXT,
      author TEXT,
      content TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(post_id) REFERENCES posts(id)
    );
  `);

//   console.log('SQLite DB initialized at ./data.db');
})().catch(err => {
  console.error('Failed to initialize database', err);
  process.exit(1);
});
// ------------------------------------------------------------

// ------------------- BullMQ Worker -------------------
const worker = new Worker('redditQueue', async job => {
//   console.log(`Processing job ${job.id}: ${job.data.url}`);

  // Wait for DB ready
  if (!db) {
    await new Promise(resolve => setTimeout(resolve, 500));
  }

  const url = job.data.url;

  // Launch puppeteer, fetch and scrape the page
  const browser = await puppeteer.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
  });

  let page;
  try {
    page = await browser.newPage();
    await page.setUserAgent('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36');
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });

    // Ensure the page has rendered the main post area
    await page.waitForSelector('shreddit-post, .shreddit-post, #shreddit-post, h1', { timeout: 10000 }).catch(() => {});

    // Extract title
    const title = await page.evaluate(() => {
      const el = document.querySelector('shreddit-post > h1') || document.querySelector('shreddit-post h1') || document.querySelector('h1');
      return el ? el.innerText.trim() : null;
    });

    // Extract content paragraphs from the provided path:
    // shreddit-post > shreddit-post-text-boady>text-neutral-content > <div> > <div> then paragraphs inside
    const content = await page.evaluate(() => {
      // try the exact path first, be tolerant to slight variant names
      const selectors = [
        'shreddit-post > shreddit-post-text-boady > text-neutral-content > div > div',
        'shreddit-post shreddit-post-text-boady text-neutral-content div > div',
        'shreddit-post text-neutral-content div > div',
        'shreddit-post > .shreddit-post-text-body',
        'shreddit-post'
      ];

      let container = null;
      for (const sel of selectors) {
        try {
          const found = document.querySelector(sel);
          if (found) { container = found; break; }
        } catch (e) {
          // ignore invalid selector variants
        }
      }

      // If we didn't find the exact containers, try to find paragraphs under shreddit-post
      if (!container) {
        const sp = document.querySelector('shreddit-post');
        if (sp) {
          const tryDiv = sp.querySelector('div');
          container = tryDiv || sp;
        } else {
          container = document.body;
        }
      }

      const paragraphs = Array.from(container.querySelectorAll('p'));
      if (paragraphs.length === 0) {
        // fallback: grab textContent of container
        const txt = container.innerText || '';
        return txt.trim();
      }
      const texts = paragraphs.map(p => p.innerText.trim()).filter(Boolean);
      return texts.join('\n\n');
    });

    // Insert post into DB (or ignore if exists)
    const existing = await db.get('SELECT id FROM posts WHERE url = ?', url);
    let postId;
    if (existing) {
      postId = existing.id;
      await db.run('UPDATE posts SET title = ?, content = ? WHERE id = ?', title, content, postId);
    //   console.log(`Updated existing post (id=${postId}) for url ${url}`);
    } else {
      const res = await db.run('INSERT INTO posts (url, title, content) VALUES (?, ?, ?)', url, title, content);
      postId = res.lastID;
      console.log(`Saved post (id=${postId}) for url ${url}`);
    }

    // Extract comments recursively
    // A shreddit-comment element has thingId and author attribute; replies are nested shreddit-comment elements.
    const comments = await page.evaluate(() => {
      function extractFromCommentElement(elem) {
        const thingId = elem.getAttribute('thingId') || elem.getAttribute('data-thingid') || null;
        const author = elem.getAttribute('author') || elem.getAttribute('data-author') || null;

        // Find the nested content area like:
        // <div class="md ..."><p>...</p><p>...</p></div>
        let contentText = '';
        const mdDiv = elem.querySelector('div.md') || elem.querySelector('[slot="comment"] .md') || elem.querySelector('.md.text-14-scalable');
        if (mdDiv) {
          const ps = Array.from(mdDiv.querySelectorAll('p')).map(p => p.innerText.trim()).filter(Boolean);
          contentText = ps.join('\n\n');
        } else {
          // fallback: look for paragraphs inside the element
          const ps = Array.from(elem.querySelectorAll('p')).map(p => p.innerText.trim()).filter(Boolean);
          contentText = ps.join('\n\n') || (elem.innerText || '').trim();
        }

        // find direct child comments (replies)
        const childComments = Array.from(elem.querySelectorAll(':scope > shreddit-comment, :scope > .shreddit-comment, shreddit-comment')).filter(c => c !== elem);

        const replies = childComments.map(c => extractFromCommentElement(c));

        return {
          thingId,
          author,
          content: contentText,
          replies,
        };
      }

      const topLevel = Array.from(document.querySelectorAll('shreddit-comment, .shreddit-comment')).filter(el => {
        // Keep only top-level comments (those not nested inside another shreddit-comment)
        let parent = el.parentElement;
        while (parent) {
          if (parent.matches && (parent.matches('shreddit-comment') || parent.matches('.shreddit-comment'))) return false;
          parent = parent.parentElement;
        }
        return true;
      });

      // If none found by that method, try all shreddit-comment elements and deduplicate by thingId
      const commentsToProcess = topLevel.length ? topLevel : Array.from(document.querySelectorAll('shreddit-comment, .shreddit-comment'));

      const extracted = commentsToProcess.map(c => extractFromCommentElement(c));
      return extracted;
    });

    // Insert comments into DB recursively with parent_thingId if present
    async function insertCommentsTree(items, parentThingId = null) {
      for (const item of items) {
        if (!item || (!item.thingId && !item.content)) continue;
        // insert or replace comment by thingId
        try {
          const existingC = item.thingId ? await db.get('SELECT id FROM comments WHERE thingId = ? AND post_id = ?', item.thingId, postId) : null;
          if (existingC) {
            await db.run(
              'UPDATE comments SET parent_thingId = ?, author = ?, content = ? WHERE id = ?',
              parentThingId,
              item.author,
              item.content,
              existingC.id
            );
          } else {
            await db.run(
              'INSERT INTO comments (thingId, post_id, parent_thingId, author, content) VALUES (?, ?, ?, ?, ?)',
              item.thingId,
              postId,
              parentThingId,
              item.author,
              item.content
            );
          }
        } catch (err) {
          console.error('DB error inserting comment', err);
        }

        if (item.replies && item.replies.length) {
          await insertCommentsTree(item.replies, item.thingId || parentThingId);
        }
      }
    }

    await insertCommentsTree(comments);

    console.log(`Saved comments for post id=${postId}, found top-level comments: ${comments.length}`);
  } catch (err) {
    console.error(`Error processing ${url}:`, err);
    throw err;
  } finally {
    if (page) await page.close().catch(() => {});
    if (browser) await browser.close().catch(() => {});
  }
}, {
  connection: {
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT) || 6379,
  },
});

worker.on('completed', job => {
//   console.log(`Job ${job.id} completed!`);
});

worker.on('failed', (job, err) => {
  console.error(`Job ${job.id} failed with error ${err && err.message ? err.message : err}`);
});
// ------------------------------------------------------------

// process a single reddit listing JSON URL (paginated by `after`)
async function processJsonListing(listingUrl) {
  try {
    let url = listingUrl;
    while (url) {
      const res = await fetch(url);
            
      if (!res.ok) {
        console.error(`Failed to fetch ${url}: ${res.status}`);
        break;
      }
      const json = await res.json();
      const data = json && json.data;
      if (!data) break;

      const children = Array.isArray(data.children) ? data.children : [];
      for (const child of children) {
        const cdata = child && child.data;
        if (!cdata) continue;
        // prefer permalink (post page) otherwise fallback to data.url
        const permalink = cdata.permalink;
        const postUrl = permalink ? `https://www.reddit.com${permalink}` : (cdata.url || null);
        if (!postUrl) continue;
        if (!processedUrls.has(postUrl)) {
          await redditQueue.add('redditPost', { url: postUrl });
          processedUrls.add(postUrl);
        }
      }

      const after = data.after || null;
      if (!after) break;

      // build next page url preserving original query params
      try {
        const u = new URL(listingUrl);
        u.searchParams.set('after', after);
        url = u.toString();
      } catch (e) {
        // fallback: append ?after=
        url = `${listingUrl}${listingUrl.includes('?') ? '&' : '?'}after=${after}`;
      }

      // small delay to be polite
      await new Promise(r => setTimeout(r, 800));
    }
  } catch (err) {
    console.error('processJsonListing error for', listingUrl, err);
  }
}

// process a plain url line (either json listing or a normal reddit post)
async function processLine(line) {
  const trimmed = (line || '').trim();
  if (!trimmed) return;
  if (trimmed.toLowerCase().endsWith('.json') || trimmed.toLowerCase().includes('.json?') ) {
    await processJsonListing(trimmed);
  } else {
    if (!processedUrls.has(trimmed)) {
      await redditQueue.add('redditPost', { url: trimmed });
      processedUrls.add(trimmed);
    }
  }
}

// read urls.txt (one URL per line) and process them
async function processUrlsFile(fileName = 'urls.txt') {
  try {
    const fp = path.resolve(process.cwd(), fileName);
    const content = await fs.readFile(fp, 'utf8');
    const lines = content.split(/\r?\n/).map(l => l.trim()).filter(Boolean);
    for (const line of lines) {
      await processLine(line);
    }
    console.log(`Finished enqueueing URLs from ${fileName}`);
  } catch (err) {
    console.error('Failed to process urls file', err);
  }
}

// kick off processing of ./urls.txt (non-blocking)
processUrlsFile('urls.txt').catch(err => {
  console.error('processUrlsFile fatal error', err);
});
