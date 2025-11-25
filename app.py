# app.py
"""
ViralScope — Fixed multiple-results bug + show publish date for each sample video
- Inline API_KEY present (keep private)
- Run: streamlit run app.py
"""

import streamlit as st
import requests
from datetime import datetime, timedelta
import re
import csv
import os
import math
import uuid
import sqlite3
import pandas as pd

# -------------------------
# Configuration (INLINE API KEY)
# -------------------------
API_KEY = "AIzaSyC8nrFFraG69j9B_34t61W9xvK3-Ptl2UM"  # <<-- keep private
YOUTUBE_SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
YOUTUBE_VIDEO_URL = "https://www.googleapis.com/youtube/v3/videos"
YOUTUBE_CHANNEL_URL = "https://www.googleapis.com/youtube/v3/channels"
DB_FILE = "viral_scope.db"

# -------------------------
# Utilities
# -------------------------
def parse_iso8601_duration_to_seconds(duration):
    if not duration or not duration.startswith("PT"):
        return 0
    hours = minutes = seconds = 0
    m = re.match(r"^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$", duration)
    if m:
        h, mm, s = m.groups()
        hours = int(h) if h else 0
        minutes = int(mm) if mm else 0
        seconds = int(s) if s else 0
        return hours * 3600 + minutes * 60 + seconds
    numbers = re.findall(r"(\d+\.?\d*)([HMS])", duration)
    total = 0.0
    for val, unit in numbers:
        valf = float(val)
        if unit == "H":
            total += valf * 3600
        elif unit == "M":
            total += valf * 60
        elif unit == "S":
            total += valf
    return int(total)

def seconds_to_readable(seconds):
    if seconds is None:
        return "N/A"
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    m, s = divmod(s, 60)
    if m < 60:
        return f"{m}m {s}s"
    h, m = divmod(m, 60)
    return f"{h}h {m}m"

def safe_int(x):
    try:
        return int(x)
    except:
        return 0

def parse_rfc3339_to_datetime(ts):
    if not ts:
        return None
    if ts.endswith("Z"):
        ts = ts[:-1]
    if "." in ts:
        ts = ts.split(".")[0]
    try:
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S")
    except:
        try:
            return datetime.strptime(ts, "%Y-%m-%d")
        except:
            return None

def compute_virality_score(views, published_at, now=None):
    if not now:
        now = datetime.utcnow()
    if not published_at:
        days = 1.0
    else:
        days = max(1.0, (now - published_at).total_seconds() / (24*3600))
    vpd = views / days
    score = math.log10(1 + vpd) * 30
    return int(max(0, min(100, score)))

def monetization_likelihood(subs, avg_views_per_video, channel_age_months):
    s = 0.0
    if subs is None:
        subs = 0
    if subs >= 10000: s += 45
    elif subs >= 5000: s += 30
    elif subs >= 1000: s += 18
    elif subs >= 500: s += 8
    else: s += 2
    v = avg_views_per_video or 0
    if v >= 50000: s += 28
    elif v >= 10000: s += 20
    elif v >= 2000: s += 12
    elif v >= 500: s += 6
    else: s += 1
    age = channel_age_months or 0
    if age >= 36: s += 18
    elif age >= 12: s += 10
    elif age >= 6: s += 5
    else: s += 1
    return int(max(0, min(100, s)))

# -------------------------
# DB helpers (no video_id anywhere)
# -------------------------
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            started_at TEXT,
            days INTEGER,
            keywords TEXT,
            notes TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS video_samples (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT,
            keyword TEXT,
            title TEXT,
            channel_id TEXT,
            channel_title TEXT,
            channel_subs INTEGER,
            views INTEGER,
            likes INTEGER,
            comments INTEGER,
            duration_seconds INTEGER,
            thumbnail TEXT,
            published_at TEXT,
            virality INTEGER,
            monetization_likelihood INTEGER,
            saved_at TEXT
        )
    """)
    conn.commit()
    conn.close()

def save_run_to_db(run_id, started_at, days, keywords_list, notes, rows):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO runs(run_id, started_at, days, keywords, notes) VALUES (?,?,?,?,?)",
                (run_id, started_at.isoformat(), days, ",".join(keywords_list), notes or ""))
    now = datetime.utcnow().isoformat()
    for r in rows:
        cur.execute("""
            INSERT INTO video_samples
            (run_id, keyword, title, channel_id, channel_title, channel_subs, views, likes, comments, duration_seconds, thumbnail, published_at, virality, monetization_likelihood, saved_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            run_id,
            r.get("keyword"),
            r.get("title"),
            r.get("channel_id"),
            r.get("channel_title"),
            r.get("channel_subs"),
            r.get("views"),
            r.get("likes"),
            r.get("comments"),
            r.get("duration_seconds"),
            r.get("thumbnail"),
            r.get("published_at"),
            r.get("virality"),
            r.get("monetization_likelihood"),
            now
        ))
    conn.commit()
    conn.close()

def load_runs_summary():
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query("SELECT * FROM runs ORDER BY started_at DESC", conn, parse_dates=["started_at"])
    conn.close()
    return df

def load_samples_for_channel(channel_id):
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query("SELECT * FROM video_samples WHERE channel_id = ? ORDER BY saved_at", conn, params=(channel_id,), parse_dates=["published_at","saved_at"])
    conn.close()
    return df

# -------------------------
# Styling (thin white border)
# -------------------------
st.set_page_config(page_title="ViralScope", layout="wide")
st.markdown("""
<style>
body { background:#061029; color:#e7eef8; }
.card {
  padding:12px;
  border-radius:10px;
  margin-bottom:12px;
  background:#071228;
  border: 1px solid rgba(255,255,255,0.12);
  box-shadow: 0 4px 10px rgba(0,0,0,0.35);
}
.small { color:#98a5b8; font-size:13px; }
.pill { display:inline-block; background:rgba(124,92,255,0.12); color:#a99bff; padding:6px 10px; border-radius:999px; font-weight:600; margin-right:6px; }
.sample-item { display:flex; gap:10px; margin-bottom:8px; align-items:center; }
.thumb { width:160px; height:90px; object-fit:cover; border-radius:6px; }
.meta { display:flex; flex-direction:column; }
a { color:#9fb7ff; text-decoration:none; }
</style>
""", unsafe_allow_html=True)

# -------------------------
# Sidebar controls (includes max_channel_age_months)
# -------------------------
st.sidebar.title("Controls")
keywords_input = st.sidebar.text_area("Keywords (one per line)", value="Affair Relationship Stories\nReddit Cheating\nAITA Update", height=140)
keywords = [k.strip() for k in re.split(r"[\n,]+", keywords_input) if k.strip()]
days = st.sidebar.number_input("Search last N days", min_value=1, max_value=90, value=7)
results_per_keyword = st.sidebar.slider("Results per keyword", 1, 50, 8)
min_channel_subs = st.sidebar.number_input("Min channel subscribers (0 = none)", min_value=0, value=0)
max_channel_age_months = st.sidebar.number_input("Max channel age (months, 0 = none)", min_value=0, value=0)
only_shorts = st.sidebar.checkbox("Only Shorts (avg duration < 60s)", value=False)
country_filter = st.sidebar.text_input("Channel country filter (ISO code or country name, optional)", value="")
auto_save_csv = st.sidebar.checkbox("Auto-save CSV after run", value=True)
save_to_db = st.sidebar.checkbox("Save run to local DB", value=True)
show_raw = st.sidebar.checkbox("Show raw results table", value=False)
st.sidebar.markdown("Keep API key in this file private!")

# Initialize DB
init_db()

# -------------------------
# Main - Run
# -------------------------
st.title("ViralScope")
st.write("Find recent viral videos for keywords. Video IDs are not stored or shown. Publish date added to each sample video.")

colL, colR = st.columns([3, 1])
with colR:
    st.markdown("Info")
    st.write(f"Keywords: {len(keywords)}")
    st.write(f"Days: {days}")
    st.write(f"Results/keyword: {results_per_keyword}")

note = st.text_input("Notes for this run (optional)")

if st.button("Run Scan"):
    if not API_KEY:
        st.error("API key missing.")
        st.stop()
    if not keywords:
        st.warning("Add at least one keyword.")
        st.stop()

    now = datetime.utcnow()
    published_after = (now - timedelta(days=days)).isoformat("T") + "Z"
    progress = st.progress(0)
    status = st.empty()

    all_video_rows = []
    channel_map = {}  # channel_id -> metadata + sample_videos

    try:
        for i, kw in enumerate(keywords, start=1):
            status.text(f"[{i}/{len(keywords)}] Searching: {kw}")
            search_params = {
                "part": "snippet",
                "q": kw,
                "type": "video",
                "order": "viewCount",
                "publishedAfter": published_after,
                "maxResults": results_per_keyword,
                "key": API_KEY
            }
            r = requests.get(YOUTUBE_SEARCH_URL, params=search_params)
            if r.status_code != 200:
                st.error(f"Search API error for '{kw}': {r.status_code} {r.text}")
                continue
            items = r.json().get("items", [])
            if not items:
                progress.progress(int(i/len(keywords)*100))
                continue

            video_ids = []
            ch_ids = []
            vid_to_kw = {}
            for it in items:
                vid = it.get("id", {}).get("videoId")
                if not vid:
                    continue
                video_ids.append(vid)
                cid = it.get("snippet", {}).get("channelId")
                ch_title_from_video = it.get("snippet", {}).get("channelTitle")
                if cid:
                    ch_ids.append(cid)
                    if cid not in channel_map:
                        channel_map[cid] = {"title": ch_title_from_video, "subs": None, "published_at": None, "country": None, "avatar": None, "sample_videos": []}
                    else:
                        if not channel_map[cid].get("title"):
                            channel_map[cid]["title"] = ch_title_from_video
                vid_to_kw[vid] = kw

            if not video_ids:
                progress.progress(int(i/len(keywords)*100))
                continue

            # call videos API
            v_params = {
                "part": "snippet,statistics,contentDetails",
                "id": ",".join(video_ids),
                "key": API_KEY
            }
            vresp = requests.get(YOUTUBE_VIDEO_URL, params=v_params)
            if vresp.status_code != 200:
                st.error(f"Videos API error for '{kw}': {vresp.status_code} {vresp.text}")
                progress.progress(int(i/len(keywords)*100))
                continue
            vitems = vresp.json().get("items", [])

            # fetch channel metadata for new channels
            new_ch_ids = [cid for cid in set(ch_ids) if cid not in channel_map or channel_map[cid].get("subs") is None]
            if new_ch_ids:
                ch_params = {"part": "snippet,statistics", "id": ",".join(new_ch_ids), "key": API_KEY}
                chresp = requests.get(YOUTUBE_CHANNEL_URL, params=ch_params)
                if chresp.status_code == 200:
                    for ch in chresp.json().get("items", []):
                        cid = ch.get("id")
                        subs = safe_int(ch.get("statistics", {}).get("subscriberCount", 0))
                        published_at = ch.get("snippet", {}).get("publishedAt")
                        country = ch.get("snippet", {}).get("country")
                        avatar = ch.get("snippet", {}).get("thumbnails", {}).get("default", {}).get("url")
                        title = ch.get("snippet", {}).get("title")
                        channel_map.setdefault(cid, {"title": title, "subs": subs, "published_at": published_at, "country": country, "avatar": avatar, "sample_videos": []})
                        channel_map[cid]["title"] = channel_map[cid].get("title") or title
                        channel_map[cid]["subs"] = channel_map[cid].get("subs") or subs
                        channel_map[cid]["published_at"] = channel_map[cid].get("published_at") or published_at
                        channel_map[cid]["country"] = channel_map[cid].get("country") or country
                        channel_map[cid]["avatar"] = channel_map[cid].get("avatar") or avatar
                else:
                    pass

            # PROCESS each video (FIX: extract vid properly and use vid_to_kw[vid])
            for vi in vitems:
                vid = vi.get("id")  # videos.list returns id as videoId string
                # ensure vid is a string (some responses sometimes have nested structure, but here it should be string)
                if isinstance(vid, dict):
                    vid = vid.get("videoId") or vid.get("id")
                snip = vi.get("snippet", {})
                cid = snip.get("channelId")
                title = snip.get("title", "")
                description = (snip.get("description") or "")[:300]
                tags = snip.get("tags") or []
                publish_ts = snip.get("publishedAt")
                publish_dt = parse_rfc3339_to_datetime(publish_ts)
                stats = vi.get("statistics", {})
                views = safe_int(stats.get("viewCount", 0))
                likes = safe_int(stats.get("likeCount", 0))
                comments = safe_int(stats.get("commentCount", 0))
                duration_s = parse_iso8601_duration_to_seconds(vi.get("contentDetails", {}).get("duration", "PT0S"))
                virality = compute_virality_score(views, publish_dt, now=now)
                thumbs = snip.get("thumbnails", {})
                thumbnail = (thumbs.get("medium") or thumbs.get("high") or thumbs.get("default") or {}).get("url")

                # channel title fallback
                ch_title = None
                if cid and cid in channel_map and channel_map[cid].get("title"):
                    ch_title = channel_map[cid]["title"]
                else:
                    ch_title = snip.get("channelTitle")

                # ensure channel_map entry exists
                if cid and cid not in channel_map:
                    channel_map[cid] = {"title": ch_title, "subs": None, "published_at": None, "country": None, "avatar": None, "sample_videos": []}

                # FIX: use vid_to_kw[vid] — this ensures correct keyword mapping for each video
                kw_for_vid = vid_to_kw.get(vid, "")

                row = {
                    "keyword": kw_for_vid,
                    "title": title,
                    "description": description,
                    "tags": ",".join(tags),
                    "url": f"https://www.youtube.com/watch?v={vid}",
                    "views": views,
                    "likes": likes,
                    "comments": comments,
                    "duration_seconds": duration_s,
                    "duration_readable": seconds_to_readable(duration_s),
                    "channel_id": cid,
                    "channel_title": ch_title,
                    "channel_subs": channel_map.get(cid, {}).get("subs"),
                    "thumbnail": thumbnail,
                    "published_at": publish_dt.isoformat() if publish_dt else None,
                    "virality": virality,
                    "monetization_likelihood": None
                }
                all_video_rows.append(row)
                if cid:
                    channel_map[cid].setdefault("sample_videos", [])
                    channel_map[cid]["sample_videos"].append(row)

            progress.progress(int(i/len(keywords)*100))

        status.text("Computing channel-level metrics...")
        # build channel cards
        channel_cards = []
        for cid, cinfo in channel_map.items():
            sv = cinfo.get("sample_videos", [])
            if not sv:
                continue

            # country filter
            ch_country = cinfo.get("country")
            if country_filter:
                cf = country_filter.strip().lower()
                if ch_country:
                    if cf not in str(ch_country).lower():
                        continue
                else:
                    continue

            sample_count = len(sv)
            avg_duration = sum(v["duration_seconds"] for v in sv) / sample_count
            avg_views_sample = sum(v["views"] for v in sv) / sample_count
            published_raw = cinfo.get("published_at")
            ch_published_dt = parse_rfc3339_to_datetime(published_raw) if published_raw else None
            if ch_published_dt:
                ch_age_months = max(0, (now.year - ch_published_dt.year) * 12 + (now.month - ch_published_dt.month))
            else:
                ch_age_months = None

            # max age filter
            if max_channel_age_months and max_channel_age_months > 0:
                if ch_age_months is None:
                    continue
                if ch_age_months > max_channel_age_months:
                    continue

            virality_list = [v["virality"] for v in sv]
            highest_virality = max(virality_list) if virality_list else 0
            median_virality = int(sorted(virality_list)[len(virality_list)//2]) if virality_list else 0
            subs = cinfo.get("subs") or 0
            monet = monetization_likelihood(subs, avg_views_sample, ch_age_months)
            ch_title = cinfo.get("title") or (sv[0].get("channel_title") if sv and sv[0].get("channel_title") else cid)
            for v in sv:
                v["monetization_likelihood"] = monet
                v["channel_title"] = ch_title
            card = {
                "channel_id": cid,
                "channel_title": ch_title,
                "subs": subs,
                "channel_age_months": ch_age_months,
                "country": cinfo.get("country"),
                "avatar": cinfo.get("avatar"),
                "avg_duration_seconds": avg_duration,
                "avg_duration_readable": seconds_to_readable(avg_duration),
                "sample_count": sample_count,
                "avg_views_sample": int(avg_views_sample),
                "highest_virality": highest_virality,
                "median_virality": median_virality,
                "monetization_likelihood": monet,
                "sample_videos": sorted(sv, key=lambda x: x["views"], reverse=True)
            }
            if min_channel_subs and card["subs"] < min_channel_subs:
                continue
            if only_shorts and card["avg_duration_seconds"] >= 60:
                continue
            channel_cards.append(card)

        # sort channels
        channel_cards = sorted(channel_cards, key=lambda x: x["highest_virality"], reverse=True)

        # prepare DB rows and CSV rows (CSV excludes channel_id; includes channel_title and published date)
        saved_rows_for_db = []
        csv_rows = []
        for c in channel_cards:
            for sv in c["sample_videos"]:
                row_db = {
                    "keyword": sv["keyword"],
                    "title": sv["title"],
                    "channel_id": sv.get("channel_id"),
                    "channel_title": sv.get("channel_title") or c.get("channel_title"),
                    "channel_subs": sv.get("channel_subs") or c.get("subs") or 0,
                    "views": sv["views"],
                    "likes": sv["likes"],
                    "comments": sv["comments"],
                    "duration_seconds": sv["duration_seconds"],
                    "thumbnail": sv.get("thumbnail"),
                    "published_at": sv.get("published_at"),
                    "virality": sv["virality"],
                    "monetization_likelihood": sv["monetization_likelihood"]
                }
                saved_rows_for_db.append(row_db)
                csv_rows.append({
                    "keyword": row_db["keyword"],
                    "title": row_db["title"],
                    "channel_title": row_db["channel_title"],
                    "channel_subs": row_db["channel_subs"],
                    "views": row_db["views"],
                    "likes": row_db["likes"],
                    "comments": row_db["comments"],
                    "duration_seconds": row_db["duration_seconds"],
                    "thumbnail": row_db["thumbnail"],
                    "published_at": row_db["published_at"],
                    "virality": row_db["virality"],
                    "monetization_likelihood": row_db["monetization_likelihood"]
                })

        # auto-save CSV
        csv_file = None
        if auto_save_csv and csv_rows:
            ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            fname = f"viral_scope_run_{ts}_{uuid.uuid4().hex[:6]}.csv"
            keys = list(csv_rows[0].keys())
            with open(fname, "w", newline='', encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()
                writer.writerows(csv_rows)
            csv_file = fname
            st.success(f"CSV saved: {csv_file} (channel_name shown)")

        # save to DB
        if save_to_db and saved_rows_for_db:
            run_id = uuid.uuid4().hex
            save_run_to_db(run_id, now, days, keywords, note, saved_rows_for_db)
            st.success("Run saved to local DB for trends (channel_title stored)")

        # DISPLAY cards — each sample shows published date (UTC)
        st.markdown("### Results")
        if not channel_cards:
            st.info("No channels matched filters.")
        else:
            cols = st.columns(2)
            for idx, c in enumerate(channel_cards):
                col = cols[idx % 2]
                with col:
                    st.markdown("<div class='card'>", unsafe_allow_html=True)
                    st.markdown(f"**Channel:** {c.get('channel_title') or c.get('channel_id')}")
                    st.markdown(f"<div class='small'>Subscribers: {c['subs']} • Country: {c.get('country') or 'N/A'} • Age (months): {c.get('channel_age_months') or 'N/A'}</div>", unsafe_allow_html=True)
                    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
                    st.markdown(f"<div><span class='pill'>Virality: {c['highest_virality']}</span><span class='pill'>Median: {c['median_virality']}</span><span class='pill'>Monet: {c['monetization_likelihood']}%</span></div>", unsafe_allow_html=True)
                    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
                    st.markdown(f"**Avg duration (sample):** {c['avg_duration_readable']}  \n**Avg views (sample):** {c['avg_views_sample']}", unsafe_allow_html=True)
                    st.markdown("<div style='margin-top:8px'><b>Top sample videos</b></div>", unsafe_allow_html=True)
                    for sv in c['sample_videos'][:6]:
                        thumb = sv.get('thumbnail')
                        title = sv.get('title')
                        url = sv.get('url')
                        views = sv.get('views')
                        dur = sv.get('duration_readable', 'N/A')
                        vir = sv.get('virality', 0)
                        pub = sv.get('published_at')
                        pub_read = pub if pub else "N/A"
                        if thumb:
                            st.markdown(
                                f"<div class='sample-item'><img class='thumb' src='{thumb}' alt='thumb'/>"
                                f" <div class='meta'><a href='{url}' target='_blank'><b>{title}</b></a>"
                                f"<div class='small'>Published(UTC): {pub_read} • Views: {views} • Duration: {dur} • Virality: {vir}</div></div></div>",
                                unsafe_allow_html=True)
                        else:
                            st.markdown(f"{title} — Published(UTC): {pub_read} • Views: {views} • Duration: {dur} • Virality: {vir}")

                    st.markdown("</div>", unsafe_allow_html=True)

        # optional raw table
        if show_raw:
            st.markdown("---")
            if csv_rows:
                df_raw = pd.DataFrame(csv_rows)
                st.dataframe(df_raw.head(1000))

        status.text("Done.")
        progress.empty()

    except Exception as err:
        st.error(f"Error: {err}")
        progress.empty()
        status.empty()

# -------------------------
# Trends (channel-level)
# -------------------------
st.markdown("---")
st.markdown("### Trends Dashboard (channel-level)")
runs_df = load_runs_summary()
if runs_df.empty:
    st.info("No runs saved. Enable 'Save run to local DB' and run the crawler to build history.")
else:
    st.dataframe(runs_df)
    conn = sqlite3.connect(DB_FILE)
    channel_df = pd.read_sql_query("SELECT DISTINCT channel_id, channel_title FROM video_samples WHERE channel_title IS NOT NULL", conn)
    channel_df = channel_df.drop_duplicates(subset=['channel_id'], keep='last')
    channel_map_display = {row['channel_title']: row['channel_id'] for _, row in channel_df.iterrows()}
    conn.close()

    sel_channel_title = st.selectbox("Select channel (by name) for trend", options=[""] + list(channel_map_display.keys()))
    if sel_channel_title:
        sel_channel_id = channel_map_display.get(sel_channel_title)
        df_ch = load_samples_for_channel(sel_channel_id)
        if df_ch.empty:
            st.warning("No data for this channel.")
        else:
            df_ch['saved_at'] = pd.to_datetime(df_ch['saved_at'])
            agg = df_ch.groupby('saved_at').agg({'views':'sum','virality':'mean'}).reset_index().sort_values('saved_at')
            agg = agg.set_index('saved_at')
            st.line_chart(agg)
            st.table(agg.tail(20).assign(views=lambda x: x['views'].astype(int), virality=lambda x: x['virality'].round(1)))

st.markdown("Run the crawler periodically to collect trends.")
