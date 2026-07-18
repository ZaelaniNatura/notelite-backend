from fastapi import FastAPI, HTTPException, Query
import yt_dlp

app = FastAPI(title="Notelite Backend")

YDL_SEARCH_OPTS = {
    "quiet": True,
    "no_warnings": True,
    "extract_flat": True,
    "default_search": "ytsearch",
}

YDL_RESOLVE_OPTS = {
    "quiet": True,
    "no_warnings": True,
    "format": "bestaudio/best",
    "noplaylist": True,
    "extractor_args": {
        "youtube": {
            "player_client": ["android", "web"],
        }
    },
}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/search")
def search(q: str = Query(..., min_length=1), limit: int = 10):
    query = f"ytsearch{limit}:{q}"
    try:
        with yt_dlp.YoutubeDL(YDL_SEARCH_OPTS) as ydl:
            info = ydl.extract_info(query, download=False)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

    entries = info.get("entries", []) if info else []
    results = [
        {
            "id": e.get("id"),
            "title": e.get("title"),
            "duration": e.get("duration"),
        }
        for e in entries
        if e
    ]
    return {"results": results}


@app.get("/resolve")
def resolve(id: str = Query(..., min_length=1)):
    url = f"https://www.youtube.com/watch?v={id}"
    try:
        with yt_dlp.YoutubeDL(YDL_RESOLVE_OPTS) as ydl:
            info = ydl.extract_info(url, download=False)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

    if not info or "url" not in info:
        raise HTTPException(status_code=404, detail="no playable audio found")

    return {
        "title": info.get("title"),
        "duration": info.get("duration"),
        "audioUrl": info.get("url"),
    }
