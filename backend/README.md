Run backend:

```bash
python3 /Users/macbook/Desktop/2/backend/server.py
```

It serves:

- `GET /api/catalog`
- `GET /api/saved-videos?owner_id=<telegram_user_id>`
- `GET /api/video-file?video_id=<id>`
- `POST /api/send-video`
- `POST /api/delete-saved-video`
- static webapp on `/`

Required env:

- `BOT_TOKEN`
- optional `BACKEND_HOST`
- optional `BACKEND_PORT`

Render deploy:

- push project to GitHub
- create a new Blueprint in Render
- select the repo
- Render will read `render.yaml`
- set `BOT_TOKEN`
- set `ADMIN_ID` if needed

Important:

- this project now includes `run_render.sh` so Render starts both `bot.py` and `backend/server.py`
- keep the persistent disk enabled, otherwise `app.db` can be lost after redeploy/restart
