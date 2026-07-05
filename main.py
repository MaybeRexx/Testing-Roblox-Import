import io
import zipfile
import asyncio
from typing import List

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

app = FastAPI(title="RBX3D Proxy")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

USERS_API = "https://users.roblox.com/v1/usernames/users"
THUMBS_API = "https://thumbnails.roblox.com/v1/users/avatar-3d"


class BatchRequest(BaseModel):
    usernames: List[str]


async def resolve_user_id(client: httpx.AsyncClient, username: str) -> dict:
    resp = await client.post(
        USERS_API,
        json={"usernames": [username], "excludeBannedUsers": True},
    )
    resp.raise_for_status()
    data = resp.json().get("data", [])
    if not data:
        raise HTTPException(404, f"Username '{username}' not found")
    return data[0]


async def fetch_avatar3d(client: httpx.AsyncClient, user_id: int) -> dict:
    resp = await client.get(THUMBS_API, params={"userId": user_id})
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("state") != "Completed" or not payload.get("imageUrl"):
        raise HTTPException(422, "Avatar 3D not ready or unavailable for this user")
    return payload


async def fetch_mesh_manifest(client: httpx.AsyncClient, image_url: str) -> dict:
    resp = await client.get(image_url)
    resp.raise_for_status()
    return resp.json()


@app.get("/api/user/{username}")
async def get_user(username: str):
    async with httpx.AsyncClient(timeout=15) as client:
        user = await resolve_user_id(client, username)
        avatar = await fetch_avatar3d(client, user["id"])
        manifest = await fetch_mesh_manifest(client, avatar["imageUrl"])
        return {"user": user, "manifest": manifest}


async def build_user_zip(client: httpx.AsyncClient, username: str) -> tuple[str, bytes]:
    user = await resolve_user_id(client, username)
    avatar = await fetch_avatar3d(client, user["id"])
    manifest = await fetch_mesh_manifest(client, avatar["imageUrl"])

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        obj_bytes = (await client.get(manifest["obj"])).content
        zf.writestr(f"{username}.obj", obj_bytes)

        mtl_bytes = (await client.get(manifest["mtl"])).content
        zf.writestr(f"{username}.mtl", mtl_bytes)

        for i, tex_url in enumerate(manifest.get("textures", [])):
            tex_bytes = (await client.get(tex_url)).content
            zf.writestr(f"{username}_tex_{i}.png", tex_bytes)

    buf.seek(0)
    return username, buf.read()


@app.get("/api/download/{username}")
async def download_single(username: str):
    async with httpx.AsyncClient(timeout=30) as client:
        name, data = await build_user_zip(client, username)
    return StreamingResponse(
        io.BytesIO(data),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{name}_3d.zip"'},
    )


@app.post("/api/batch")
async def download_batch(req: BatchRequest):
    if not req.usernames:
        raise HTTPException(400, "No usernames provided")
    if len(req.usernames) > 25:
        raise HTTPException(400, "Max 25 usernames per batch")

    async with httpx.AsyncClient(timeout=30) as client:
        results = await asyncio.gather(
            *(build_user_zip(client, u) for u in req.usernames),
            return_exceptions=True,
        )

    master_buf = io.BytesIO()
    with zipfile.ZipFile(master_buf, "w", zipfile.ZIP_DEFLATED) as master:
        for item in results:
            if isinstance(item, Exception):
                continue
            name, data = item
            master.writestr(f"{name}_3d.zip", data)
    master_buf.seek(0)

    return StreamingResponse(
        master_buf,
        media_type="application/zip",
        headers={"Content-Disposition": 'attachment; filename="rbx3d_batch.zip"'},
    )


@app.get("/api/health")
async def health():
    return {"status": "ok"}
