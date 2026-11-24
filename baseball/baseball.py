
import asyncio
import random
import string
from dataclasses import dataclass, field
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field


app = FastAPI(title="Online Number Baseball")

STATIC_DIR = Path(__file__).parent / "static"


@dataclass
class GameRoom:
    code: str
    secret: str
    host_id: str
    players: dict = field(default_factory=dict)
    connections: dict = field(default_factory=dict)
    history: list = field(default_factory=list)
    finished: bool = False
    winner: str | None = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


rooms: dict[str, GameRoom] = {}


def generate_number():
    """첫 자리는 1~9, 나머지는 0~9 중복 없이 생성"""
    first = random.choice(string.digits[1:])
    rest = random.sample(
        [x for x in string.digits if x != first], 2
    )
    return first + "".join(rest)


def generate_room_code():
    while True:
        code = "".join(random.choices(
            string.ascii_uppercase + string.digits, k=6
        ))
        if code not in rooms:
            return code


def check_number(secret: str, guess: str):
    strike = sum(a == b for a, b in zip(secret, guess))
    ball = sum(
        digit in secret for digit in guess
    ) - strike

    if strike == 0 and ball == 0:
        return {"strike": 0, "ball": 0, "result": "OUT"}

    return {
        "strike": strike,
        "ball": ball,
        "result": f"{strike}S {ball}B"
    }


def get_game_state(room: GameRoom):
    return {
        "type": "state",
        "room_code": room.code,
        "players": [
            {
                "id": pid,
                "name": player["name"],
                "online": pid in room.connections
            }
            for pid, player in room.players.items()
        ],
        "history": room.history[-50:],
        "finished": room.finished,
        "winner": room.winner,
        "host_id": room.host_id
    }


async def broadcast(room: GameRoom):
    state = get_game_state(room)

    disconnected = []

    for pid, ws in room.connections.items():
        try:
            await ws.send_json(state)
        except Exception:
            disconnected.append(pid)

    for pid in disconnected:
        room.connections.pop(pid, None)


class CreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=20)


class JoinRequest(BaseModel):
    room_code: str
    name: str = Field(min_length=1, max_length=20)


@app.get("/")
async def home():
    return FileResponse(STATIC_DIR / "index.html")


@app.post("/api/rooms")
async def create_room(data: CreateRequest):
    code = generate_room_code()
    player_id = "".join(
        random.choices(string.ascii_letters + string.digits, k=16)
    )

    room = GameRoom(
        code=code,
        secret=generate_number(),
        host_id=player_id
    )

    room.players[player_id] = {"name": data.name}
    rooms[code] = room

    return {
        "room_code": code,
        "player_id": player_id,
        "name": data.name
    }


@app.post("/api/rooms/join")
async def join_room(data: JoinRequest):
    code = data.room_code.upper().strip()
    room = rooms.get(code)

    if not room:
        raise HTTPException(404, "존재하지 않는 게임방입니다.")

    player_id = "".join(
        random.choices(string.ascii_letters + string.digits, k=16)
    )

    async with room.lock:
        room.players[player_id] = {"name": data.name}
        await broadcast(room)

    return {
        "room_code": code,
        "player_id": player_id,
        "name": data.name
    }


@app.websocket("/ws/{room_code}/{player_id}")
async def game_socket(
    websocket: WebSocket,
    room_code: str,
    player_id: str
):
    room = rooms.get(room_code.upper())

    if not room or player_id not in room.players:
        await websocket.close(code=1008)
        return

    await websocket.accept()

    async with room.lock:
        room.connections[player_id] = websocket
        await broadcast(room)

    try:
        while True:
            message = await websocket.receive_json()
            action = message.get("action")

            async with room.lock:

                if action == "guess":
                    guess = str(message.get("number", ""))

                    if room.finished:
                        await websocket.send_json({
                            "type": "error",
                            "message": "이미 게임이 종료되었습니다."
                        })
                        continue

                    if (
                        len(guess) != 3
                        or not guess.isdigit()
                        or guess[0] == "0"
                        or len(set(guess)) != 3
                    ):
                        await websocket.send_json({
                            "type": "error",
                            "message": "중복 없는 3자리 숫자를 입력하세요."
                        })
                        continue

                    result = check_number(room.secret, guess)

                    record = {
                        "player_id": player_id,
                        "player_name": room.players[player_id]["name"],
                        "guess": guess,
                        "strike": result["strike"],
                        "ball": result["ball"],
                        "result": result["result"]
                    }

                    room.history.append(record)

                    if result["strike"] == 3:
                        room.finished = True
                        room.winner = room.players[player_id]["name"]

                    await broadcast(room)

                elif action == "restart":
                    if player_id != room.host_id:
                        await websocket.send_json({
                            "type": "error",
                            "message": "방장만 게임을 초기화할 수 있습니다."
                        })
                        continue

                    room.secret = generate_number()
                    room.history.clear()
                    room.finished = False
                    room.winner = None

                    await broadcast(room)

    except WebSocketDisconnect:
        async with room.lock:
            if room.connections.get(player_id) is websocket:
                room.connections.pop(player_id, None)
            await broadcast(room)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
