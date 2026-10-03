from fastapi import FastAPI, Request, Form, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from typing import Optional
import asyncio
import sqlite3
import random
import string


# =========================================================
# APP / PATHS
# =========================================================

BASE_DIR = Path(__file__).resolve().parent

DATABASE = BASE_DIR / "game.db"
TEMPLATE_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(title="Truth or Lie")

templates = Jinja2Templates(
    directory=str(TEMPLATE_DIR)
)

app.mount(
    "/static",
    StaticFiles(directory=str(STATIC_DIR)),
    name="static"
)


MAX_PLAYERS = 10
RESULTS_DELAY_SECONDS = 5


# =========================================================
# DATABASE
# =========================================================

def get_db():
    conn = sqlite3.connect(
        DATABASE,
        timeout=30,
        check_same_thread=False
    )

    conn.row_factory = sqlite3.Row

    conn.execute(
        "PRAGMA busy_timeout = 30000"
    )

    conn.execute(
        "PRAGMA foreign_keys = ON"
    )

    return conn


def init_db():

    conn = get_db()

    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS games (
            id TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'waiting',
            current_player_index INTEGER NOT NULL DEFAULT 0
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS players (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT NOT NULL,
            name TEXT NOT NULL,
            score INTEGER NOT NULL DEFAULT 0
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS rounds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT NOT NULL,
            player_id INTEGER NOT NULL,
            round_number INTEGER NOT NULL,
            statement1 TEXT NOT NULL,
            statement2 TEXT NOT NULL,
            statement3 TEXT NOT NULL,
            lie INTEGER NOT NULL,
            revealed INTEGER NOT NULL DEFAULT 0
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            round_id INTEGER NOT NULL,
            player_id INTEGER NOT NULL,
            vote INTEGER NOT NULL,
            UNIQUE(round_id, player_id)
        )
    """)

    conn.commit()
    conn.close()


init_db()


# =========================================================
# WEBSOCKET CONNECTION MANAGER
# =========================================================

class ConnectionManager:

    def __init__(self):

        self.connections = {}


    async def connect(
        self,
        websocket: WebSocket,
        game_id: str,
        player_id: int
    ):

        await websocket.accept()

        self.connections[websocket] = (
            game_id.upper(),
            player_id
        )


    def disconnect(self, websocket: WebSocket):

        self.connections.pop(
            websocket,
            None
        )


    async def send_state(
        self,
        websocket: WebSocket
    ):

        info = self.connections.get(websocket)

        if not info:
            return

        game_id, player_id = info

        state = get_game_state(
            game_id,
            player_id
        )

        try:

            await websocket.send_json(state)

        except Exception:

            self.disconnect(websocket)


    async def broadcast_game(
        self,
        game_id: str
    ):

        game_id = game_id.upper()

        targets = [
            websocket
            for websocket, (
                connected_game_id,
                _
            ) in self.connections.items()
            if connected_game_id == game_id
        ]

        for websocket in targets:

            await self.send_state(
                websocket
            )


manager = ConnectionManager()


# One automatic-advance task per game.
advance_tasks = {}


# =========================================================
# HELPERS
# =========================================================

def generate_game_code():

    characters = (
        string.ascii_uppercase +
        string.digits
    )

    while True:

        code = "".join(
            random.choices(
                characters,
                k=6
            )
        )

        conn = get_db()

        game = conn.execute(
            """
            SELECT id
            FROM games
            WHERE id = ?
            """,
            (code,)
        ).fetchone()

        conn.close()

        if not game:
            return code


def get_game(game_id):

    conn = get_db()

    game = conn.execute(
        """
        SELECT *
        FROM games
        WHERE id = ?
        """,
        (game_id.upper(),)
    ).fetchone()

    conn.close()

    return game


def get_players(game_id):

    conn = get_db()

    players = conn.execute(
        """
        SELECT *
        FROM players
        WHERE game_id = ?
        ORDER BY id
        """,
        (game_id.upper(),)
    ).fetchall()

    conn.close()

    return players


def get_scoreboard(game_id):

    conn = get_db()

    players = conn.execute(
        """
        SELECT *
        FROM players
        WHERE game_id = ?
        ORDER BY score DESC, id ASC
        """,
        (game_id.upper(),)
    ).fetchall()

    conn.close()

    return players


def get_player(
    game_id,
    player_id
):

    conn = get_db()

    player = conn.execute(
        """
        SELECT *
        FROM players
        WHERE game_id = ?
        AND id = ?
        """,
        (
            game_id.upper(),
            player_id
        )
    ).fetchone()

    conn.close()

    return player


def get_current_player(game_id):

    game = get_game(game_id)

    if not game:
        return None

    players = get_players(game_id)

    if not players:
        return None

    index = (
        game["current_player_index"]
        % len(players)
    )

    return players[index]


def get_latest_round(game_id):

    conn = get_db()

    round_data = conn.execute(
        """
        SELECT *
        FROM rounds
        WHERE game_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (game_id.upper(),)
    ).fetchone()

    conn.close()

    return round_data


def get_vote_count(round_id):

    conn = get_db()

    count = conn.execute(
        """
        SELECT COUNT(*)
        FROM votes
        WHERE round_id = ?
        """,
        (round_id,)
    ).fetchone()[0]

    conn.close()

    return count


def build_redirect(
    game_id,
    player_id,
    status=None
):

    game = get_game(game_id)

    if not game:
        return "/"


    if status is None:
        status = game["status"]


    if status == "finished":

        return f"/final/{game_id}"


    if status == "voting":

        return (
            f"/vote/{game_id}"
            f"?player_id={player_id}"
        )


    if status == "results":

        return (
            f"/results/{game_id}"
            f"?player_id={player_id}"
        )


    current_player = get_current_player(
        game_id
    )


    if (
        status == "playing"
        and current_player
        and current_player["id"] == player_id
    ):

        return (
            f"/create/{game_id}"
            f"?player_id={player_id}"
        )


    return (
        f"/room/{game_id}"
        f"?player_id={player_id}"
    )


# =========================================================
# GAME STATE SENT THROUGH WEBSOCKET
# =========================================================

def get_game_state(
    game_id,
    player_id
):

    game_id = game_id.upper()

    game = get_game(game_id)

    if not game:

        return {
            "game_id": game_id,
            "status": "missing",
            "players": [],
            "current_player_id": None,
            "redirect": "/"
        }


    players = get_players(game_id)

    current_player = get_current_player(
        game_id
    )

    latest_round = get_latest_round(
        game_id
    )


    vote_count = 0

    if (
        latest_round
        and game["status"] == "voting"
    ):

        vote_count = get_vote_count(
            latest_round["id"]
        )


    return {

        "game_id": game_id,

        "status": game["status"],

        "players": [

            {
                "id": player["id"],
                "name": player["name"],
                "score": player["score"]
            }

            for player in players
        ],

        "current_player_id": (

            current_player["id"]

            if current_player

            else None
        ),

        "current_player_name": (

            current_player["name"]

            if current_player

            else None
        ),

        "round_id": (

            latest_round["id"]

            if latest_round

            else None
        ),

        "vote_count": vote_count,

        "redirect": build_redirect(
            game_id,
            player_id,
            game["status"]
        )
    }


async def broadcast(game_id):

    await manager.broadcast_game(
        game_id
    )


# =========================================================
# AUTOMATIC NEXT ROUND
# =========================================================

async def auto_advance_round(
    game_id,
    round_id
):

    try:

        await asyncio.sleep(
            RESULTS_DELAY_SECONDS
        )

        conn = get_db()

        conn.execute(
            "BEGIN IMMEDIATE"
        )


        game = conn.execute(
            """
            SELECT *
            FROM games
            WHERE id = ?
            """,
            (game_id,)
        ).fetchone()


        round_data = conn.execute(
            """
            SELECT *
            FROM rounds
            WHERE id = ?
            AND game_id = ?
            """,
            (
                round_id,
                game_id
            )
        ).fetchone()


        players = conn.execute(
            """
            SELECT *
            FROM players
            WHERE game_id = ?
            ORDER BY id
            """,
            (game_id,)
        ).fetchall()


        if (
            not game
            or not round_data
            or game["status"] != "results"
            or not players
        ):

            conn.rollback()
            conn.close()

            return


        current_index = (
            game["current_player_index"]
            % len(players)
        )


        next_index = (
            current_index + 1
        ) % len(players)


        conn.execute(
            """
            UPDATE games
            SET
                current_player_index = ?,
                status = 'playing'
            WHERE id = ?
            """,
            (
                next_index,
                game_id
            )
        )


        conn.commit()
        conn.close()


        await broadcast(
            game_id
        )


    except asyncio.CancelledError:

        raise


    except Exception:

        try:
            conn.close()
        except Exception:
            pass


    finally:

        advance_tasks.pop(
            game_id,
            None
        )


def schedule_auto_advance(
    game_id,
    round_id
):

    existing = advance_tasks.get(
        game_id
    )


    if (
        existing
        and not existing.done()
    ):

        return


    advance_tasks[game_id] = (
        asyncio.create_task(
            auto_advance_round(
                game_id,
                round_id
            )
        )
    )


# =========================================================
# HOME
# =========================================================

@app.get("/",response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="home.html",
        context={}
    )


# =========================================================
# CREATE GAME
# =========================================================

@app.post("/create")
def create_game(
    name: str = Form(...)
):

    name = name.strip()


    if not name:

        return RedirectResponse(
            "/",
            status_code=303
        )


    game_id = generate_game_code()


    conn = get_db()

    cursor = conn.cursor()


    cursor.execute(
        """
        INSERT INTO games (
            id,
            status,
            current_player_index
        )
        VALUES (
            ?,
            'waiting',
            0
        )
        """,
        (game_id,)
    )


    cursor.execute(
        """
        INSERT INTO players (
            game_id,
            name,
            score
        )
        VALUES (?, ?, 0)
        """,
        (
            game_id,
            name[:30]
        )
    )


    player_id = cursor.lastrowid


    conn.commit()
    conn.close()


    return RedirectResponse(
        (
            f"/room/{game_id}"
            f"?player_id={player_id}"
        ),
        status_code=303
    )


# =========================================================
# JOIN PAGE
# =========================================================

@app.get(
    "/join/{game_id}",
    response_class=HTMLResponse
)
def join_game_page(
    request: Request,
    game_id: str
):

    game_id = game_id.upper()

    game = get_game(game_id)


    if not game:

        return templates.TemplateResponse(
        request=request,
        name="home.html",
        context={
"error": "Game not found."
            }
        )
        


    return templates.TemplateResponse(
        request=request,
        name="join.html",
        context={
            "game": game
        }
)


# =========================================================
# JOIN GAME
# =========================================================

@app.post("/join/{game_id}")
async def join_game(
    game_id: str,
    name: str = Form(...)
):

    game_id = game_id.upper()

    name = name.strip()


    if not name:

        return RedirectResponse(
            f"/join/{game_id}",
            status_code=303
        )


    conn = get_db()

    conn.execute(
        "BEGIN IMMEDIATE"
    )


    game = conn.execute(
        """
        SELECT *
        FROM games
        WHERE id = ?
        """,
        (game_id,)
    ).fetchone()


    if not game:

        conn.rollback()
        conn.close()

        return RedirectResponse(
            "/",
            status_code=303
        )


    players = conn.execute(
        """
        SELECT *
        FROM players
        WHERE game_id = ?
        ORDER BY id
        """,
        (game_id,)
    ).fetchall()


    if (
        len(players) >= MAX_PLAYERS
        or game["status"] != "waiting"
    ):

        conn.rollback()
        conn.close()

        return RedirectResponse(
            f"/join/{game_id}",
            status_code=303
        )


    cursor = conn.execute(
        """
        INSERT INTO players (
            game_id,
            name,
            score
        )
        VALUES (?, ?, 0)
        """,
        (
            game_id,
            name[:30]
        )
    )


    player_id = cursor.lastrowid


    conn.commit()
    conn.close()


    await broadcast(
        game_id
    )


    return RedirectResponse(
        (
            f"/room/{game_id}"
            f"?player_id={player_id}"
        ),
        status_code=303
    )


# =========================================================
# START GAME
# The first player to join is the host.
# =========================================================

@app.post("/start/{game_id}")
async def start_game(
    game_id: str,
    player_id: int = Form(...)
):

    game_id = game_id.upper()

    conn = get_db()
    conn.execute("BEGIN IMMEDIATE")

    game = conn.execute(
        """
        SELECT *
        FROM games
        WHERE id = ?
        """,
        (game_id,)
    ).fetchone()

    players = conn.execute(
        """
        SELECT *
        FROM players
        WHERE game_id = ?
        ORDER BY id
        """,
        (game_id,)
    ).fetchall()

    if (
        not game
        or game["status"] != "waiting"
        or len(players) < 2
        or players[0]["id"] != player_id
    ):
        conn.rollback()
        conn.close()
        return RedirectResponse(
            f"/room/{game_id}?player_id={player_id}",
            status_code=303
        )

    conn.execute(
        """
        UPDATE games
        SET status = 'playing',
            current_player_index = 0
        WHERE id = ?
        """,
        (game_id,)
    )

    conn.commit()
    conn.close()

    await broadcast(game_id)

    return RedirectResponse(
        build_redirect(game_id, player_id),
        status_code=303
    )


# =========================================================
# ROOM
# =========================================================

@app.get(
    "/room/{game_id}",
    response_class=HTMLResponse
)
def room(
    request: Request,
    game_id: str,
    player_id: Optional[int] = None
):

    game_id = game_id.upper()

    game = get_game(game_id)


    if not game:

        return RedirectResponse(
            "/",
            status_code=303
        )


    players = get_players(
        game_id
    )

    current_player = get_current_player(
        game_id
    )


    if (
        game["status"] == "voting"
        and player_id
    ):

        return RedirectResponse(
            (
                f"/vote/{game_id}"
                f"?player_id={player_id}"
            ),
            status_code=303
        )


    if (
        game["status"] == "results"
        and player_id
    ):

        return RedirectResponse(
            (
                f"/results/{game_id}"
                f"?player_id={player_id}"
            ),
            status_code=303
        )


    if game["status"] == "finished":

        return RedirectResponse(
            f"/final/{game_id}",
            status_code=303
        )


    if (
        game["status"] == "playing"
        and current_player
        and player_id
        and current_player["id"] == player_id
    ):

        return RedirectResponse(
            (
                f"/create/{game_id}"
                f"?player_id={player_id}"
            ),
            status_code=303
        )


    return templates.TemplateResponse(
        request=request,
        name="room.html",
        context={
            "game": game,
            "players": players,
            "player_id": player_id,
            "player": (
                {"id": player_id}
                if player_id
                else None
            ),
            "is_host": (
                bool(players)
                and player_id == players[0]["id"]
            ),
            "current_player": current_player
        }
    )


# =========================================================
# CREATE STATEMENTS PAGE
# =========================================================

@app.get(
    "/create/{game_id}",
    response_class=HTMLResponse
)
def create_statements_page(
    request: Request,
    game_id: str,
    player_id: int
):

    game_id = game_id.upper()

    game = get_game(game_id)


    if not game:

        return RedirectResponse(
            "/",
            status_code=303
        )


    player = get_player(
        game_id,
        player_id
    )

    current_player = get_current_player(
        game_id
    )


    if (
        not player
        or not current_player
        or current_player["id"] != player_id
    ):

        return RedirectResponse(
            (
                f"/room/{game_id}"
                f"?player_id={player_id}"
            ),
            status_code=303
        )


    if game["status"] != "playing":

        return RedirectResponse(
            build_redirect(
                game_id,
                player_id
            ),
            status_code=303
        )


    return templates.TemplateResponse(
        request=request,
        name="create.html",
        context={
            "game_id": game_id,
            "player_id": player_id
        }
    )


# =========================================================
# SUBMIT STATEMENTS
# =========================================================

@app.post(
    "/create-round/{game_id}"
)
async def create_round(
    game_id: str,
    player_id: int = Form(...),
    statement1: str = Form(...),
    statement2: str = Form(...),
    statement3: str = Form(...),
    lie: int = Form(...)
):

    game_id = game_id.upper()


    statement1 = statement1.strip()
    statement2 = statement2.strip()
    statement3 = statement3.strip()


    if (
        lie not in (1, 2, 3)
        or not statement1
        or not statement2
        or not statement3
    ):

        return RedirectResponse(
            (
                f"/create/{game_id}"
                f"?player_id={player_id}"
            ),
            status_code=303
        )


    conn = get_db()

    conn.execute(
        "BEGIN IMMEDIATE"
    )


    game = conn.execute(
        """
        SELECT *
        FROM games
        WHERE id = ?
        """,
        (game_id,)
    ).fetchone()


    players = conn.execute(
        """
        SELECT *
        FROM players
        WHERE game_id = ?
        ORDER BY id
        """,
        (game_id,)
    ).fetchall()


    if (
        not game
        or game["status"] != "playing"
        or not players
    ):

        conn.rollback()
        conn.close()

        return RedirectResponse(
            (
                f"/room/{game_id}"
                f"?player_id={player_id}"
            ),
            status_code=303
        )


    current_index = (
        game["current_player_index"]
        % len(players)
    )


    if (
        players[current_index]["id"]
        != player_id
    ):

        conn.rollback()
        conn.close()

        return RedirectResponse(
            (
                f"/room/{game_id}"
                f"?player_id={player_id}"
            ),
            status_code=303
        )


    last_round = conn.execute(
        """
        SELECT MAX(round_number)
        FROM rounds
        WHERE game_id = ?
        """,
        (game_id,)
    ).fetchone()[0]


    round_number = (
        (last_round or 0) + 1
    )


    conn.execute(
        """
        INSERT INTO rounds (
            game_id,
            player_id,
            round_number,
            statement1,
            statement2,
            statement3,
            lie,
            revealed
        )
        VALUES (
            ?, ?, ?, ?, ?, ?, ?, 0
        )
        """,
        (
            game_id,
            player_id,
            round_number,
            statement1[:200],
            statement2[:200],
            statement3[:200],
            lie
        )
    )


    conn.execute(
        """
        UPDATE games
        SET status = 'voting'
        WHERE id = ?
        """,
        (game_id,)
    )


    conn.commit()
    conn.close()


    await broadcast(
        game_id
    )


    return RedirectResponse(
        (
            f"/vote/{game_id}"
            f"?player_id={player_id}"
        ),
        status_code=303
    )


# =========================================================
# VOTING PAGE
# =========================================================

@app.get(
    "/vote/{game_id}",
    response_class=HTMLResponse
)
def vote_page(
    request: Request,
    game_id: str,
    player_id: int
):

    game_id = game_id.upper()

    game = get_game(game_id)


    if not game:

        return RedirectResponse(
            "/",
            status_code=303
        )


    if game["status"] == "finished":

        return RedirectResponse(
            f"/final/{game_id}",
            status_code=303
        )


    if game["status"] == "results":

        return RedirectResponse(
            (
                f"/results/{game_id}"
                f"?player_id={player_id}"
            ),
            status_code=303
        )


    round_data = get_latest_round(
        game_id
    )


    if not round_data:

        return RedirectResponse(
            (
                f"/room/{game_id}"
                f"?player_id={player_id}"
            ),
            status_code=303
        )


    player = get_player(
        game_id,
        player_id
    )


    if not player:

        return RedirectResponse(
            "/",
            status_code=303
        )


    conn = get_db()


    existing_vote = conn.execute(
        """
        SELECT *
        FROM votes
        WHERE round_id = ?
        AND player_id = ?
        """,
        (
            round_data["id"],
            player_id
        )
    ).fetchone()


    vote_count = conn.execute(
        """
        SELECT COUNT(*)
        FROM votes
        WHERE round_id = ?
        """,
        (round_data["id"],)
    ).fetchone()[0]


    conn.close()


    return templates.TemplateResponse(
        request=request,
        name="vote.html",
        context={
            "game": game_id,
            "game_id": game_id,
            "round": round_data,
            "players": get_players(game_id),
            "player": player,
            "player_id": player_id,
            "is_owner": (
                round_data["player_id"]
                == player_id
            ),
            "existing_vote": existing_vote,
            "vote_count": vote_count
        }
    )


# =========================================================
# SUBMIT VOTE
# =========================================================

@app.post(
    "/vote/{game_id}"
)
async def submit_vote(
    game_id: str,
    player_id: int = Form(...),
    vote: int = Form(...)
):

    game_id = game_id.upper()


    if vote not in (1, 2, 3):

        return RedirectResponse(
            (
                f"/vote/{game_id}"
                f"?player_id={player_id}"
            ),
            status_code=303
        )


    conn = get_db()

    conn.execute(
        "BEGIN IMMEDIATE"
    )


    game = conn.execute(
        """
        SELECT *
        FROM games
        WHERE id = ?
        """,
        (game_id,)
    ).fetchone()


    round_data = conn.execute(
        """
        SELECT *
        FROM rounds
        WHERE game_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (game_id,)
    ).fetchone()


    player = conn.execute(
        """
        SELECT *
        FROM players
        WHERE id = ?
        AND game_id = ?
        """,
        (
            player_id,
            game_id
        )
    ).fetchone()


    if (
        not game
        or not round_data
        or not player
        or game["status"] != "voting"
    ):

        conn.rollback()
        conn.close()

        return RedirectResponse(
            (
                f"/room/{game_id}"
                f"?player_id={player_id}"
            ),
            status_code=303
        )


    # Statement owner cannot vote.
    if (
        round_data["player_id"]
        == player_id
    ):

        conn.rollback()
        conn.close()

        return RedirectResponse(
            (
                f"/vote/{game_id}"
                f"?player_id={player_id}"
            ),
            status_code=303
        )


    existing_vote = conn.execute(
        """
        SELECT id
        FROM votes
        WHERE round_id = ?
        AND player_id = ?
        """,
        (
            round_data["id"],
            player_id
        )
    ).fetchone()


    if not existing_vote:

        conn.execute(
            """
            INSERT INTO votes (
                round_id,
                player_id,
                vote
            )
            VALUES (?, ?, ?)
            """,
            (
                round_data["id"],
                player_id,
                vote
            )
        )


    total_players = conn.execute(
        """
        SELECT COUNT(*)
        FROM players
        WHERE game_id = ?
        """,
        (game_id,)
    ).fetchone()[0]


    total_votes = conn.execute(
        """
        SELECT COUNT(*)
        FROM votes
        WHERE round_id = ?
        """,
        (round_data["id"],)
    ).fetchone()[0]


    required_votes = (
        total_players - 1
    )


    should_reveal = (
        required_votes > 0
        and total_votes >= required_votes
        and round_data["revealed"] == 0
    )


    if should_reveal:

        correct_voters = conn.execute(
            """
            SELECT player_id
            FROM votes
            WHERE round_id = ?
            AND vote = ?
            """,
            (
                round_data["id"],
                round_data["lie"]
            )
        ).fetchall()


        for voter in correct_voters:

            conn.execute(
                """
                UPDATE players
                SET score = score + 1
                WHERE id = ?
                """,
                (voter["player_id"],)
            )


        conn.execute(
            """
            UPDATE rounds
            SET revealed = 1
            WHERE id = ?
            """,
            (round_data["id"],)
        )


        conn.execute(
            """
            UPDATE games
            SET status = 'results'
            WHERE id = ?
            """,
            (game_id,)
        )


    conn.commit()
    conn.close()


    if should_reveal:

        schedule_auto_advance(
            game_id,
            round_data["id"]
        )


    await broadcast(
        game_id
    )


    return RedirectResponse(
        build_redirect(
            game_id,
            player_id
        ),
        status_code=303
    )


# =========================================================
# RESULTS
# =========================================================

@app.get(
    "/results/{game_id}",
    response_class=HTMLResponse
)
async def results_page(
    request: Request,
    game_id: str,
    player_id: int
):

    game_id = game_id.upper()

    game = get_game(game_id)

    if not game:

        return RedirectResponse(
            "/",
            status_code=303
        )

    if game["status"] == "finished":

        return RedirectResponse(
            f"/final/{game_id}",
            status_code=303
        )

    if game["status"] == "voting":

        return RedirectResponse(
            (
                f"/vote/{game_id}"
                f"?player_id={player_id}"
            ),
            status_code=303
        )

    if game["status"] == "playing":

        return RedirectResponse(
            build_redirect(
                game_id,
                player_id
            ),
            status_code=303
        )

    round_data = get_latest_round(
        game_id
    )

    if not round_data:

        return RedirectResponse(
            (
                f"/room/{game_id}"
                f"?player_id={player_id}"
            ),
            status_code=303
        )

    if game["status"] == "results":

        schedule_auto_advance(
            game_id,
            round_data["id"]
        )

    return templates.TemplateResponse(
        request=request,
        name="results.html",
        context={
            "game": game_id,
            "game_id": game_id,
            "round": round_data,
            "players": get_scoreboard(game_id),
            "player_id": player_id
        }
    )


# =========================================================
# MANUAL NEXT ROUND
# Compatibility endpoint.
# Normal gameplay uses the automatic timer.
# =========================================================

@app.post(
    "/next-round/{game_id}"
)
async def next_round(
    game_id: str,
    player_id: int = Form(...)
):

    game_id = game_id.upper()


    task = advance_tasks.get(
        game_id
    )


    if task and not task.done():

        task.cancel()


    game = get_game(game_id)

    if not game:

        return RedirectResponse(
            "/",
            status_code=303
        )


    if game["status"] != "results":

        return RedirectResponse(
            build_redirect(
                game_id,
                player_id
            ),
            status_code=303
        )


    conn = get_db()

    conn.execute(
        "BEGIN IMMEDIATE"
    )


    players = conn.execute(
        """
        SELECT *
        FROM players
        WHERE game_id = ?
        ORDER BY id
        """,
        (game_id,)
    ).fetchall()


    if not players:

        conn.rollback()
        conn.close()

        return RedirectResponse(
            "/",
            status_code=303
        )


    current_index = (
        game["current_player_index"]
        % len(players)
    )


    next_index = (
        current_index + 1
    ) % len(players)


    conn.execute(
        """
        UPDATE games
        SET
            current_player_index = ?,
            status = 'playing'
        WHERE id = ?
        """,
        (
            next_index,
            game_id
        )
    )


    conn.commit()
    conn.close()


    await broadcast(
        game_id
    )


    return RedirectResponse(
        build_redirect(
            game_id,
            player_id
        ),
        status_code=303
    )


# =========================================================
# FINISH GAME
# =========================================================

@app.post(
    "/finish/{game_id}"
)
async def finish_game(
    game_id: str,
    player_id: int = Form(...)
):

    game_id = game_id.upper()

    game = get_game(game_id)


    if not game:

        return RedirectResponse(
            "/",
            status_code=303
        )


    round_data = get_latest_round(
        game_id
    )


    if (
        round_data
        and round_data["player_id"]
        != player_id
    ):

        return RedirectResponse(
            (
                f"/results/{game_id}"
                f"?player_id={player_id}"
            ),
            status_code=303
        )


    task = advance_tasks.get(
        game_id
    )


    if task and not task.done():

        task.cancel()


    conn = get_db()

    conn.execute(
        """
        UPDATE games
        SET status = 'finished'
        WHERE id = ?
        """,
        (game_id,)
    )

    conn.commit()
    conn.close()


    await broadcast(
        game_id
    )


    return RedirectResponse(
        f"/final/{game_id}",
        status_code=303
    )


# =========================================================
# FINAL SCORE
# =========================================================

@app.get(
    "/final/{game_id}",
    response_class=HTMLResponse
)
def final_page(
    request: Request,
    game_id: str
):

    game_id = game_id.upper()

    game = get_game(game_id)


    if not game:

        return RedirectResponse(
            "/",
            status_code=303
        )


    players = get_scoreboard(
        game_id
    )


    return templates.TemplateResponse(
        request=request,
        name="final.html",
        context={
            "game": game,
            "players": players
        }
    )


# =========================================================
# WEBSOCKET
# =========================================================

@app.websocket(
    "/ws/{game_id}/{player_id}"
)
async def websocket_endpoint(
    websocket: WebSocket,
    game_id: str,
    player_id: int
):

    game_id = game_id.upper()


    game = get_game(game_id)

    player = get_player(
        game_id,
        player_id
    )


    if not game or not player:

        await websocket.close(
            code=1008
        )

        return


    await manager.connect(
        websocket,
        game_id,
        player_id
    )


    try:

        await manager.send_state(
            websocket
        )


        while True:

            await websocket.receive_text()


    except WebSocketDisconnect:

        manager.disconnect(
            websocket
        )


    except Exception:

        manager.disconnect(
            websocket
        )


@app.get("/game-state/{game_id}/{player_id}")
def game_state_endpoint(
    game_id: str,
    player_id: int
):

    game_id = game_id.upper()

    if (
        not get_game(game_id)
        or not get_player(game_id, player_id)
    ):
        return {
            "game_id": game_id,
            "status": "missing",
            "players": [],
            "current_player_id": None,
            "redirect": "/"
        }

    return get_game_state(
        game_id,
        player_id
    )


# =========================================================
# HEALTH CHECK
# =========================================================

@app.get("/health")
def health():

    return {
        "status": "ok"
    }