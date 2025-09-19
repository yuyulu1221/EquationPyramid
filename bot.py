# bot.py
import os
import asyncio
import json
import time
import random
import ast
import io
import re
from typing import List, Tuple, Dict, Optional

import aiosqlite
from dotenv import load_dotenv
from telegram import Update, InputFile
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_PATH = "game.db"

# ------- 配置參數（可以調整） -------
TIME_LIMIT_DEFAULT = 180  # 每題秒數（3分鐘）
NUM_COUNT = 10            # 每題用幾個數字（固定 10）
REQUIRE_USE_ALL = False   # 新規則：僅需使用相鄰的三個數字
FIRST_CORRECT_POINTS = 2
LATER_CORRECT_POINTS = 1
# 答錯扣分
WRONG_PENALTY = 1
# 若要實作「找完所有解則提早結束」，請設定相關策略；目前占位，待確認需求
EARLY_END_ON_ALL_SOLVED = False
# ------------------------------------

# ------- 常用文字模板 -------
MSG_NO_ACTIVE_GAME = "目前沒有進行中的題目。"
MSG_GAME_IN_PROGRESS = "目前已有題目在進行中，请先 /end 或等待結束。"
MSG_SESSION_COMPLETE = "賽局已達到設定回合數，請 /endgame 結算或 /newgame 重新開始。"
MSG_NO_ACTIVE_SESSION = "目前沒有進行中的賽局。"
MSG_NO_SCORES = "目前還沒有分數紀錄。"
MSG_SYNC_ENDED = "已同步結束當前題目。"
MSG_TERMINATED = "本題已被終止。"
MSG_ALL_SOLVED_EARLY = "✅ 本題所有可行組合皆已被答出，提前結束！"
MSG_TIME_UP = "時間到！本題結束。"
MSG_ONE_MIN_REMAINING = "⏰ 剩 1 分鐘！請盡快提交你的答案（回覆三個字母 A-J）。"

# 答題相關訊息
MSG_INVALID_ADJACENT = "❌ {name}，答案不合規則：需為直線相鄰三點。"
MSG_WRONG_ANSWER = "❌ {name}，這三個數字無法組成目標，已扣 {penalty} 分（總分：{score}）。"
MSG_CORRECT_ANSWER = "✅ {name} 答對！獲得 {points} 分（總分：{score}）\n已解組合 {solved}/{total}"
MSG_ALREADY_SCORED = "{name} 已以此組合得過分，這次不再加分。試試不同的相鄰組合！"
MSG_ALREADY_TAKEN = "答對，但此組合已有他人搶先得分。試試不同的相鄰組合！"

# 指令說明
CMD_HELP_TEXT = (
    "/new - 出一題（若已有題目請先 /end或等待結束）\n"
    "/newgame N - 開新賽局，共 N 題（預設 5）\n"
    "/end - 強制結束本題（會 reveal 解答）\n"
    "/endgame - 提前結束賽局並結算\n"
    "/score - 查看自己分數 /leaderboard - 排行榜\n\n"
    "作答：回覆三個字母 A-J（直線相鄰，例：ABD）。\n"
    "規則：僅允許直線相鄰三點，四則運算可得目標即算答對。"
)

# 題目相關文字
QUESTION_PREFIX = "🔢 題目：目標 {target}"
QUESTION_WITH_PYRAMID = "🔢 題目：目標 {target}\n{pyramid}"
QUESTION_INSTRUCTION = "作答：直接回覆三個字母 A-J（例：ABD）。"
QUESTION_RULE = "規則：只能選『直線相鄰的三個數字』（依 A-J 標籤）。"
QUESTION_HINT = "提示：本題共有 {count} 組可行解。"
QUESTION_SOLUTIONS = "所有可行解：\n{solutions}"
QUESTION_LEADERBOARD = "當前排行榜：\n{leaderboard}"

# 排行榜相關
LEADERBOARD_HEADER = "🏆 排行榜"
FINAL_LEADERBOARD_HEADER = "🏁 本回合最終排行榜"
LEADERBOARD_ENTRY = "{rank}. {username} — {score}"

# 計分說明
SCORING_RULES = (
    "計分：首位答對 +{first}，後續答對 +{later}；"
    "若合規但算不到目標，扣 {penalty} 分。不合規不扣分。\n"
    "同一組（如 ABD）只有最先者得分；你可嘗試不同直線三點組合。\n\n"
    "指令：/new 出題、/end 結束本題、/endgame 結算賽局。"
)

# 用來存 Chat 的 schedule task，以便中途取消
chat_tasks: Dict[int, asyncio.Task] = {}
chat_reminder_tasks: Dict[int, asyncio.Task] = {}
current_valid_combos: Dict[int, Dict[str, str]] = {}
session_state: Dict[int, Dict[str, int]] = {}
solved_combo_keys: Dict[int, set] = {}

# ----------------- OOP: GameEngine -----------------
class GameEngine:
    def __init__(self) -> None:
        self.chat_to_valid_combos: Dict[int, Dict[str, str]] = {}
        self.chat_to_solved: Dict[int, set] = {}
        self.chat_tasks: Dict[int, asyncio.Task] = {}
        self.chat_reminder_tasks: Dict[int, asyncio.Task] = {}

    def set_valid_combos(self, chat_id: int, label_to_expr: Dict[str, str]) -> None:
        self.chat_to_valid_combos[chat_id] = label_to_expr
        self.chat_to_solved[chat_id] = set()

    def get_valid_combos(self, chat_id: int) -> Dict[str, str]:
        return self.chat_to_valid_combos.get(chat_id, {})

    def add_solved_combo(self, chat_id: int, combo_key: str) -> Tuple[int, int]:
        solved = self.chat_to_solved.setdefault(chat_id, set())
        solved.add(combo_key)
        return len(solved), len(self.get_valid_combos(chat_id))

    def all_solved(self, chat_id: int) -> bool:
        return len(self.chat_to_solved.get(chat_id, set())) >= len(self.get_valid_combos(chat_id)) > 0

ENGINE = GameEngine()

# 封裝：排程與結束
async def engine_schedule_end(chat_id:int, delay:int, context: ContextTypes.DEFAULT_TYPE):
    try:
        await asyncio.sleep(delay)
        game = await get_game(chat_id)
        if not game or not game["active"]:
            return
        await end_game_db(chat_id)
        sols = build_all_solutions_text(chat_id)
        rank_rows = await get_leaderboard(chat_id, limit=10)
        rank_text = "\n".join([LEADERBOARD_ENTRY.format(rank=i+1, username=u, score=s) for i,(u,s) in enumerate(rank_rows)]) if rank_rows else MSG_NO_SCORES
        msg = (
            f"{MSG_TIME_UP}\n"
            f"題目：數字 {game['numbers']}，目標 {game['target']}\n"
            f"{QUESTION_SOLUTIONS.format(solutions=sols)}\n\n"
            f"{QUESTION_LEADERBOARD.format(leaderboard=rank_text)}"
        )
        await context.bot.send_message(chat_id=chat_id, text=msg)
    except asyncio.CancelledError:
        return

def engine_set_timer(chat_id:int, context: ContextTypes.DEFAULT_TYPE):
    if chat_id in ENGINE.chat_tasks:
        ENGINE.chat_tasks[chat_id].cancel()
    task = asyncio.create_task(engine_schedule_end(chat_id, TIME_LIMIT_DEFAULT, context))
    ENGINE.chat_tasks[chat_id] = task
    if chat_id in ENGINE.chat_reminder_tasks:
        ENGINE.chat_reminder_tasks[chat_id].cancel()
    remind_delay = max(0, TIME_LIMIT_DEFAULT - 60)
    if remind_delay > 0:
        rtask = asyncio.create_task(schedule_reminder(chat_id, remind_delay, context))
        ENGINE.chat_reminder_tasks[chat_id] = rtask

async def engine_cancel_timers(chat_id:int):
    if chat_id in ENGINE.chat_tasks:
        ENGINE.chat_tasks[chat_id].cancel()
        del ENGINE.chat_tasks[chat_id]
    if chat_id in ENGINE.chat_reminder_tasks:
        ENGINE.chat_reminder_tasks[chat_id].cancel()
        del ENGINE.chat_reminder_tasks[chat_id]

class EngineFacade:
    @staticmethod
    async def prepare_round(chat_id:int, context: ContextTypes.DEFAULT_TYPE) -> Tuple[List[int], int, int]:
        # 產生題目
        numbers, target, solution = generate_solvable_puzzle(num_count=NUM_COUNT)
        expires_at = int(time.time()) + TIME_LIMIT_DEFAULT
        await clear_answers_for_chat(chat_id)
        await set_game(chat_id, numbers, target, expires_at, solution, int(REQUIRE_USE_ALL))
        # 可行解快取
        all_solutions: Dict[str, str] = {}
        for path in enumerate_adjacent_triplet_index_paths():
            vals = [float(numbers[i]) for i in path]
            exprs = [str(numbers[i]) for i in path]
            sol = find_solution_expr(vals, exprs, float(target))
            if sol:
                labels = ''.join(chr(ord('A') + i) for i in path)
                all_solutions[labels] = sol
        ENGINE.set_valid_combos(chat_id, all_solutions)
        solutions_count = len(all_solutions)
        # 發題
        bio = None
        try:
            bio = render_pyramid_image(numbers)
        except Exception:
            bio = None
        if bio:
            try:
                bio.name = "pyramid.png"
            except Exception:
                pass
            try:
                await context.bot.send_photo(
                    chat_id=chat_id,
                    photo=InputFile(bio, filename="pyramid.png"),
                    caption=MessageBuilder.caption_for_image(target, solutions_count)
                )
            except Exception:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=MessageBuilder.message_for_text(numbers, target, solutions_count)
                )
        else:
            await context.bot.send_message(
                chat_id=chat_id,
                text=MessageBuilder.message_for_text(numbers, target, solutions_count)
            )
        # 排程
        engine_set_timer(chat_id, context)
        return numbers, target, solutions_count

    @staticmethod
    async def finish_round_with_summary(chat_id:int, context: ContextTypes.DEFAULT_TYPE, prefix:str=MSG_TIME_UP) -> None:
        await end_game_db(chat_id)
        await engine_cancel_timers(chat_id)
        game = await get_game(chat_id)
        if not game:
            return
        sols = build_all_solutions_text(chat_id)
        rank_rows = await get_leaderboard(chat_id, limit=10)
        rank_text = "\n".join([LEADERBOARD_ENTRY.format(rank=i+1, username=u, score=s) for i,(u,s) in enumerate(rank_rows)]) if rank_rows else MSG_NO_SCORES
        msg = (
            f"{prefix}\n"
            f"題目：數字 {game['numbers']}，目標 {game['target']}\n"
            f"{QUESTION_SOLUTIONS.format(solutions=sols)}\n\n"
            f"{QUESTION_LEADERBOARD.format(leaderboard=rank_text)}"
        )
        await context.bot.send_message(chat_id=chat_id, text=msg)

    @staticmethod
    async def check_answer(chat_id:int, text:str, user, context: ContextTypes.DEFAULT_TYPE) -> None:
        game = await get_game(chat_id)
        if not game or not game["active"]:
            return
        numbers = game["numbers"]
        target = game["target"]
        m = re.fullmatch(r"\s*([A-Ja-j])\s*([A-Ja-j])\s*([A-Ja-j])\s*", text)
        if not m:
            return
        labels = [m.group(1).upper(), m.group(2).upper(), m.group(3).upper()]
        idxs = [ord(ch) - ord('A') for ch in labels]
        if tuple(sorted(idxs)) not in PRECOMPUTED_TRIPLETS_SORTED:
            await add_answer_record(chat_id, user.id, ''.join(sorted(labels)), 0)
            await context.bot.send_message(chat_id=chat_id, text=MSG_INVALID_ADJACENT.format(name=user.first_name))
            return
        cache = ENGINE.get_valid_combos(chat_id)
        key_label = ''.join(chr(ord('A') + i) for i in idxs)
        sol = cache.get(key_label)
        if sol is None:
            vals = [float(numbers[i]) for i in idxs]
            exprs = [str(numbers[i]) for i in idxs]
            sol = find_solution_expr(vals, exprs, float(target))
        if sol is None:
            await add_answer_record(chat_id, user.id, ''.join(sorted(labels)), 0)
            newscore = await add_score(chat_id, user.id, user.first_name, -WRONG_PENALTY)
            await context.bot.send_message(chat_id=chat_id, text=MSG_WRONG_ANSWER.format(name=user.first_name, penalty=WRONG_PENALTY, score=newscore))
            return
        combo_key = ''.join(sorted(labels))
        if await user_already_correct_combo(chat_id, user.id, combo_key):
            await add_answer_record(chat_id, user.id, combo_key, 1)
            await context.bot.send_message(chat_id=chat_id, text=MSG_ALREADY_SCORED.format(name=user.first_name))
            return
        first_solver = await get_combo_first_solver(chat_id, combo_key)
        if first_solver is not None and first_solver != user.id:
            await add_answer_record(chat_id, user.id, combo_key, 1)
            await context.bot.send_message(chat_id=chat_id, text=MSG_ALREADY_TAKEN)
            return
        correct_count = await count_correct_answers(chat_id)
        points = FIRST_CORRECT_POINTS if correct_count == 0 else LATER_CORRECT_POINTS
        newscore = await add_score(chat_id, user.id, user.first_name, points)
        await add_answer_record(chat_id, user.id, combo_key, 1)
        solved_count, total_needed = ENGINE.add_solved_combo(chat_id, combo_key)
        await context.bot.send_message(chat_id=chat_id, text=MSG_CORRECT_ANSWER.format(name=user.first_name, points=points, score=newscore, solved=solved_count, total=total_needed))
        if ENGINE.all_solved(chat_id):
            await EngineFacade.finish_round_with_summary(chat_id, context, prefix=MSG_ALL_SOLVED_EARLY)

class MessageBuilder:
    @staticmethod
    def help_text() -> str:
        return CMD_HELP_TEXT

    @staticmethod
    def caption_for_image(target:int, solutions_count:int) -> str:
        return f"🔢 目標 {target}｜可行解 {solutions_count} 組\n{QUESTION_INSTRUCTION}"

    @staticmethod
    def message_for_text(numbers:List[int], target:int, solutions_count:int) -> str:
        pyramid = build_pyramid_text(numbers)
        return f"{QUESTION_WITH_PYRAMID.format(target=target, pyramid=pyramid)}\n{QUESTION_INSTRUCTION}\n可行解：{solutions_count} 組"

    @staticmethod
    def newgame_intro(total_rounds:int) -> str:
        return (
            f"🎮 新賽局開始（共 {total_rounds} 題）\n"
            f"每題限時 {TIME_LIMIT_DEFAULT} 秒。\n\n"
            f"{QUESTION_INSTRUCTION}\n"
            f"規則：僅允許直線相鄰三點，四則運算可得目標即算答對。\n\n"
            f"{SCORING_RULES.format(first=FIRST_CORRECT_POINTS, later=LATER_CORRECT_POINTS, penalty=WRONG_PENALTY)}"
        )

def build_all_solutions_text(chat_id:int) -> str:
    combos = ENGINE.get_valid_combos(chat_id)
    if not combos:
        return "（無可用範例）"
    lines = []
    for label in sorted(combos.keys()):
        expr = combos[label]
        lines.append(f"[{label}] {expr}")
    return "\n".join(lines)

# 預先窮舉 10 個節點的所有「直線相鄰三點」組合（索引）
# 索引佈局：
# 0
# 1 2
# 3 4 5
# 6 7 8 9
PRECOMPUTED_TRIPLETS: List[Tuple[int,int,int]] = [
    # 水平（同層連續三個）
    (3, 4, 5),
    (6, 7, 8), (7, 8, 9),
    # 左斜（col 不變）
    (0, 1, 3), (1, 3, 6), (2, 4, 7),
    # 右斜（col +1）
    (0, 2, 5), (1, 4, 8), (2, 5, 9),
]
PRECOMPUTED_TRIPLETS_SORTED: set = {tuple(sorted(t)) for t in PRECOMPUTED_TRIPLETS}

# ----------------- DB helpers -----------------
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS scores(
            chat_id INTEGER,
            user_id INTEGER,
            username TEXT,
            score INTEGER,
            PRIMARY KEY(chat_id, user_id)
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS sessions(
            chat_id INTEGER PRIMARY KEY,
            total_rounds INTEGER,
            current_round INTEGER,
            active INTEGER
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS games(
            chat_id INTEGER PRIMARY KEY,
            numbers TEXT,
            target INTEGER,
            expires_at INTEGER,
            active INTEGER,
            solution TEXT,
            require_use_all INTEGER
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS answers(
            chat_id INTEGER,
            user_id INTEGER,
            expression TEXT,
            correct INTEGER,
            ts INTEGER
        )
        """)
        await db.commit()

async def set_game(chat_id:int, numbers:List[int], target:int, expires_at:int, solution:str, require_use_all:int=1):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        INSERT OR REPLACE INTO games(chat_id, numbers, target, expires_at, active, solution, require_use_all)
        VALUES (?, ?, ?, ?, 1, ?, ?)
        """, (chat_id, json.dumps(numbers), target, expires_at, solution, require_use_all))
        await db.commit()

async def get_game(chat_id:int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT numbers, target, expires_at, active, solution, require_use_all FROM games WHERE chat_id=?", (chat_id,))
        row = await cur.fetchone()
        if not row: return None
        nums = json.loads(row[0])
        return {
            "numbers": nums,
            "target": row[1],
            "expires_at": row[2],
            "active": bool(row[3]),
            "solution": row[4],
            "require_use_all": bool(row[5])
        }

async def end_game_db(chat_id:int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE games SET active=0 WHERE chat_id=?", (chat_id,))
        await db.commit()

async def add_answer_record(chat_id:int, user_id:int, expression:str, correct:int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO answers(chat_id, user_id, expression, correct, ts) VALUES (?, ?, ?, ?, ?)",
                         (chat_id, user_id, expression, correct, int(time.time())))
        await db.commit()

async def user_already_correct(chat_id:int, user_id:int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT 1 FROM answers WHERE chat_id=? AND user_id=? AND correct=1 LIMIT 1", (chat_id, user_id))
        return await cur.fetchone() is not None

async def count_correct_answers(chat_id:int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM answers WHERE chat_id=? AND correct=1", (chat_id,))
        row = await cur.fetchone()
        return row[0] if row else 0

async def clear_answers_for_chat(chat_id:int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM answers WHERE chat_id=?", (chat_id,))
        await db.commit()

async def get_combo_first_solver(chat_id:int, combo_key:str) -> Optional[int]:
    """回傳最早用該組合答對的 user_id，若無則 None。combo_key 需為排序後字母字串，例如 'GHI'。"""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT user_id FROM answers WHERE chat_id=? AND expression=? AND correct=1 ORDER BY ts ASC LIMIT 1",
            (chat_id, combo_key)
        )
        row = await cur.fetchone()
        return row[0] if row else None

async def user_already_correct_combo(chat_id:int, user_id:int, combo_key:str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT 1 FROM answers WHERE chat_id=? AND user_id=? AND expression=? AND correct=1 LIMIT 1",
            (chat_id, user_id, combo_key)
        )
        return await cur.fetchone() is not None

async def add_score(chat_id:int, user_id:int, username:str, delta:int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT score FROM scores WHERE chat_id=? AND user_id=?", (chat_id, user_id))
        row = await cur.fetchone()
        if row:
            new = row[0] + delta
            await db.execute("UPDATE scores SET score=? , username=? WHERE chat_id=? AND user_id=?", (new, username, chat_id, user_id))
        else:
            new = delta
            await db.execute("INSERT INTO scores(chat_id, user_id, username, score) VALUES (?, ?, ?, ?)", (chat_id, user_id, username, new))
        await db.commit()
        return new

async def get_user_score(chat_id:int, user_id:int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT score FROM scores WHERE chat_id=? AND user_id=?", (chat_id, user_id))
        row = await cur.fetchone()
        return row[0] if row else 0

async def get_leaderboard(chat_id:int, limit=10):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT username, score FROM scores WHERE chat_id=? ORDER BY score DESC LIMIT ?", (chat_id, limit))
        rows = await cur.fetchall()
        return rows

# ------------- Session helpers (multi-round) -------------
async def start_session(chat_id:int, total_rounds:int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO sessions(chat_id, total_rounds, current_round, active) VALUES (?, ?, ?, 1)", (chat_id, total_rounds, 0))
        await db.execute("DELETE FROM scores WHERE chat_id=?", (chat_id,))
        await db.commit()

async def get_session(chat_id:int) -> Optional[Dict[str,int]]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT total_rounds, current_round, active FROM sessions WHERE chat_id=?", (chat_id,))
        row = await cur.fetchone()
        if not row:
            return None
        return {"total_rounds": int(row[0]), "current_round": int(row[1]), "active": int(row[2])}

async def set_session_round(chat_id:int, current_round:int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE sessions SET current_round=? WHERE chat_id=?", (current_round, chat_id))
        await db.commit()

async def end_session(chat_id:int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE sessions SET active=0 WHERE chat_id=?", (chat_id,))
        await db.commit()

async def get_leaderboard_text(chat_id:int) -> str:
    rows = await get_leaderboard(chat_id, limit=10)
    if not rows:
        return MSG_NO_SCORES
    text = f"{FINAL_LEADERBOARD_HEADER}\n"
    for i, (username, score) in enumerate(rows, start=1):
        text += f"{LEADERBOARD_ENTRY.format(rank=i, username=username, score=score)}\n"
    return text

async def get_solved_combo_keys(chat_id:int) -> set:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT DISTINCT expression FROM answers WHERE chat_id=? AND correct=1",
            (chat_id,)
        )
        rows = await cur.fetchall()
        return {r[0] for r in rows}

async def maybe_end_if_all_solved(chat_id:int, context: ContextTypes.DEFAULT_TYPE):
    combos = current_valid_combos.get(chat_id)
    if not combos:
        return
    solved = await get_solved_combo_keys(chat_id)
    # keys in cache are like 'ABC' unsorted; normalize to sorted for comparison
    wanted = {''.join(sorted(k)) for k in combos.keys()}
    if wanted.issubset(solved):
        game = await get_game(chat_id)
        if not game or not game.get("active"):
            return
        await end_game_db(chat_id)
        if chat_id in chat_tasks:
            chat_tasks[chat_id].cancel()
            del chat_tasks[chat_id]
        if chat_id in chat_reminder_tasks:
            chat_reminder_tasks[chat_id].cancel()
            del chat_reminder_tasks[chat_id]
        await context.bot.send_message(
            chat_id=chat_id,
            text="✅ 本題所有可行組合皆已被答出，提前結束！"
        )

# ----------------- 安全解析與驗算 -----------------
# 只允許 + - * / 與 () 與 數字
ALLOWED_BINOPS = (ast.Add, ast.Sub, ast.Mult, ast.Div)
ALLOWED_UNARYOPS = (ast.UAdd, ast.USub)

def evaluate_and_collect_constants(expr: str) -> Tuple[float, Dict[int,int]]:
    """
    解析 expr，計算數值並回傳所使用的常數出現次數 (int -> count)
    若遇到不合法語法或節點，raise ValueError
    """
    node = ast.parse(expr, mode='eval')
    def _eval(n) -> Tuple[float, Dict[int,int]]:
        if isinstance(n, ast.Expression):
            return _eval(n.body)
        if isinstance(n, ast.Constant):  # Python 3.8+
            v = n.value
            if not isinstance(v, (int, float)):
                raise ValueError("僅允許數字常數")
            # 若是 float 但實際上是整數（例如 2.0），把它當作 int
            if isinstance(v, float) and abs(v - round(v)) < 1e-9:
                v = int(round(v))
            if not isinstance(v, int):
                # 我們只允許整數常數（避免 user 打出 0.5 等）
                raise ValueError("僅允許整數常數")
            return float(v), {int(v): 1}
        if isinstance(n, ast.BinOp):
            if not isinstance(n.op, ALLOWED_BINOPS):
                raise ValueError("不允許的運算子")
            lv, lcounts = _eval(n.left)
            rv, rcounts = _eval(n.right)
            if isinstance(n.op, ast.Add):
                val = lv + rv
            elif isinstance(n.op, ast.Sub):
                val = lv - rv
            elif isinstance(n.op, ast.Mult):
                val = lv * rv
            elif isinstance(n.op, ast.Div):
                if abs(rv) < 1e-12:
                    raise ValueError("除以零")
                val = lv / rv
            counts = {}
            for k,v in lcounts.items():
                counts[k] = counts.get(k,0) + v
            for k,v in rcounts.items():
                counts[k] = counts.get(k,0) + v
            return val, counts
        if isinstance(n, ast.UnaryOp):
            if not isinstance(n.op, ALLOWED_UNARYOPS):
                raise ValueError("不允許的unary op")
            v, counts = _eval(n.operand)
            if isinstance(n.op, ast.UAdd):
                return v, counts
            else:
                return -v, counts
        # 拒絕所有其他型別（函數呼叫、Name、Attribute 等）
        raise ValueError("不允許的語法或運算")
    return _eval(node)

# 檢查使用者的常數是否僅來自 allowed_numbers，且使用次數不超過
def validate_constants_usage(used_counts:Dict[int,int], allowed:List[int], require_use_all:bool=False) -> Tuple[bool,str]:
    """
    新規則：只能使用題目中的相鄰三個數字（依金字塔佈局的「相鄰」）。
    條件：
      - 僅允許剛好使用三個數字（可重複值，次數不得超過題目中該數字出現次數）
      - 這三個數字必須對應到金字塔中一條相鄰長度為 3 的路徑（索引相鄰）。
    """
    # 1) 是否只使用題目中的數字、且次數不超過各自提供數量
    allowed_map: Dict[int,int] = {}
    for n in allowed:
        allowed_map[n] = allowed_map.get(n,0) + 1
    for k, v in used_counts.items():
        if k not in allowed_map:
            return False, f"數字 {k} 非題目提供的數字"
        if v > allowed_map[k]:
            return False, f"數字 {k} 使用次數超過題目提供"
    used_total = sum(used_counts.values())
    if used_total != 3:
        return False, "本題僅允許使用相鄰的三個數字（請剛好使用 3 個數字）"

    # 2) 是否對得上某條相鄰長度 3 的索引路徑的值 multiset
    triplet_value_counts = compute_all_adjacent_triplet_value_counts(allowed)
    key = tuple(sorted([(k, v) for k, v in used_counts.items()]))
    if key not in triplet_value_counts:
        return False, "選用的三個數字在金字塔中必須彼此相鄰（連成一條長度 3 的路徑）"
    return True, ""

# ----------------- 求解器（可回傳一個解的式子） -----------------
def find_solution_expr(nums: List[float], exprs: List[str], target: float, tol=1e-6) -> Optional[str]:
    # nums: current numeric list
    # exprs: corresponding expression strings
    if len(nums) == 1:
        if abs(nums[0] - target) < tol:
            return exprs[0]
        return None
    n = len(nums)
    for i in range(n):
        for j in range(i+1, n):
            a, b = nums[i], nums[j]
            ea, eb = exprs[i], exprs[j]
            next_nums = [nums[k] for k in range(n) if k!=i and k!=j]
            next_exprs = [exprs[k] for k in range(n) if k!=i and k!=j]

            candidates = []
            candidates.append((a+b, f"({ea}+{eb})"))
            candidates.append((a*b, f"({ea}*{eb})"))
            candidates.append((a-b, f"({ea}-{eb})"))
            candidates.append((b-a, f"({eb}-{ea})"))
            if abs(b) > 1e-12:
                candidates.append((a/b, f"({ea}/{eb})"))
            if abs(a) > 1e-12:
                candidates.append((b/a, f"({eb}/{ea})"))

            for val, expr in candidates:
                res = find_solution_expr(next_nums + [val], next_exprs + [expr], target, tol)
                if res is not None:
                    return res
    return None

# 產生題目：隨機數字 -> 計算可達成的整數結果 -> 隨機挑選目標
def generate_solvable_puzzle(num_count:int=10, min_n=1, max_n=10, target_min=-50, target_max=200, tries=2000) -> Tuple[List[int], int, str]:
    """
    新規則：從 10 個數字的金字塔中，隨機挑選一組「相鄰長度為 3」的數字，
    找到一個整數目標值與對應解。
    """
    for _ in range(tries):
        nums = [random.randint(min_n, max_n) for _ in range(num_count)]
        triplets = enumerate_adjacent_triplet_index_paths()
        possible_targets = list(range(target_min, target_max+1))
        random.shuffle(possible_targets)
        # 預先枚舉所有直線三點對於每個目標的可行解
        all_solutions: Dict[str, str] = {}
        chosen_target: Optional[int] = None
        for t in possible_targets:
            found_any = False
            for path in triplets:
                vals = [float(nums[i]) for i in path]
                exprs = [str(nums[i]) for i in path]
                sol = find_solution_expr(vals, exprs, float(t))
                if sol:
                    labels = ''.join(chr(ord('A') + i) for i in path)
                    all_solutions[labels] = sol
                    found_any = True
            if found_any:
                chosen_target = int(t)
                break
        if chosen_target is not None and all_solutions:
            # 儲存本 chat 後面答題可直接查表（在 /new 裡設置）
            # 這裡只回傳其中一個示範（任選第一個）
            first_label, first_expr = next(iter(all_solutions.items()))
            sol_with_label = f"[{first_label}] {first_expr}"
            # 暫存在函式返回值；放入全域表會在 /new 中完成
            return nums, chosen_target, sol_with_label
    # fallback：找不到可行組合時回傳簡單目標
    nums = [random.randint(min_n, max_n) for _ in range(num_count)]
    target = int(sum(nums[:3]))
    sol = f"({nums[0]}+{nums[1]}+{nums[2]})"
    labels = 'ABC'
    sol_with_label = f"[{labels}] {sol}"
    return nums, target, sol_with_label

# ----------------- 圖像呈現：金字塔圈圈 -----------------
def render_pyramid_image(numbers: List[int]) -> Optional[io.BytesIO]:
    """
    嘗試用 Pillow 產生 1-2-3-4 金字塔的圈圈數字圖。
    若 Pillow 不可用，回傳 None。
    """
    try:
        from PIL import Image, ImageDraw, ImageFont
    except Exception:
        return None

    # 版面配置
    levels = [1, 2, 3, 4]
    assert sum(levels) == len(numbers)
    circle_diameter = 120
    circle_radius = circle_diameter // 2
    h_gap = 14  # 縮小水平間距
    v_gap = 14  # 縮小垂直間距
    margin = 32

    width = int(max(levels) * circle_diameter + (max(levels) - 1) * h_gap + margin * 2)
    height = int(len(levels) * circle_diameter + (len(levels) - 1) * v_gap + margin * 2)

    img = Image.new("RGB", (width, height), color=(255, 255, 255))
    draw = ImageDraw.Draw(img)

    def measure(draw_obj, text, font_obj):
        try:
            left, top, right, bottom = draw_obj.textbbox((0, 0), text, font=font_obj)
            return right - left, bottom - top
        except Exception:
            try:
                return font_obj.getsize(text)
            except Exception:
                # 最後退路
                return (len(text) * 10, 20)

    # 字型
    font = None
    for candidate in [
        "/System/Library/Fonts/SFNS.ttf",
        "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
        "/Library/Fonts/Arial.ttf",
    ]:
        try:
            font = ImageFont.truetype(candidate, size=48)
            break
        except Exception:
            continue
    if font is None:
        try:
            font = ImageFont.load_default()
        except Exception:
            font = None

    # 畫圈圈與數字
    idx = 0
    y = margin
    labels = [chr(ord('A') + i) for i in range(10)]
    for level_idx, level_count in enumerate(levels):
        row_width = level_count * circle_diameter + (level_count - 1) * h_gap
        x = (width - row_width) // 2
        for _ in range(level_count):
            cx = x + circle_radius
            cy = y + circle_radius
            bbox = [cx - circle_radius, cy - circle_radius, cx + circle_radius, cy + circle_radius]
            draw.ellipse(bbox, outline=(0, 0, 0), width=6)
            num_str = str(numbers[idx])
            tw, th = measure(draw, num_str, font)
            draw.text((cx - tw / 2, cy - th / 2 - 4), num_str, fill=(0, 0, 0), font=font)
            # 畫上 A-J 標籤（放大且更明顯，置於圓圈上方）
            label = labels[idx]
            try:
                small_font = ImageFont.truetype(font.path, size=36) if hasattr(font, 'path') else ImageFont.load_default()
            except Exception:
                small_font = ImageFont.load_default()
            ltw, lth = measure(draw, label, small_font)
            draw.text(
                (cx - ltw / 2, cy - circle_radius - lth),
                label,
                fill=(20, 20, 200),
                font=small_font,
                stroke_width=5,
                stroke_fill=(255, 255, 255),
            )
            x += circle_diameter + h_gap
            idx += 1
        y += circle_diameter + v_gap

    bio = io.BytesIO()
    img.save(bio, format="PNG")
    bio.seek(0)
    return bio

# ----------------- 相鄰三數：路徑與匹配 -----------------
def build_pyramid_text(numbers: List[int]) -> str:
    labels = [chr(ord('A') + i) for i in range(10)]
    rows = [
        [0],
        [1, 2],
        [3, 4, 5],
        [6, 7, 8, 9],
    ]
    raw_lines: List[str] = []
    for row in rows:
        parts = []
        for i in row:
            parts.append(f"{labels[i]}({numbers[i]})")
        raw_lines.append(" ".join(parts))
    max_len = max(len(s) for s in raw_lines)
    centered_lines = [
        (" " * ((max_len - len(s)) // 2)) + s
        for s in raw_lines
    ]
    return "\n".join(centered_lines)

def _pyramid_index_rows() -> List[List[int]]:
    # 固定 4 層：1,2,3,4 共 10 個索引
    return [
        [0],
        [1, 2],
        [3, 4, 5],
        [6, 7, 8, 9],
    ]

def enumerate_adjacent_triplet_index_paths() -> List[Tuple[int,int,int]]:
    # 直接回傳預先窮舉好的直線三點組合
    return PRECOMPUTED_TRIPLETS

def compute_all_adjacent_triplet_value_counts(numbers: List[int]) -> set:
    """
    將所有相鄰三數路徑的值做成 multiset key（排序後的 (value,count) 序列），回傳集合。
    用於快速比對使用者選的三數是否對應到任一合法相鄰路徑。
    """
    keys = set()
    for path in enumerate_adjacent_triplet_index_paths():
        vals = [numbers[i] for i in path]
        counts: Dict[int,int] = {}
        for v in vals:
            counts[v] = counts.get(v, 0) + 1
        key = tuple(sorted([(k, v) for k, v in counts.items()]))
        keys.add(key)
    return keys

# ----------------- Telegram Handlers -----------------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("哈囉，我是數字計算遊戲 bot！/new 出題，/score 看你的分數，/leaderboard 看排行榜，/help 了解更多。")

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(CMD_HELP_TEXT)

async def cmd_new(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    # 若賽局進行中，控制回合數
    sess = await get_session(chat_id)
    if sess and sess["active"]:
        # 若當前回合已達總回合則拒絕出新題
        if sess["current_round"] >= sess["total_rounds"]:
            await update.message.reply_text(MSG_SESSION_COMPLETE)
            return
    # 檢查是否已有進行中題目
    game = await get_game(chat_id)
    if game and game["active"]:
        await update.message.reply_text(MSG_GAME_IN_PROGRESS)
        return
    # 產生題目
    numbers, target, solution = generate_solvable_puzzle(num_count=NUM_COUNT)
    expires_at = int(time.time()) + TIME_LIMIT_DEFAULT
    # 清除上一題留下的作答紀錄，避免重複判斷讀到舊資料
    await clear_answers_for_chat(chat_id)
    await set_game(chat_id, numbers, target, expires_at, solution, int(REQUIRE_USE_ALL))
    # 若賽局進行中，回合 +1
    if sess and sess["active"]:
        await set_session_round(chat_id, sess["current_round"] + 1)

    # 預先計算此題所有可行的直線三點解，存入快取表
    all_solutions: Dict[str, str] = {}
    for path in enumerate_adjacent_triplet_index_paths():
        vals = [float(numbers[i]) for i in path]
        exprs = [str(numbers[i]) for i in path]
        sol = find_solution_expr(vals, exprs, float(target))
        if sol:
            labels = ''.join(chr(ord('A') + i) for i in path)
            all_solutions[labels] = sol
    current_valid_combos[chat_id] = all_solutions
    ENGINE.set_valid_combos(chat_id, all_solutions)
    solutions_count = len(all_solutions)
    # 初始化本題解答計數器集合
    solved_combo_keys[chat_id] = set()

    # 送出金字塔圈圈圖；若無 Pillow 則退回純文字
    bio = None
    try:
        bio = render_pyramid_image(numbers)
    except Exception:
        bio = None
    if bio:
        try:
            bio.name = "pyramid.png"
        except Exception:
            pass
        try:
            await context.bot.send_photo(
                chat_id=chat_id,
                photo=InputFile(bio, filename="pyramid.png"),
                caption=(
                    f"{QUESTION_PREFIX.format(target=target)}\n"
                    f"{QUESTION_INSTRUCTION}\n"
                    f"{QUESTION_RULE}\n"
                    f"{QUESTION_HINT.format(count=solutions_count)}"
                )
            )
        except Exception as e:
            # 若 photo 發送失敗，改用文件備援
            # 發送文字版金字塔備援
            text_pyr = build_pyramid_text(numbers)
            await update.message.reply_text(
                f"{QUESTION_WITH_PYRAMID.format(target=target, pyramid=text_pyr)}\n"
                f"{QUESTION_INSTRUCTION}\n"
                f"{QUESTION_RULE}\n"
                f"{QUESTION_HINT.format(count=solutions_count)}"
            )
    else:
        # 沒圖像，直接送文字金字塔
        text_pyr = build_pyramid_text(numbers)
        await update.message.reply_text(
            f"{QUESTION_WITH_PYRAMID.format(target=target, pyramid=text_pyr)}\n"
            f"{QUESTION_INSTRUCTION}\n"
            f"{QUESTION_RULE}\n"
            f"{QUESTION_HINT.format(count=solutions_count)}"
        )
    # 設定排程
    engine_set_timer(chat_id, context)

async def schedule_end(chat_id:int, delay:int, context: ContextTypes.DEFAULT_TYPE):
    try:
        await asyncio.sleep(delay)
        game = await get_game(chat_id)
        if not game or not game["active"]:
            return
        await end_game_db(chat_id)
        # reveal solutions
        sols = build_all_solutions_text(chat_id)
        rank_rows = await get_leaderboard(chat_id, limit=10)
        rank_text = "\n".join([LEADERBOARD_ENTRY.format(rank=i+1, username=u, score=s) for i,(u,s) in enumerate(rank_rows)]) if rank_rows else MSG_NO_SCORES
        msg = (
            f"{MSG_TIME_UP}\n"
            f"題目：數字 {game['numbers']}，目標 {game['target']}\n"
            f"{QUESTION_SOLUTIONS.format(solutions=sols)}\n\n"
            f"{QUESTION_LEADERBOARD.format(leaderboard=rank_text)}"
        )
        await context.bot.send_message(chat_id=chat_id, text=msg)
    except asyncio.CancelledError:
        # 被手動取消（例如 /end 或新題）
        return

async def cmd_end(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    game = await get_game(chat_id)
    if not game or not game["active"]:
        await update.message.reply_text(MSG_NO_ACTIVE_GAME)
        return
    await end_game_db(chat_id)
    await engine_cancel_timers(chat_id)
    sol = game.get("solution") or "（無可用範例）"
    await update.message.reply_text(f"{MSG_TERMINATED}\n題目：數字 {game['numbers']}，目標 {game['target']}\n示範解：`{sol}`")

async def cmd_newgame(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    args = context.args if hasattr(context, 'args') else []
    try:
        total = int(args[0]) if args else 5
        if total <= 0:
            raise ValueError()
    except Exception:
        await update.message.reply_text("用法：/newgame N （N 為題數，預設 5）")
        return
    await start_session(chat_id, total)
    await update.message.reply_text(MessageBuilder.newgame_intro(total))

async def cmd_endgame(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    sess = await get_session(chat_id)
    if not sess or not sess["active"]:
        await update.message.reply_text(MSG_NO_ACTIVE_SESSION)
        return
    # 確認本聊天室當前題目已結束
    game = await get_game(chat_id)
    if game and game["active"]:
        await end_game_db(chat_id)
        await engine_cancel_timers(chat_id)
        await update.message.reply_text(MSG_SYNC_ENDED)
    await end_session(chat_id)
    text = await get_leaderboard_text(chat_id)
    await update.message.reply_text(text)

async def schedule_reminder(chat_id:int, delay:int, context: ContextTypes.DEFAULT_TYPE):
    try:
        await asyncio.sleep(delay)
        game = await get_game(chat_id)
        if not game or not game["active"]:
            return
        await context.bot.send_message(chat_id=chat_id, text=MSG_ONE_MIN_REMAINING)
    except asyncio.CancelledError:
        return

async def cmd_score(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user = update.effective_user
    score = await get_user_score(chat_id, user.id)
    await update.message.reply_text(f"{user.first_name}，你在本群的分數：{score}")

async def cmd_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    rows = await get_leaderboard(chat_id, limit=10)
    if not rows:
        await update.message.reply_text(MSG_NO_SCORES)
        return
    text = f"{LEADERBOARD_HEADER}\n"
    for i, (username, score) in enumerate(rows, start=1):
        text += f"{LEADERBOARD_ENTRY.format(rank=i, username=username, score=score)}\n"
    await update.message.reply_text(text)

# 處理群組內的答案（非命令的文字）
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message is None or update.message.text is None:
        return
    chat_id = update.effective_chat.id
    text = update.message.text.strip()
    # 忽略命令
    if text.startswith("/"):
        return
    game = await get_game(chat_id)
    if not game or not game["active"]:
        return  # 沒題目就忽略
    numbers = game["numbers"]
    target = game["target"]
    
    # 新答題方式：接受 A-J 三個字母（不分大小寫），例如 ABC 或 aBd
    m = re.fullmatch(r"\s*([A-Ja-j])\s*([A-Ja-j])\s*([A-Ja-j])\s*", text)
    if not m:
        return  # 非預期格式就忽略
    labels = [m.group(1).upper(), m.group(2).upper(), m.group(3).upper()]
    # 轉換為索引
    idxs = [ord(ch) - ord('A') for ch in labels]
    # 驗證是否三個字母相鄰（存在於合法相鄰路徑中）
    if tuple(sorted(idxs)) not in PRECOMPUTED_TRIPLETS_SORTED:
        user = update.effective_user
        # 記錄錯誤（不合規則不扣分）
        await add_answer_record(chat_id, user.id, ''.join(sorted(labels)), 0)
        await update.message.reply_text(f"❌ {user.first_name}，答案不合規則：需為直線相鄰三點。")
        return
    await EngineFacade.check_answer(chat_id, text, update.effective_user, context)

# ----------------- main -----------------
def main():
    # 確保主執行緒有事件迴圈
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    # 初始化資料庫於同一事件迴圈
    loop.run_until_complete(init_db())
    if not BOT_TOKEN:
        print("請在環境變數 BOT_TOKEN 中設定你的 bot token")
        return
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("newgame", cmd_newgame))
    app.add_handler(CommandHandler("new", cmd_new))
    app.add_handler(CommandHandler("end", cmd_end))
    app.add_handler(CommandHandler("endgame", cmd_endgame))
    app.add_handler(CommandHandler("score", cmd_score))
    app.add_handler(CommandHandler("leaderboard", cmd_leaderboard))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), message_handler))
    print("bot start polling...")
    app.run_polling()

if __name__ == "__main__":
    main()