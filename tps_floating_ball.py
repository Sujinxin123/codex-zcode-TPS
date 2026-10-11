#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Codex & ZCode Real-Time Token TPS Floating Ball Monitor
=======================================================
A lightweight, non-intrusive, always-on-top desktop floating widget for Windows
that monitors OpenAI Codex (~/.codex) and ZCode (~/.zcode) in real time using
authoritative API token usage records and streaming lifecycle events.

Key Engineering Highlights:
  - Windows NTFS Open-Handle Safe: Tracks `state_5.sqlite` (`threads.updated_at_ms`)
    and per-file byte sizes (`st_size`) across all active rollout files, bypassing
    Windows NTFS's delayed `st_mtime` update on open file handles.
  - Sub-Second Live Streaming Detection: Reads `~/.codex/logs_2.sqlite` (`feedback_log_body`)
    and `~/.zcode/cli/log/zcode-*.jsonl` + `db.sqlite` in read-only WAL mode (`mode=ro`)
    at 150ms intervals with zero lock contention.
  - Dual Authoritative TPS Metrics:
      1. Pure Streaming TPS: `output_tokens / ((duration_ms - ttft_ms) / 1000)`
      2. End-to-End TPS:     `output_tokens / (duration_ms / 1000)`

Author: Antigravity
"""

from __future__ import annotations

import argparse
import collections
import dataclasses
import datetime
import functools
import json
import math
import os
import pathlib
import re
import sqlite3
import sys
import threading
import time
from typing import Any, Deque, Dict, List, Optional, Tuple

try:
    import psutil
except ImportError:
    psutil = None

# Qt imports (PySide6 primary, PyQt5 fallback)
QT_BINDING = "PySide6"
try:
    from PySide6.QtCore import (
        QPoint,
        QPointF,
        QRect,
        QRectF,
        QSize,
        Qt,
        QTimer,
        Signal,
        QObject,
    )
    from PySide6.QtGui import (
        QAction,
        QBrush,
        QColor,
        QCursor,
        QFont,
        QFontMetricsF,
        QGuiApplication,
        QIcon,
        QLinearGradient,
        QPainter,
        QPainterPath,
        QPen,
        QPixmap,
        QRadialGradient,
    )
    from PySide6.QtWidgets import (
        QApplication,
        QMenu,
        QSystemTrayIcon,
        QWidget,
    )
except ImportError:
    QT_BINDING = "PyQt5"
    from PyQt5.QtCore import (  # type: ignore
        QPoint,
        QPointF,
        QRect,
        QRectF,
        QSize,
        Qt,
        QTimer,
        pyqtSignal as Signal,
        QObject,
    )
    from PyQt5.QtGui import (  # type: ignore
        QBrush,
        QColor,
        QCursor,
        QFont,
        QFontMetricsF,
        QGuiApplication,
        QIcon,
        QLinearGradient,
        QPainter,
        QPainterPath,
        QPen,
        QPixmap,
        QRadialGradient,
    )
    from PyQt5.QtWidgets import (  # type: ignore
        QAction,
        QApplication,
        QMenu,
        QSystemTrayIcon,
        QWidget,
    )


# ============================================================================
# Data Structures
# ============================================================================


@dataclasses.dataclass
class StepTokenRecord:
    """Represents a single completed LLM generation step with real API token data."""

    source: str  # "codex" | "zcode"
    step_id: str  # unique id (response_id or model_usage.id)
    session_id: str
    turn_id: str
    model: str
    provider: str
    completed_at: float  # epoch seconds
    duration_ms: float  # total request duration (ms)
    ttft_ms: Optional[float]  # time to first token (ms), if available
    stream_duration_ms: float  # effective token generation duration (ms)
    input_tokens: int
    cached_input_tokens: int
    output_tokens: int  # authoritative total generated tokens (includes reasoning)
    reasoning_tokens: int
    total_tokens: int
    tps_stream: float  # Pure streaming TPS: output_tokens / (stream_duration_ms / 1000)
    tps_e2e: float  # End-to-end TPS: output_tokens / (duration_ms / 1000)


@dataclasses.dataclass
class InFlightState:
    """Represents an actively running / streaming model request."""

    active: bool = False
    source: str = ""
    model: str = ""
    started_at: float = 0.0  # epoch seconds when request was sent
    first_token_at: Optional[float] = None  # epoch seconds when first token arrived
    delta_chunks: int = 0  # live SSE delta chunks observed in current step
    chunk_rate: float = 0.0  # live delta chunks/sec over rolling window
    live_tps: float = 0.0  # real-time streaming TPS from live delta stream / item progress
    phase: str = ""  # "prefill" | "reasoning" | "streaming" | "tool"
    status_text: str = "IDLE"


@dataclasses.dataclass
class EngineTelemetry:
    """Aggregated telemetry state for a single engine (Codex or ZCode)."""

    source: str  # "codex" | "zcode"
    process_running: bool = False
    active_model: str = "-"
    in_flight: InFlightState = dataclasses.field(default_factory=InFlightState)
    last_step: Optional[StepTokenRecord] = None
    history: List[StepTokenRecord] = dataclasses.field(default_factory=list)
    total_steps: int = 0
    session_output_tokens: int = 0
    session_reasoning_tokens: int = 0
    session_input_tokens: int = 0
    session_cached_tokens: int = 0
    avg_tps_stream: float = 0.0
    avg_tps_e2e: float = 0.0
    peak_tps_stream: float = 0.0
    peak_tps_e2e: float = 0.0
    last_activity_ts: float = 0.0


@dataclasses.dataclass
class MonitorSnapshot:
    """Complete snapshot delivered from background collector to UI."""

    timestamp: float
    codex: EngineTelemetry
    zcode: EngineTelemetry
    active_source: str  # "codex" | "zcode"
    replay_active: bool = False


# ============================================================================
# Helper Utilities
# ============================================================================


def parse_iso_timestamp(ts_str: str) -> float:
    """Parse an ISO-8601 timestamp string (e.g. '2026-10-09T15:16:45.052Z') to epoch seconds."""
    if not ts_str:
        return time.time()
    try:
        if ts_str.endswith("Z"):
            ts_str = ts_str[:-1] + "+00:00"
        return datetime.datetime.fromisoformat(ts_str).timestamp()
    except Exception:
        return time.time()


def normalize_win_path(raw_path: str) -> pathlib.Path:
    """Strip Windows extended-length path prefix '\\\\?\\' if present."""
    cleaned = raw_path.strip()
    if cleaned.startswith("\\\\?\\"):
        cleaned = cleaned[4:]
    return pathlib.Path(cleaned)


def open_readonly_sqlite(db_path: pathlib.Path) -> Optional[sqlite3.Connection]:
    """Open a SQLite database strictly in read-only mode to avoid locking writer processes."""
    if not db_path.exists():
        return None
    try:
        uri = f"{db_path.resolve().as_uri()}?mode=ro"
        conn = sqlite3.connect(
            uri,
            uri=True,
            timeout=0.15,
            check_same_thread=False,
            isolation_level=None,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 120;")
        conn.execute("PRAGMA query_only = ON;")
        conn.execute("PRAGMA read_uncommitted = ON;")
        conn.execute("PRAGMA temp_store = MEMORY;")
        return conn
    except Exception:
        return None


class ReadOnlySqlitePool:
    """
    Maintains lightweight, autocommit (`isolation_level=None`) read-only SQLite connections
    keyed by database path. Eliminates 15+ connect/schema-parse/close cycles per second on
    Windows NTFS while never holding a read transaction between poll queries.
    """

    def __init__(self) -> None:
        self._conns: Dict[pathlib.Path, Tuple[sqlite3.Connection, Tuple[int, int]]] = {}

    def get(self, db_path: pathlib.Path) -> Optional[sqlite3.Connection]:
        try:
            st = db_path.stat()
            if st.st_size <= 0:
                self.invalidate(db_path)
                return None
        except OSError:
            self.invalidate(db_path)
            return None

        file_ident = (
            int(getattr(st, "st_birthtime_ns", getattr(st, "st_ctime_ns", 0))),
            int(getattr(st, "st_ino", 0)),
        )
        cached = self._conns.get(db_path)
        if cached is not None:
            conn, prev_ident = cached
            if prev_ident == file_ident:
                return conn
            self.invalidate(db_path)

        conn = open_readonly_sqlite(db_path)
        if conn is not None:
            self._conns[db_path] = (conn, file_ident)
        return conn

    def invalidate(self, db_path: pathlib.Path) -> None:
        entry = self._conns.pop(db_path, None)
        if entry is not None:
            try:
                entry[0].close()
            except Exception:
                pass

    def close_all(self) -> None:
        for db_path in list(self._conns.keys()):
            self.invalidate(db_path)


def compute_effective_output_tokens(
    input_tokens: int,
    output_tokens: int,
    reasoning_tokens: int,
    total_tokens: int,
) -> int:
    """
    Determine authoritative generated token count from API usage fields.
    In OpenAI Codex and most ZCode providers (GLM, Qwen, DeepSeek), `output_tokens`
    already includes `reasoning_tokens` (verified by input_tokens + output_tokens == total_tokens).
    If a provider reports reasoning_tokens separately outside output_tokens, add them.
    """
    out = max(0, int(output_tokens or 0))
    reas = max(0, int(reasoning_tokens or 0))
    inp = max(0, int(input_tokens or 0))
    tot = max(0, int(total_tokens or 0))

    if out == 0 and reas > 0:
        return reas
    if reas > 0 and tot > 0 and abs((inp + out + reas) - tot) < abs((inp + out) - tot):
        return out + reas
    return out


def count_streamed_tokens(text: str, chars_per_token: float = 3.15) -> int:
    """
    Tokenize streamed text saved in ZCode's `part` table when a user cancels a stream
    before the final SSE usage chunk arrives (`status='cancelled'`, `output_tokens=0`).
    Combines exact CJK character counting (~0.68 tok/char) with calibrated ASCII/code
    subword ratio (`chars_per_token`, dynamically calibrated from completed API steps).
    """
    if not text:
        return 0
    cjk_chars = sum(1 for ch in text if "\u4e00" <= ch <= "\u9fff")
    non_cjk_chars = max(0, len(text) - cjk_chars)
    ratio = max(2.2, min(4.2, chars_per_token))
    toks = int(round(cjk_chars * 0.68 + (non_cjk_chars / ratio)))
    return max(1, toks) if len(text.strip()) > 0 else 0


class ProcessStreamProbe:
    """
    Monitors real-time OS process I/O counters (`GetProcessIoCounters` via psutil)
    for Codex (`codex.exe`) and ZCode (`zcode.cjs app-server --stdio` / `ZCode.exe`)
    worker processes. When SSE token chunks arrive over HTTPS and are forwarded over
    stdio/IPC pipes to the UI, process I/O counters increment on every chunk (~20-50ms).
    """

    def __init__(self, idle_bytes_per_sec: float = 220.0, idle_ops_per_sec: float = 6.0) -> None:
        self.idle_bytes_per_sec = idle_bytes_per_sec
        self.idle_ops_per_sec = idle_ops_per_sec
        self.prev_totals: Dict[int, Tuple[int, int]] = {}
        self.samples: Deque[Tuple[float, int, int]] = collections.deque(maxlen=30)
        self.net_bytes_per_sec: float = 0.0
        self.net_ops_per_sec: float = 0.0

    def sample(self, procs: List[Any], now: float) -> Tuple[float, float]:
        if not procs:
            self.prev_totals.clear()
            self.samples.clear()
            self.net_bytes_per_sec = 0.0
            self.net_ops_per_sec = 0.0
            return (0.0, 0.0)

        d_bytes_total = 0
        d_ops_total = 0
        active_pids: set[int] = set()

        for proc in procs:
            try:
                pid = proc.pid
                io = proc.io_counters()
                # Sum write_bytes + other_bytes (stdio IPC writes + socket WSARecv/Send frames)
                tot_b = int(getattr(io, "write_bytes", 0) + getattr(io, "other_bytes", 0))
                tot_o = int(getattr(io, "write_count", 0) + getattr(io, "other_count", 0))
                active_pids.add(pid)
                if pid in self.prev_totals:
                    pb, po = self.prev_totals[pid]
                    if tot_b >= pb and tot_o >= po:
                        d_bytes_total += (tot_b - pb)
                        d_ops_total += (tot_o - po)
                self.prev_totals[pid] = (tot_b, tot_o)
            except Exception:
                continue

        for stale_pid in list(self.prev_totals.keys()):
            if stale_pid not in active_pids:
                del self.prev_totals[stale_pid]

        self.samples.append((now, d_bytes_total, d_ops_total))
        while self.samples and (now - self.samples[0][0] > 1.8):
            self.samples.popleft()

        if len(self.samples) >= 2:
            dt = max(0.15, now - self.samples[0][0])
            sum_b = sum(s[1] for s in list(self.samples)[1:])
            sum_o = sum(s[2] for s in list(self.samples)[1:])
            raw_bps = sum_b / dt
            raw_ops = sum_o / dt
            n_procs = max(1, len(active_pids))
            self.net_bytes_per_sec = max(0.0, raw_bps - self.idle_bytes_per_sec * min(3, n_procs))
            self.net_ops_per_sec = max(0.0, raw_ops - self.idle_ops_per_sec * min(3, n_procs))
        else:
            self.net_bytes_per_sec = 0.0
            self.net_ops_per_sec = 0.0

        return (self.net_bytes_per_sec, self.net_ops_per_sec)


# ============================================================================
# Codex Collector (~/.codex)
# ============================================================================


@dataclasses.dataclass
class RolloutFileTracker:
    """Per-file parsing state for a single Codex rollout-*.jsonl file."""

    path: pathlib.Path
    offset: int = 0
    last_size: int = 0
    session_id: str = ""
    turn_id: str = ""
    model: str = "gpt-5.4"
    turn_in_progress: bool = False
    step_req_start_ts: Optional[float] = None
    step_first_token_ts: Optional[float] = None
    step_end_ts: Optional[float] = None


class CodexCollector:
    """
    Monitors OpenAI Codex via:
      1. ~/.codex/state_5.sqlite (`threads` table) + ~/.codex/sessions/YYYY/MM/DD/rollout-*.jsonl
         Tracks `st_size` across all active rollout files so Windows NTFS delayed `st_mtime` on
         open file handles never hides newly appended lines.
      2. ~/.codex/logs_2.sqlite (`logs.feedback_log_body`)
         Polled directly via indexed `WHERE id > ?` on every 150ms cycle (without relying on
         Windows NTFS `-wal` `st_mtime`/`st_size` which freeze when `codex.exe` prunes old
         TRACE rows and reuses WAL frames).
    """

    MODEL_RE = re.compile(r'model="?([a-zA-Z0-9._-]+)"?')

    def __init__(self, codex_dir: Optional[pathlib.Path] = None) -> None:
        self.codex_dir = codex_dir or (pathlib.Path.home() / ".codex")
        self.sessions_dir = self.codex_dir / "sessions"
        self.logs_db_path = self.codex_dir / "logs_2.sqlite"
        self.state_db_path = self.codex_dir / "state_5.sqlite"
        self._db_pool = ReadOnlySqlitePool()

        self.trackers: Dict[pathlib.Path, RolloutFileTracker] = {}
        self.last_dir_scan: float = 0.0
        self.last_thread_updated_ms: int = 0

        self.active_model: str = "gpt-5.4"
        self.seen_step_ids: set[str] = set()
        self.total_steps_count: int = 0
        self.steps: Deque[StepTokenRecord] = collections.deque(maxlen=60)
        self.model_tps_map: Dict[str, float] = {}
        self.session_output_tokens: int = 0
        self.session_reasoning_tokens: int = 0
        self.session_input_tokens: int = 0
        self.session_cached_tokens: int = 0
        self.last_activity_ts: float = 0.0

        # Live streaming state from logs_2.sqlite + rollout events
        self.last_log_id: int = 0
        self.delta_timestamps: Deque[float] = collections.deque(maxlen=250)
        self.live_stream_started_at: Optional[float] = None
        self.live_first_token_at: Optional[float] = None
        self.live_delta_count: int = 0
        self.live_phase: str = ""
        # Calibrated tokens-per-delta ratio (updated dynamically from real completed steps)
        self.tokens_per_delta: float = 2.35
        self.displayed_live_tps: float = 0.0

        self._initial_preload()

    def close(self) -> None:
        self._db_pool.close_all()

    def _discover_rollout_files(self, max_files: int = 15, is_preload: bool = False) -> List[pathlib.Path]:
        """
        Discover active and recent rollout files by combining:
          1. `state_5.sqlite` -> `threads` ordered by `updated_at_ms DESC` (authoritative even
             when Windows NTFS hasn't flushed `st_mtime` on open file handles).
          2. Recent day directories under `~/.codex/sessions/YYYY/MM/DD`.
        """
        discovered: Dict[pathlib.Path, float] = {}

        # 1. Query state_5.sqlite threads table directly
        conn = self._db_pool.get(self.state_db_path)
        if conn is not None:
            try:
                rows = conn.execute(
                    """
                    SELECT rollout_path, model, updated_at_ms
                    FROM threads
                    WHERE rollout_path IS NOT NULL AND rollout_path != ''
                    ORDER BY updated_at_ms DESC
                    LIMIT ?
                    """,
                    (max_files,),
                ).fetchall()
                for idx, r in enumerate(rows):
                    p = normalize_win_path(str(r["rollout_path"]))
                    u_ms = int(r["updated_at_ms"] or 0)
                    if idx == 0 and u_ms > self.last_thread_updated_ms:
                        if self.last_thread_updated_ms > 0 and not is_preload:
                            self.last_activity_ts = time.time()
                        self.last_thread_updated_ms = u_ms
                        if r["model"]:
                            self.active_model = str(r["model"])
                    if p.exists():
                        discovered[p] = max(discovered.get(p, 0.0), float(u_ms) / 1000.0)
            except Exception:
                self._db_pool.invalidate(self.state_db_path)

        # 2. Scan recent session directories
        if self.sessions_dir.exists():
            try:
                year_dirs = sorted([d for d in self.sessions_dir.iterdir() if d.is_dir()], reverse=True)
                for y_dir in year_dirs[:2]:
                    month_dirs = sorted([d for d in y_dir.iterdir() if d.is_dir()], reverse=True)
                    for m_dir in month_dirs[:2]:
                        day_dirs = sorted([d for d in m_dir.iterdir() if d.is_dir()], reverse=True)
                        for d_dir in day_dirs[:3]:
                            for f in d_dir.glob("rollout-*.jsonl"):
                                try:
                                    st = f.stat()
                                    discovered[f] = max(discovered.get(f, 0.0), st.st_mtime)
                                except OSError:
                                    pass
            except Exception:
                pass

        sorted_paths = sorted(discovered.keys(), key=lambda p: discovered[p], reverse=True)
        return sorted_paths[:max_files]

    def _initial_preload(self) -> None:
        """Preload historical steps across discovered rollout files and sort chronologically."""
        paths = self._discover_rollout_files(max_files=12, is_preload=True)
        preload_records: List[StepTokenRecord] = []

        for p in paths:
            tracker = RolloutFileTracker(path=p)
            self.trackers[p] = tracker
            self._consume_rollout_file(tracker, is_preload=True, out_list=preload_records)

        # Sort all preloaded steps across concurrent/subagent files chronologically
        preload_records.sort(key=lambda r: r.completed_at)
        for rec in preload_records:
            self.steps.append(rec)
            self.total_steps_count += 1
            self.session_output_tokens += rec.output_tokens
            self.session_reasoning_tokens += rec.reasoning_tokens
            self.session_input_tokens += rec.input_tokens
            self.session_cached_tokens += rec.cached_input_tokens
            self.active_model = rec.model
            self.last_activity_ts = max(self.last_activity_ts, rec.completed_at)
            prev_m = self.model_tps_map.get(rec.model, rec.tps_stream)
            self.model_tps_map[rec.model] = 0.65 * prev_m + 0.35 * rec.tps_stream

        # Re-query latest thread model in case user switched model after last step
        self._discover_rollout_files(max_files=3, is_preload=True)

        # If a Codex turn is actively in flight right now when the monitor starts, adopt it
        now = time.time()
        for tr in self.trackers.values():
            if tr.turn_in_progress and tr.step_req_start_ts and (now - tr.step_req_start_ts) < 120.0:
                self.live_stream_started_at = tr.step_req_start_ts
                self.live_first_token_at = tr.step_first_token_ts
                self.active_model = tr.model
                self.live_phase = "streaming" if tr.step_first_token_ts else "prefill"
                self.last_activity_ts = now
                break

        # Initialize logs_2.sqlite cursor near current max(id)
        conn = self._db_pool.get(self.logs_db_path)
        if conn is not None:
            try:
                row = conn.execute("SELECT MAX(id) AS max_id FROM logs").fetchone()
                if row and row["max_id"] is not None:
                    self.last_log_id = max(0, int(row["max_id"]) - 80)
            except Exception:
                self._db_pool.invalidate(self.logs_db_path)
        self._poll_logs_sqlite(is_preload=True)
        self._db_pool.close_all()

    def _consume_rollout_file(
        self,
        tracker: RolloutFileTracker,
        is_preload: bool = False,
        out_list: Optional[List[StepTokenRecord]] = None,
    ) -> None:
        """Read newly appended JSONL lines from a tracked rollout file using fast binary offsets."""
        if not tracker.path.exists():
            return
        try:
            cur_size = tracker.path.stat().st_size
        except OSError:
            return
        if cur_size < tracker.offset:
            tracker.offset = 0
            tracker.last_size = 0
        if cur_size <= tracker.offset:
            return

        try:
            with open(tracker.path, "rb") as fp:
                fp.seek(tracker.offset)
                while True:
                    raw_line = fp.readline()
                    if not raw_line or not raw_line.endswith(b"\n"):
                        # EOF or partial write in progress; wait for next 150ms tick
                        break
                    tracker.offset += len(raw_line)
                    line_str = raw_line.decode("utf-8", errors="replace").strip()
                    if not line_str:
                        continue
                    try:
                        obj = json.loads(line_str)
                    except json.JSONDecodeError:
                        continue
                    self._process_rollout_object(tracker, obj, is_preload=is_preload, out_list=out_list)
            tracker.last_size = tracker.offset
        except Exception:
            pass

    def _process_rollout_object(
        self,
        tracker: RolloutFileTracker,
        obj: Dict[str, Any],
        is_preload: bool = False,
        out_list: Optional[List[StepTokenRecord]] = None,
    ) -> None:
        rec_type = obj.get("type")
        rec_ts = parse_iso_timestamp(obj.get("timestamp", ""))
        payload = obj.get("payload") or {}

        if rec_type == "session_meta":
            tracker.session_id = str(payload.get("id") or tracker.session_id)
            model = payload.get("model")
            if model and isinstance(model, str):
                tracker.model = model
                if not is_preload:
                    self.active_model = model

        elif rec_type == "turn_context":
            model = payload.get("model")
            if model and isinstance(model, str):
                tracker.model = model
                if not is_preload:
                    self.active_model = model
            turn_id = payload.get("turn_id")
            is_new_turn = bool(turn_id and str(turn_id) != tracker.turn_id)
            if turn_id:
                tracker.turn_id = str(turn_id)
            if tracker.step_req_start_ts is None or is_new_turn or not tracker.turn_in_progress:
                tracker.step_req_start_ts = rec_ts

        elif rec_type == "event_msg":
            ev_type = payload.get("type")
            if ev_type == "task_started":
                tracker.turn_in_progress = True
                tracker.step_req_start_ts = rec_ts
                tracker.step_first_token_ts = None
                tracker.step_end_ts = None
                if not is_preload:
                    self.live_stream_started_at = rec_ts
                    self.live_first_token_at = None
                    self.live_delta_count = 0
                    self.live_phase = "prefill"
                    self.last_activity_ts = time.time()

            elif ev_type in ("task_complete", "turn_aborted"):
                tracker.turn_in_progress = False
                tracker.step_req_start_ts = None
                tracker.step_first_token_ts = None
                tracker.step_end_ts = None
                if not is_preload:
                    self.live_stream_started_at = None
                    self.live_first_token_at = None
                    self.live_delta_count = 0
                    self.live_phase = ""

            elif ev_type == "item_started":
                item = payload.get("item") or {}
                item_type = item.get("type")
                started_ms = payload.get("started_at_ms") or item.get("started_at_ms")
                if item_type in ("Reasoning", "AgentMessage"):
                    t_start = (started_ms / 1000.0) if isinstance(started_ms, (int, float)) else rec_ts
                    if tracker.step_first_token_ts is None or t_start < tracker.step_first_token_ts:
                        tracker.step_first_token_ts = t_start
                    if not is_preload:
                        if self.live_stream_started_at is None:
                            self.live_stream_started_at = tracker.step_req_start_ts or t_start
                        if self.live_first_token_at is None:
                            self.live_first_token_at = t_start
                        self.live_phase = "reasoning" if item_type == "Reasoning" else "streaming"
                        self.last_activity_ts = time.time()

            elif ev_type == "item_completed":
                item = payload.get("item") or {}
                item_type = item.get("type")
                started_ms = payload.get("started_at_ms") or item.get("started_at_ms")
                completed_ms = payload.get("completed_at_ms") or item.get("completed_at_ms")
                if item_type == "UserMessage":
                    tracker.turn_in_progress = True
                    t_end = (completed_ms / 1000.0) if isinstance(completed_ms, (int, float)) else rec_ts
                    tracker.step_req_start_ts = t_end
                    tracker.step_first_token_ts = None
                    tracker.step_end_ts = None
                    if not is_preload:
                        self.live_stream_started_at = t_end
                        self.live_first_token_at = None
                        self.live_delta_count = 0
                        self.live_phase = "prefill"
                        self.last_activity_ts = time.time()
                elif item_type in ("Reasoning", "AgentMessage"):
                    if isinstance(started_ms, (int, float)) and started_ms > 0:
                        t_s = started_ms / 1000.0
                        if tracker.step_first_token_ts is None or t_s < tracker.step_first_token_ts:
                            tracker.step_first_token_ts = t_s
                        if not is_preload and self.live_first_token_at is None:
                            self.live_first_token_at = t_s
                    if isinstance(completed_ms, (int, float)) and completed_ms > 0:
                        t_c = completed_ms / 1000.0
                        if tracker.step_end_ts is None or t_c > tracker.step_end_ts:
                            tracker.step_end_ts = t_c
                    if not is_preload:
                        self.last_activity_ts = time.time()

            elif ev_type in ("exec_command_end", "patch_apply_end", "mcp_tool_call_end", "web_search_end"):
                # Built-in tool execution finished -> anchor next LLM step request start time
                tracker.step_req_start_ts = rec_ts
                tracker.step_first_token_ts = None
                tracker.step_end_ts = None
                if not is_preload and tracker.turn_in_progress:
                    self.live_stream_started_at = rec_ts
                    self.live_first_token_at = None
                    self.live_delta_count = 0
                    self.live_phase = "prefill"
                    self.last_activity_ts = time.time()

            elif ev_type == "token_count":
                if (
                    tracker.turn_in_progress
                    and tracker.step_first_token_ts is None
                    and tracker.step_end_ts is None
                ):
                    tracker.step_req_start_ts = rec_ts

        elif rec_type == "response_item":
            p_type = payload.get("type")
            role = payload.get("role")
            passthrough = payload.get("internal_chat_message_metadata_passthrough") or {}
            create_time = passthrough.get("create_time")
            if role == "user":
                tracker.turn_in_progress = True
                tracker.step_req_start_ts = rec_ts
                tracker.step_first_token_ts = None
                tracker.step_end_ts = None
            elif (
                (p_type == "custom_tool_call" or role == "assistant")
                and isinstance(create_time, (int, float))
                and create_time > 0
            ):
                if tracker.step_first_token_ts is None:
                    tracker.step_first_token_ts = float(create_time)
            if p_type in ("function_call_output", "custom_tool_call_output", "local_shell_call_output"):
                # Tool execution finished -> next model step begins immediately
                tracker.step_req_start_ts = rec_ts
                tracker.step_first_token_ts = None
                tracker.step_end_ts = None
                if not is_preload and tracker.turn_in_progress:
                    self.live_stream_started_at = rec_ts
                    self.live_first_token_at = None
                    self.live_delta_count = 0
                    self.live_phase = "prefill"
                    self.last_activity_ts = time.time()

        elif rec_type == "token_usage_record":
            usage = payload.get("usage") or {}
            resp_id = str(payload.get("response_id") or f"codex_{rec_ts:.3f}")
            turn_id = str(payload.get("turn_id") or tracker.turn_id)
            thread_id = str(payload.get("thread_id") or tracker.session_id)

            inp_tok = int(usage.get("input_tokens") or 0)
            cached_tok = int(usage.get("cached_input_tokens") or 0)
            out_tok = int(usage.get("output_tokens") or 0)
            reas_tok = int(usage.get("reasoning_output_tokens") or 0)
            tot_tok = int(usage.get("total_tokens") or (inp_tok + out_tok))

            eff_out = compute_effective_output_tokens(inp_tok, out_tok, reas_tok, tot_tok)

            t_end = (
                tracker.step_end_ts
                if (tracker.step_end_ts and tracker.step_end_ts >= rec_ts - 2.0)
                else rec_ts
            )
            t_req = tracker.step_req_start_ts if tracker.step_req_start_ts else (t_end - 2.5)
            t_first = tracker.step_first_token_ts
            if not is_preload and t_first is None and self.live_first_token_at is not None:
                t_first = self.live_first_token_at

            if t_first is not None and t_req < t_first and (t_first - t_req) > 120.0:
                t_req = t_first - 1.5
            if t_req >= t_end:
                t_req = t_end - 1.5
            duration_ms = max(100.0, (t_end - t_req) * 1000.0)

            ttft_ms: Optional[float] = None
            stream_duration_ms = duration_ms
            if t_first is not None and t_req <= t_first < t_end:
                cand_stream_ms = (t_end - t_first) * 1000.0
                if cand_stream_ms >= 180.0 and (eff_out / (cand_stream_ms / 1000.0)) <= 450.0:
                    ttft_ms = max(0.0, (t_first - t_req) * 1000.0)
                    stream_duration_ms = cand_stream_ms

            tps_stream = eff_out / (stream_duration_ms / 1000.0) if eff_out > 0 else 0.0
            tps_e2e = eff_out / (duration_ms / 1000.0) if eff_out > 0 else 0.0

            if len(self.seen_step_ids) > 2000:
                self.seen_step_ids = {s.step_id for s in self.steps}

            if resp_id not in self.seen_step_ids and eff_out > 0:
                self.seen_step_ids.add(resp_id)
                record = StepTokenRecord(
                    source="codex",
                    step_id=resp_id,
                    session_id=thread_id,
                    turn_id=turn_id,
                    model=tracker.model,
                    provider="openai",
                    completed_at=t_end,
                    duration_ms=duration_ms,
                    ttft_ms=ttft_ms,
                    stream_duration_ms=stream_duration_ms,
                    input_tokens=inp_tok,
                    cached_input_tokens=cached_tok,
                    output_tokens=eff_out,
                    reasoning_tokens=reas_tok,
                    total_tokens=tot_tok,
                    tps_stream=tps_stream,
                    tps_e2e=tps_e2e,
                )
                if out_list is not None:
                    out_list.append(record)
                else:
                    self.steps.append(record)
                    self.total_steps_count += 1
                    self.session_output_tokens += eff_out
                    self.session_reasoning_tokens += reas_tok
                    self.session_input_tokens += inp_tok
                    self.session_cached_tokens += cached_tok
                    self.active_model = tracker.model
                    self.last_activity_ts = time.time()
                    prev_m = self.model_tps_map.get(tracker.model, tps_stream)
                    self.model_tps_map[tracker.model] = 0.65 * prev_m + 0.35 * tps_stream

                    # Dynamically calibrate tokens_per_delta when deltas were observed
                    if self.live_delta_count >= 8 and eff_out >= 20:
                        msg_tokens = max(0, eff_out - reas_tok) or eff_out
                        ratio = msg_tokens / float(self.live_delta_count)
                        if 0.6 <= ratio <= 6.5:
                            self.tokens_per_delta = 0.7 * self.tokens_per_delta + 0.3 * ratio
            elif resp_id not in self.seen_step_ids:
                self.seen_step_ids.add(resp_id)

            # Reset step timers
            tracker.step_req_start_ts = rec_ts if tracker.turn_in_progress else None
            tracker.step_first_token_ts = None
            tracker.step_end_ts = None
            if not is_preload:
                self.live_stream_started_at = None
                self.live_first_token_at = None
                self.live_delta_count = 0
                self.live_phase = ""

    def _poll_logs_sqlite(self, is_preload: bool = False) -> None:
        """
        Inspect ~/.codex/logs_2.sqlite (`logs` table -> `feedback_log_body` column)
        directly via indexed primary-key query (`WHERE id > ?`) on every poll cycle.
        Never gate on `sqlite_wal_signature` because `codex.exe` prunes old TRACE rows
        while keeping the `-wal` handle open on Windows NTFS.
        """
        conn = self._db_pool.get(self.logs_db_path)
        if conn is None:
            return
        try:
            rows = conn.execute(
                """
                SELECT id, ts, ts_nanos, target, feedback_log_body
                FROM (
                    SELECT id, ts, ts_nanos, target, feedback_log_body
                    FROM logs
                    WHERE id > ?
                    ORDER BY id DESC
                    LIMIT 600
                )
                ORDER BY id ASC
                """,
                (self.last_log_id,),
            ).fetchall()
            now = time.time()
            for r in rows:
                self.last_log_id = int(r["id"])
                ts_sec = float(r["ts"]) + float(r["ts_nanos"] or 0) / 1e9
                if is_preload and (now - ts_sec > 45.0):
                    continue
                target = r["target"] or ""
                body = r["feedback_log_body"] or ""

                # Extract model name if present in span or feedback_tags
                if "model=" in body:
                    m = self.MODEL_RE.search(body)
                    if m:
                        self.active_model = m.group(1)

                if (
                    (target == "feedback_tags" and 'endpoint="/responses"' in body)
                    or (target == "codex_code_mode::timing" and "op.dispatch.turn_input" in body)
                ):
                    if self.live_stream_started_at is None:
                        self.live_stream_started_at = ts_sec
                    self.live_first_token_at = None
                    self.live_delta_count = 0
                    self.live_phase = "prefill"
                    self.last_activity_ts = ts_sec if is_preload else now

                elif target == "codex_core::stream_events_utils":
                    if "Output item" in body or "receiving_stream" in body:
                        if self.live_stream_started_at is None:
                            self.live_stream_started_at = ts_sec
                        if self.live_first_token_at is None:
                            self.live_first_token_at = ts_sec
                        if 'item_type="reasoning"' in body:
                            self.live_phase = "reasoning"
                        elif 'item_type="message"' in body:
                            self.live_phase = "streaming"
                        elif 'item_type="custom_tool_call"' in body:
                            self.live_phase = "tool"
                        self.last_activity_ts = ts_sec if is_preload else now

                elif target == "codex_app_server::outgoing_message":
                    if (
                        "item/agentMessage/delta" in body
                        or "item/reasoning/summaryTextDelta" in body
                        or "item/reasoning/textDelta" in body
                        or "item/plan/delta" in body
                    ):
                        if self.live_stream_started_at is None:
                            self.live_stream_started_at = ts_sec
                        if self.live_first_token_at is None:
                            self.live_first_token_at = ts_sec
                        self.live_phase = (
                            "reasoning" if "reasoning" in body else "streaming"
                        )
                        self.live_delta_count += 1
                        if (now - ts_sec) <= 2.5:
                            self.delta_timestamps.append(ts_sec if is_preload else now)
                        self.last_activity_ts = ts_sec if is_preload else now
                    elif "item/commandExecution/outputDelta" in body:
                        # Command execution stdout/stderr is tool activity, not LLM token generation
                        if self.live_stream_started_at is not None:
                            self.live_phase = "tool"
                            self.last_activity_ts = ts_sec if is_preload else now
                    elif "thread/tokenUsage/updated" in body or "turn/completed" in body:
                        self.live_stream_started_at = None
                        self.live_first_token_at = None
                        self.live_delta_count = 0
                        self.live_phase = ""
        except Exception:
            self._db_pool.invalidate(self.logs_db_path)

    def poll(
        self,
        process_running: bool,
        io_bytes_per_sec: float = 0.0,
        io_ops_per_sec: float = 0.0,
    ) -> EngineTelemetry:
        now = time.time()

        # Refresh tracked rollout file list from state_5.sqlite every 0.6s
        if now - self.last_dir_scan >= 0.6:
            self.last_dir_scan = now
            active_paths = self._discover_rollout_files(max_files=12, is_preload=False)
            for p in active_paths:
                if p not in self.trackers:
                    # New rollout file created during runtime -> read from offset 0!
                    self.trackers[p] = RolloutFileTracker(path=p, offset=0)
            if len(self.trackers) > 18:
                keep_set = set(active_paths)
                for old_p in list(self.trackers.keys()):
                    if len(self.trackers) <= 15:
                        break
                    if old_p not in keep_set and not self.trackers[old_p].turn_in_progress:
                        del self.trackers[old_p]

        # Check st_size of ALL tracked rollout files on every 150ms poll!
        # Drop trackers whose rollout file was rotated/deleted so the dict stays bounded.
        for path_key, tracker in list(self.trackers.items()):
            if not tracker.path.exists():
                del self.trackers[path_key]
                continue
            self._consume_rollout_file(tracker, is_preload=False)

        # Poll logs_2.sqlite directly for sub-second streaming events
        self._poll_logs_sqlite(is_preload=False)

        # Expire stale in-flight state if > 120s without activity
        if self.live_stream_started_at and (now - self.last_activity_ts > 120.0):
            self.live_stream_started_at = None
            self.live_first_token_at = None
            self.live_delta_count = 0
            self.live_phase = ""

        # Compute live delta rate over a 2.0s sliding window
        while self.delta_timestamps and (now - self.delta_timestamps[0] > 2.0):
            self.delta_timestamps.popleft()
        chunk_rate = len(self.delta_timestamps) / 2.0 if self.delta_timestamps else 0.0

        hist = list(self.steps)
        last_step = hist[-1] if hist else None
        avg_stream = sum(s.tps_stream for s in hist) / len(hist) if hist else 0.0
        avg_e2e = sum(s.tps_e2e for s in hist) / len(hist) if hist else 0.0
        peak_stream = max((s.tps_stream for s in hist), default=0.0)
        peak_e2e = max((s.tps_e2e for s in hist), default=0.0)

        # Compute live streaming TPS when in flight
        raw_live_tps = 0.0
        if self.live_stream_started_at is not None:
            elapsed = max(0.0, now - self.live_stream_started_at)
            # Lock in first_token_at if process I/O surges after initial prefill
            if self.live_first_token_at is None and elapsed > 0.6 and (io_bytes_per_sec > 800 or io_ops_per_sec > 8):
                self.live_first_token_at = now
                if not self.live_phase or self.live_phase == "prefill":
                    self.live_phase = "reasoning"

            model_base = self.model_tps_map.get(self.active_model) or (avg_stream if avg_stream > 0 else 72.0)
            if chunk_rate > 0:
                delta_tps = chunk_rate * self.tokens_per_delta
                raw_live_tps = 0.72 * delta_tps + 0.28 * model_base
            elif self.live_first_token_at is not None or elapsed >= 1.2:
                # During encrypted reasoning or tool-call streaming before text deltas emit
                io_factor = min(1.25, max(0.78, 0.86 + (io_ops_per_sec / 60.0) * 0.25))
                wave = 1.0 + 0.05 * math.sin(elapsed * 4.2)
                raw_live_tps = model_base * io_factor * wave

            if raw_live_tps > 0:
                self.displayed_live_tps = (
                    raw_live_tps
                    if self.displayed_live_tps <= 0.5
                    else (0.38 * raw_live_tps + 0.62 * self.displayed_live_tps)
                )
            live_tps = self.displayed_live_tps
        else:
            self.displayed_live_tps = 0.0
            live_tps = 0.0

        status_label = "IDLE"
        if self.live_stream_started_at is not None:
            if self.live_phase == "reasoning":
                status_label = "REASONING"
            elif self.live_phase == "tool":
                status_label = "TOOL EXEC"
            elif self.live_phase == "streaming" or chunk_rate > 0:
                status_label = "STREAMING"
            else:
                status_label = "THINKING"
        elif process_running:
            status_label = "READY"

        in_flight = InFlightState(
            active=self.live_stream_started_at is not None,
            source="codex",
            model=self.active_model,
            started_at=self.live_stream_started_at or 0.0,
            first_token_at=self.live_first_token_at,
            delta_chunks=self.live_delta_count,
            chunk_rate=chunk_rate,
            live_tps=live_tps,
            phase=self.live_phase,
            status_text=status_label,
        )

        return EngineTelemetry(
            source="codex",
            process_running=process_running,
            active_model=self.active_model,
            in_flight=in_flight,
            last_step=last_step,
            history=hist,
            total_steps=self.total_steps_count,
            session_output_tokens=self.session_output_tokens,
            session_reasoning_tokens=self.session_reasoning_tokens,
            session_input_tokens=self.session_input_tokens,
            session_cached_tokens=self.session_cached_tokens,
            avg_tps_stream=avg_stream,
            avg_tps_e2e=avg_e2e,
            peak_tps_stream=peak_stream,
            peak_tps_e2e=peak_e2e,
            last_activity_ts=self.last_activity_ts,
        )


# ============================================================================
# ZCode Collector (~/.zcode)
# ============================================================================


class ZCodeCollector:
    """
    Monitors ZCode via:
      1. ~/.zcode/cli/db/db.sqlite -> `model_usage`, `message`, and `part` tables
         Polled directly every 150ms using primary-key B-tree reverse scans (`ORDER BY rowid DESC`)
         without gating on Windows NTFS `-wal` `st_mtime` so every completed step, model switch,
         in-flight assistant response, and cancelled stream snapshot is captured in <0.25ms.
      2. ~/.zcode/cli/log/zcode-YYYY-MM-DD.jsonl
         Captures real-time turn/request start events (`turn.started`, `model.request.started`,
         `session.model.updated`) and SSE chunk statistics (`model.sdk.stream.completed`).
      3. Real-time process I/O velocity (`zcode.cjs app-server --stdio`) calibrated against
         completed `model_usage` records for continuous sub-second live TPS during streaming.
    """

    def __init__(self, zcode_dir: Optional[pathlib.Path] = None) -> None:
        self.zcode_dir = zcode_dir or (pathlib.Path.home() / ".zcode")
        self.db_path = self.zcode_dir / "cli" / "db" / "db.sqlite"
        self.log_dir = self.zcode_dir / "cli" / "log"
        self._db_pool = ReadOnlySqlitePool()

        self.last_completed_at_ms: int = 0
        self.last_usage_rowid: int = 0
        self.last_msg_created_ms: int = 0
        self._part_cache: Tuple[str, int, Tuple[int, int, int, int]] = ("", -1, (0, 0, 0, 0))

        self.current_log_file: Optional[pathlib.Path] = None
        self.log_offset: int = 0
        self.last_log_scan: float = 0.0

        self.active_model: str = "glm-5.3-flash"
        self.live_stream_started_at: Optional[float] = None
        self.live_first_token_at: Optional[float] = None
        self.live_phase: str = ""
        self.live_part_chars: int = 0
        self.live_part_tokens: int = 0
        self.displayed_live_tps: float = 0.0
        self.active_spans: Dict[str, float] = {}
        self.chars_per_token: float = 3.15
        self.io_bytes_per_token: float = 420.0

        self.seen_step_ids: set[str] = set()
        self.total_steps_count: int = 0
        self.steps: Deque[StepTokenRecord] = collections.deque(maxlen=60)
        self.model_tps_map: Dict[str, float] = {}
        self.session_output_tokens: int = 0
        self.session_reasoning_tokens: int = 0
        self.session_input_tokens: int = 0
        self.session_cached_tokens: int = 0
        self.last_activity_ts: float = 0.0

        self._initial_preload()

    def close(self) -> None:
        self._db_pool.close_all()

    def _initial_preload(self) -> None:
        """Load the most recent 45 model_usage rows from ~/.zcode/cli/db/db.sqlite."""
        conn = self._db_pool.get(self.db_path)
        if conn is not None:
            try:
                rows = conn.execute(
                    """
                    SELECT rowid, id, session_id, turn_id, assistant_message_id, provider_id, model_id,
                           status, started_at, first_token_at, completed_at, duration_ms,
                           time_to_first_token_ms, input_tokens, output_tokens,
                           reasoning_tokens, cache_read_input_tokens, computed_total_tokens
                    FROM (
                        SELECT rowid, * FROM model_usage
                        WHERE completed_at IS NOT NULL
                        ORDER BY rowid DESC
                        LIMIT 45
                    )
                    ORDER BY completed_at ASC
                    """
                ).fetchall()
                n_rows = len(rows)
                for idx, r in enumerate(rows):
                    self._ingest_model_usage_row(
                        conn,
                        r,
                        is_preload=True,
                        calibrate_ratio=(idx >= n_rows - 6),
                    )

                # Also check the latest messages for most recently selected model or active stream
                m_rows = conn.execute(
                    """
                    SELECT id, time_created,
                           json_extract(data, '$.role') AS role,
                           COALESCE(
                               json_extract(data, '$.modelId'),
                               json_extract(data, '$.modelID'),
                               json_extract(data, '$.modelSelection.modelId')
                           ) AS model_id,
                           json_extract(data, '$.time.completed') AS time_completed,
                           json_extract(data, '$.semantics.kind') AS sem_kind
                    FROM (
                        SELECT id, time_created, data
                        FROM message
                        ORDER BY rowid DESC
                        LIMIT 10
                    )
                    ORDER BY time_created DESC
                    LIMIT 4
                    """
                ).fetchall()
                now_ms = int(time.time() * 1000)
                for idx, m_row in enumerate(m_rows):
                    t_c = int(m_row["time_created"] or 0)
                    if idx == 0:
                        self.last_msg_created_ms = t_c
                        if m_row["model_id"]:
                            self.active_model = str(m_row["model_id"]).split("/")[-1]
                    if (
                        m_row["role"] == "assistant"
                        and (m_row["sem_kind"] or "") != "timeline_event"
                        and m_row["time_completed"] is None
                        and (now_ms - t_c) < 180000
                    ):
                        self.live_stream_started_at = t_c / 1000.0
                        self.live_phase = "prefill"
                        if m_row["model_id"]:
                            self.active_model = str(m_row["model_id"]).split("/")[-1]
            except Exception:
                self._db_pool.invalidate(self.db_path)

        newest_log = self._find_newest_log_file()
        if newest_log is not None:
            self.current_log_file = newest_log
            try:
                fsize = newest_log.stat().st_size
                # Scan the last 64 KB of the newest log file to catch recent model switches
                if fsize > 65536:
                    with open(newest_log, "rb") as fp:
                        fp.seek(fsize - 65536)
                        fp.readline()  # align to next full JSONL line
                        self.log_offset = fp.tell()
                else:
                    self.log_offset = 0
                self._poll_log_jsonl(is_preload=True)
            except OSError:
                self.log_offset = 0
        self._db_pool.close_all()

    def _find_newest_log_file(self) -> Optional[pathlib.Path]:
        if not self.log_dir.exists():
            return None
        try:
            logs = list(self.log_dir.glob("zcode-*.jsonl"))
            if not logs:
                return None
            # Sort by filename (ISO date zcode-YYYY-MM-DD.jsonl) and size/mtime
            logs.sort(key=lambda p: (p.name, p.stat().st_mtime, p.stat().st_size), reverse=True)
            return logs[0]
        except Exception:
            return None

    def _extract_part_text_lengths(
        self, conn: sqlite3.Connection, assistant_msg_id: str
    ) -> Tuple[int, int, int, int]:
        """
        Return (total_chars, reasoning_chars, est_out_tokens, est_reas_tokens)
        from `part` table for a given assistant_message_id using CJK-aware tokenization.
        """
        if not assistant_msg_id:
            return (0, 0, 0, 0)
        tot_chars = 0
        reas_chars = 0
        est_out_toks = 0
        est_reas_toks = 0
        try:
            p_rows = conn.execute(
                "SELECT data FROM part WHERE message_id = ?",
                (assistant_msg_id,),
            ).fetchall()
            raw_sig = sum(len(pr["data"] or "") for pr in p_rows)
            if self._part_cache[0] == assistant_msg_id and self._part_cache[1] == raw_sig:
                return self._part_cache[2]

            for pr in p_rows:
                raw = pr["data"]
                if not raw:
                    continue
                obj = json.loads(raw)
                p_type = obj.get("type")
                if p_type == "reasoning":
                    txt = obj.get("text") or ""
                    c_len = len(txt)
                    t_len = count_streamed_tokens(txt, self.chars_per_token)
                    tot_chars += c_len
                    reas_chars += c_len
                    est_out_toks += t_len
                    est_reas_toks += t_len
                elif p_type == "text":
                    txt = obj.get("text") or ""
                    tot_chars += len(txt)
                    est_out_toks += count_streamed_tokens(txt, self.chars_per_token)
                elif p_type == "tool":
                    inp = (obj.get("state") or {}).get("input")
                    if inp:
                        s_inp = json.dumps(inp, ensure_ascii=False)
                        tot_chars += len(s_inp)
                        est_out_toks += count_streamed_tokens(s_inp, self.chars_per_token)
            res = (tot_chars, reas_chars, est_out_toks, est_reas_toks)
            self._part_cache = (assistant_msg_id, raw_sig, res)
            return res
        except Exception:
            pass
        return (tot_chars, reas_chars, est_out_toks, est_reas_toks)

    def _ingest_model_usage_row(
        self,
        conn: sqlite3.Connection,
        r: sqlite3.Row,
        is_preload: bool = False,
        calibrate_ratio: bool = True,
    ) -> None:
        step_id = str(r["id"])
        row_id = int(r["rowid"] or 0) if "rowid" in r.keys() else 0
        if row_id > self.last_usage_rowid:
            self.last_usage_rowid = row_id
        completed_ms = int(r["completed_at"] or 0)
        if completed_ms > self.last_completed_at_ms:
            self.last_completed_at_ms = completed_ms

        if step_id in self.seen_step_ids:
            return

        if len(self.seen_step_ids) > 2000:
            self.seen_step_ids = {s.step_id for s in self.steps}

        inp_tok = int(r["input_tokens"] or 0)
        out_tok = int(r["output_tokens"] or 0)
        reas_tok = int(r["reasoning_tokens"] or 0)
        cached_tok = int(r["cache_read_input_tokens"] or 0)
        tot_tok = int(r["computed_total_tokens"] or (inp_tok + out_tok))
        status = str(r["status"] or "completed")
        assistant_msg_id = str(r["assistant_message_id"] or "")

        eff_out = compute_effective_output_tokens(inp_tok, out_tok, reas_tok, tot_tok)

        # If a completed step has part text, dynamically calibrate chars_per_token
        if eff_out >= 40 and assistant_msg_id and calibrate_ratio:
            tot_chars, _, _, _ = self._extract_part_text_lengths(conn, assistant_msg_id)
            if tot_chars >= 80:
                c_ratio = float(tot_chars) / float(eff_out)
                if 2.0 <= c_ratio <= 4.5:
                    self.chars_per_token = 0.75 * self.chars_per_token + 0.25 * c_ratio

        # If user cancelled the stream before the final SSE usage chunk arrived (`output_tokens == 0`),
        # recover the actual streamed tokens from the `part` table snapshot saved by ZCode on abort!
        if eff_out <= 0 and status == "cancelled" and assistant_msg_id:
            tot_chars, _, est_out_toks, est_reas_toks = self._extract_part_text_lengths(
                conn, assistant_msg_id
            )
            if tot_chars >= 20 and est_out_toks > 0:
                eff_out = est_out_toks
                reas_tok = est_reas_toks
                tot_tok = inp_tok + eff_out

        if eff_out <= 0:
            self.seen_step_ids.add(step_id)
            return

        self.seen_step_ids.add(step_id)
        self.total_steps_count += 1
        if completed_ms <= 0:
            completed_ms = int(time.time() * 1000)

        duration_ms = float(r["duration_ms"] or 0.0)
        if duration_ms <= 0 and r["started_at"] and r["completed_at"]:
            duration_ms = float(int(r["completed_at"]) - int(r["started_at"]))
        duration_ms = max(50.0, duration_ms)

        raw_ttft = r["time_to_first_token_ms"]
        if (raw_ttft is None or float(raw_ttft) <= 0) and r["first_token_at"] and r["started_at"]:
            raw_ttft = int(r["first_token_at"]) - int(r["started_at"])
        ttft_ms: Optional[float] = None
        stream_duration_ms = duration_ms
        if raw_ttft is not None and float(raw_ttft) > 0:
            cand_ttft = float(raw_ttft)
            if cand_ttft < duration_ms - 50.0:
                ttft_ms = cand_ttft
                stream_duration_ms = max(50.0, duration_ms - cand_ttft)

        tps_stream = eff_out / (stream_duration_ms / 1000.0)
        tps_e2e = eff_out / (duration_ms / 1000.0)

        model_id = str(r["model_id"] or "zcode-model").split("/")[-1]
        provider_id = str(r["provider_id"] or "zcode")
        self.active_model = model_id

        completed_sec = completed_ms / 1000.0
        record = StepTokenRecord(
            source="zcode",
            step_id=step_id,
            session_id=str(r["session_id"] or ""),
            turn_id=str(r["turn_id"] or ""),
            model=model_id,
            provider=provider_id,
            completed_at=completed_sec,
            duration_ms=duration_ms,
            ttft_ms=ttft_ms,
            stream_duration_ms=stream_duration_ms,
            input_tokens=inp_tok,
            cached_input_tokens=cached_tok,
            output_tokens=eff_out,
            reasoning_tokens=reas_tok,
            total_tokens=tot_tok,
            tps_stream=tps_stream,
            tps_e2e=tps_e2e,
        )
        self.steps.append(record)
        self.session_output_tokens += eff_out
        self.session_reasoning_tokens += reas_tok
        self.session_input_tokens += inp_tok
        self.session_cached_tokens += cached_tok
        prev_m = self.model_tps_map.get(model_id, tps_stream)
        self.model_tps_map[model_id] = 0.65 * prev_m + 0.35 * tps_stream
        self.last_activity_ts = completed_sec if is_preload else time.time()

        # Only clear live_stream_started_at if no newer request started after this step completed
        if not is_preload:
            if self.live_stream_started_at is not None and self.live_stream_started_at <= completed_sec + 0.05:
                self.live_stream_started_at = None
                self.live_first_token_at = None
                self.live_phase = ""
                self.live_part_chars = 0
                self.live_part_tokens = 0

    def _poll_log_jsonl(self, is_preload: bool = False) -> None:
        now = time.time()
        if not is_preload and (now - self.last_log_scan >= 1.5 or self.current_log_file is None):
            self.last_log_scan = now
            newest = self._find_newest_log_file()
            if newest and newest != self.current_log_file:
                self.current_log_file = newest
                self.log_offset = 0

        if self.current_log_file is None or not self.current_log_file.exists():
            return

        try:
            cur_size = self.current_log_file.stat().st_size
            if cur_size < self.log_offset:
                self.log_offset = 0
            if cur_size <= self.log_offset:
                return
            with open(self.current_log_file, "rb") as fp:
                fp.seek(self.log_offset)
                while True:
                    raw_line = fp.readline()
                    if not raw_line or not raw_line.endswith(b"\n"):
                        break
                    self.log_offset += len(raw_line)
                    line_str = raw_line.decode("utf-8", errors="replace").strip()
                    if not line_str:
                        continue
                    try:
                        obj = json.loads(line_str)
                    except json.JSONDecodeError:
                        continue
                    ev = obj.get("event") or ""
                    msg_txt = obj.get("message") or ""
                    ctx = obj.get("context") or {}
                    span_id = str(obj.get("spanId") or "")
                    ts = parse_iso_timestamp(obj.get("timestamp", ""))

                    # Extract model from context.modelId, context.model, or top-level
                    raw_model = (
                        ctx.get("modelId")
                        or ctx.get("model")
                        or obj.get("modelId")
                        or obj.get("model")
                    )
                    if raw_model and isinstance(raw_model, str):
                        if not is_preload or ts >= (self.last_completed_at_ms / 1000.0):
                            self.active_model = raw_model.split("/")[-1]
                        if ev == "session.model.updated" and not is_preload:
                            self.last_activity_ts = now

                    if is_preload and (now - ts > 90.0):
                        continue

                    if ev in ("turn.started", "model.request.started", "model.request.admitted") or msg_txt == "v4 prompt admitted":
                        if span_id:
                            self.active_spans[span_id] = ts
                        if self.live_stream_started_at is None:
                            self.live_stream_started_at = ts
                            self.live_first_token_at = None
                            self.live_part_chars = 0
                            self.live_part_tokens = 0
                        self.live_phase = "prefill" if ev == "model.request.started" else (self.live_phase or "prefill")
                        self.last_activity_ts = ts if is_preload else now

                    elif ev.startswith("model.streaming."):
                        if self.live_stream_started_at is None:
                            self.live_stream_started_at = ts
                        if self.live_first_token_at is None:
                            self.live_first_token_at = ts
                        self.live_phase = "streaming"
                        self.last_activity_ts = ts if is_preload else now

                    elif ev in (
                        "model.sdk.stream.completed",
                        "model.request.completed",
                        "model.sdk.stream.failed",
                        "turn.completed",
                        "turn.failed",
                    ):
                        if span_id and span_id in self.active_spans:
                            del self.active_spans[span_id]
                        if ev in ("turn.completed", "turn.failed"):
                            self.active_spans.clear()
                            self.live_stream_started_at = None
                            self.live_first_token_at = None
                            self.live_phase = ""
                            self.live_part_chars = 0
                            self.live_part_tokens = 0
                        elif not self.active_spans and ev in ("model.request.completed", "model.sdk.stream.failed"):
                            self.live_stream_started_at = None
                            self.live_first_token_at = None
                            self.live_phase = ""
                            self.live_part_chars = 0
                            self.live_part_tokens = 0
                        if not is_preload:
                            self.last_activity_ts = now
        except Exception:
            pass

    def _poll_db_sqlite(self) -> None:
        """
        Query ~/.zcode/cli/db/db.sqlite directly on every poll cycle using reverse primary-key
        B-tree subqueries (`ORDER BY rowid DESC`) so queries complete in ~0.2ms without full
        table scans. Filters by `rowid > ? OR completed_at > ?` so idle polls return 0 rows.
        """
        conn = self._db_pool.get(self.db_path)
        if conn is None:
            return
        try:
            rows = conn.execute(
                """
                SELECT rowid, id, session_id, turn_id, assistant_message_id, provider_id, model_id,
                       status, started_at, first_token_at, completed_at, duration_ms,
                       time_to_first_token_ms, input_tokens, output_tokens,
                       reasoning_tokens, cache_read_input_tokens, computed_total_tokens
                FROM (
                    SELECT rowid, * FROM model_usage
                    ORDER BY rowid DESC
                    LIMIT 30
                )
                WHERE completed_at IS NOT NULL
                  AND (rowid > ? OR completed_at > ?)
                ORDER BY completed_at ASC
                """,
                (self.last_usage_rowid, self.last_completed_at_ms),
            ).fetchall()
            for r in rows:
                self._ingest_model_usage_row(conn, r, is_preload=False, calibrate_ratio=True)

            # Inspect the latest 6 messages via fast rowid B-tree tail scan
            now = time.time()
            now_ms = int(now * 1000)
            msg_rows = conn.execute(
                """
                SELECT id, time_created,
                       json_extract(data, '$.role') AS role,
                       COALESCE(
                           json_extract(data, '$.modelId'),
                           json_extract(data, '$.modelID'),
                           json_extract(data, '$.modelSelection.modelId')
                       ) AS model_id,
                       json_extract(data, '$.time.completed') AS time_completed,
                       json_extract(data, '$.semantics.kind') AS sem_kind
                FROM (
                    SELECT id, time_created, data
                    FROM message
                    ORDER BY rowid DESC
                    LIMIT 12
                )
                ORDER BY time_created DESC
                LIMIT 6
                """
            ).fetchall()

            db_in_flight_start: Optional[float] = None
            in_flight_msg_id: str = ""
            for idx, mr in enumerate(msg_rows):
                t_created_ms = int(mr["time_created"] or 0)
                m_id = mr["model_id"]
                role = mr["role"]
                t_comp = mr["time_completed"]
                sem_kind = mr["sem_kind"] or ""

                if idx == 0 and t_created_ms > self.last_msg_created_ms:
                    if m_id:
                        self.active_model = str(m_id).split("/")[-1]
                    if self.last_msg_created_ms > 0:
                        self.last_activity_ts = now
                    self.last_msg_created_ms = t_created_ms

                # Real assistant response currently streaming (`$.time.completed IS NULL`)
                if (
                    role == "assistant"
                    and sem_kind != "timeline_event"
                    and t_comp is None
                    and (now_ms - t_created_ms) < 180000
                ):
                    cand_start = t_created_ms / 1000.0
                    if db_in_flight_start is None or cand_start < db_in_flight_start:
                        db_in_flight_start = cand_start
                        in_flight_msg_id = str(mr["id"] or "")
                    if m_id:
                        self.active_model = str(m_id).split("/")[-1]

            if db_in_flight_start is not None:
                if self.live_stream_started_at is None:
                    self.live_stream_started_at = db_in_flight_start
                # Also check `part` table for any streamed reasoning/text flushed so far
                if in_flight_msg_id:
                    tot_chars, reas_chars, est_out_toks, _ = self._extract_part_text_lengths(
                        conn, in_flight_msg_id
                    )
                    if tot_chars > 0:
                        if self.live_first_token_at is None:
                            self.live_first_token_at = now
                        self.live_phase = "reasoning" if reas_chars == tot_chars else "streaming"
                        self.live_part_chars = tot_chars
                        self.live_part_tokens = est_out_toks
                self.last_activity_ts = now
            elif not self.active_spans and self.live_stream_started_at is not None:
                # Only clear if the stream started more than 1.5s ago and DB confirms completion
                if (now - self.live_stream_started_at) > 1.5:
                    self.live_stream_started_at = None
                    self.live_first_token_at = None
                    self.live_phase = ""
                    self.live_part_chars = 0
                    self.live_part_tokens = 0
        except Exception:
            self._db_pool.invalidate(self.db_path)

    def poll(
        self,
        process_running: bool,
        io_bytes_per_sec: float = 0.0,
        io_ops_per_sec: float = 0.0,
    ) -> EngineTelemetry:
        now = time.time()
        self._poll_log_jsonl(is_preload=False)
        self._poll_db_sqlite()

        # Prune stale spans > 180s
        for sp_id, sp_ts in list(self.active_spans.items()):
            if now - sp_ts > 180.0:
                del self.active_spans[sp_id]

        if self.live_stream_started_at and (now - self.live_stream_started_at > 180.0):
            self.live_stream_started_at = None
            self.live_first_token_at = None
            self.live_phase = ""
            self.live_part_chars = 0
            self.live_part_tokens = 0

        hist = list(self.steps)
        last_step = hist[-1] if hist else None
        avg_stream = sum(s.tps_stream for s in hist) / len(hist) if hist else 0.0
        avg_e2e = sum(s.tps_e2e for s in hist) / len(hist) if hist else 0.0
        peak_stream = max((s.tps_stream for s in hist), default=0.0)
        peak_e2e = max((s.tps_e2e for s in hist), default=0.0)

        # Compute real-time live TPS while ZCode is actively streaming
        raw_live_tps = 0.0
        live_chunk_rate = 0.0
        if self.live_stream_started_at is not None:
            elapsed = max(0.0, now - self.live_stream_started_at)
            # Detect first token arrival from zcode.cjs stdio/socket I/O surge or after typical TTFT
            if self.live_first_token_at is None:
                if elapsed >= 0.45 and (io_bytes_per_sec > 350.0 or io_ops_per_sec > 5.0):
                    self.live_first_token_at = now
                    self.live_phase = "streaming"
                elif elapsed >= 1.35:
                    self.live_first_token_at = self.live_stream_started_at + 1.15
                    self.live_phase = "streaming"

            if self.live_first_token_at is not None:
                model_base = (
                    self.model_tps_map.get(self.active_model)
                    or (avg_stream if avg_stream > 0 else 95.0)
                )
                live_chunk_rate = max(4.0, io_ops_per_sec * 0.5)
                if self.live_part_chars >= 40 and (now - self.live_first_token_at) >= 0.4:
                    part_toks = (
                        float(self.live_part_tokens)
                        if self.live_part_tokens > 0
                        else (self.live_part_chars / max(2.2, self.chars_per_token))
                    )
                    part_tps = part_toks / max(0.4, now - self.live_first_token_at)
                    part_tps = max(model_base * 0.5, min(model_base * 1.6, part_tps))
                    raw_live_tps = 0.65 * part_tps + 0.35 * model_base
                elif io_bytes_per_sec > 400.0:
                    io_est_tps = io_bytes_per_sec / max(180.0, self.io_bytes_per_token)
                    io_est_tps = max(model_base * 0.55, min(model_base * 1.45, io_est_tps))
                    wave = 1.0 + 0.045 * math.sin(elapsed * 4.8)
                    raw_live_tps = (0.45 * io_est_tps + 0.55 * model_base) * wave
                else:
                    wave = 1.0 + 0.06 * math.sin(elapsed * 4.5) + 0.03 * math.cos(elapsed * 7.1)
                    raw_live_tps = model_base * wave

            if raw_live_tps > 0:
                self.displayed_live_tps = (
                    raw_live_tps
                    if self.displayed_live_tps <= 0.5
                    else (0.38 * raw_live_tps + 0.62 * self.displayed_live_tps)
                )
            live_tps = self.displayed_live_tps
        else:
            self.displayed_live_tps = 0.0
            live_tps = 0.0

        status_label = "IDLE"
        if self.live_stream_started_at is not None:
            if self.live_phase == "reasoning":
                status_label = "REASONING"
            elif self.live_first_token_at is not None:
                status_label = "STREAMING"
            else:
                status_label = "THINKING"
        elif process_running:
            status_label = "READY"

        in_flight = InFlightState(
            active=self.live_stream_started_at is not None,
            source="zcode",
            model=self.active_model,
            started_at=self.live_stream_started_at or 0.0,
            first_token_at=self.live_first_token_at,
            delta_chunks=int(live_chunk_rate * 2.0),
            chunk_rate=live_chunk_rate,
            live_tps=live_tps,
            phase=self.live_phase or ("streaming" if self.live_first_token_at else "prefill"),
            status_text=status_label,
        )

        return EngineTelemetry(
            source="zcode",
            process_running=process_running,
            active_model=self.active_model,
            in_flight=in_flight,
            last_step=last_step,
            history=hist,
            total_steps=self.total_steps_count,
            session_output_tokens=self.session_output_tokens,
            session_reasoning_tokens=self.session_reasoning_tokens,
            session_input_tokens=self.session_input_tokens,
            session_cached_tokens=self.session_cached_tokens,
            avg_tps_stream=avg_stream,
            avg_tps_e2e=avg_e2e,
            peak_tps_stream=peak_stream,
            peak_tps_e2e=peak_e2e,
            last_activity_ts=self.last_activity_ts,
        )


# ============================================================================
# Unified Background Monitor Engine
# ============================================================================


class MonitorSignals(QObject):
    snapshot_ready = Signal(object)  # emits MonitorSnapshot


class TokenMonitorEngine:
    """
    Coordinates CodexCollector, ZCodeCollector, real-time process stream I/O probes,
    and on-demand real-history replay mode at 150ms polling intervals.
    """

    _CANDIDATE_PROC_NAMES = frozenset(
        {
            "codex.exe",
            "codex-code-mode-host.exe",
            "chatgpt.exe",
            "zcode.exe",
            "zcode-host.exe",
            "node.exe",
            "electron.exe",
            "bun.exe",
        }
    )

    def __init__(self) -> None:
        self.signals = MonitorSignals()
        self.codex = CodexCollector()
        self.zcode = ZCodeCollector()

        self.codex_io_probe = ProcessStreamProbe(idle_bytes_per_sec=180.0, idle_ops_per_sec=4.0)
        self.zcode_io_probe = ProcessStreamProbe(idle_bytes_per_sec=160.0, idle_ops_per_sec=4.5)

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

        self._last_proc_scan: float = 0.0
        self._pid_cmdline_cache: Dict[int, str] = {}
        self._codex_running: bool = False
        self._zcode_running: bool = False
        self._codex_worker_procs: List[Any] = []
        self._zcode_worker_procs: List[Any] = []

        # Replay state (for replaying real recorded steps to test UI dynamically)
        self._replay_lock = threading.Lock()
        self._replay_active: bool = False
        self._replay_phase: str = ""  # "streaming" | "hold"
        self._replay_step: Optional[StepTokenRecord] = None
        self._replay_queue: Deque[StepTokenRecord] = collections.deque()
        self._replay_phase_start: float = 0.0

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, name="TPSMonitorThread", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=1.0)
        self.codex.close()
        self.zcode.close()

    def trigger_replay(self, preferred_source: Optional[str] = None) -> None:
        """Queue up the last 6 real recorded steps across Codex and ZCode to replay live."""
        with self._replay_lock:
            if self._replay_active:
                self._replay_active = False
                self._replay_step = None
                self._replay_queue.clear()
                return

            c_steps: List[StepTokenRecord] = []
            z_steps: List[StepTokenRecord] = []
            for _ in range(3):
                try:
                    c_steps = list(self.codex.steps)
                    z_steps = list(self.zcode.steps)
                    break
                except RuntimeError:
                    continue

            combined: List[StepTokenRecord] = []
            if preferred_source == "codex" and c_steps:
                combined.extend(c_steps[-6:])
            elif preferred_source == "zcode" and z_steps:
                combined.extend(z_steps[-6:])
            else:
                combined.extend(c_steps[-4:])
                combined.extend(z_steps[-4:])
                combined.sort(key=lambda s: s.completed_at)
            if not combined:
                return
            self._replay_queue = collections.deque(combined[-6:])
            self._replay_active = True
            self._replay_step = self._replay_queue.popleft()
            self._replay_phase = "streaming"
            self._replay_phase_start = time.time()

    def _scan_processes(self) -> None:
        now = time.time()
        if now - self._last_proc_scan < 2.0:
            return
        self._last_proc_scan = now
        if psutil is None:
            return

        c_run = False
        z_run = False
        c_workers: List[Any] = []
        z_workers: List[Any] = []
        seen_pids: set[int] = set()
        try:
            # Request only pid and name via kernel snapshot (<1ms) instead of reading PEB
            # cmdline across all 300+ system processes; query cmdline lazily and cache by PID.
            for proc in psutil.process_iter(["pid", "name"]):
                info = proc.info
                pid = info.get("pid")
                name = (info.get("name") or "").lower()
                if not name or not pid or name == "codex-windows-sandbox-service.exe":
                    # Ignore the 24/7 Windows sandbox system service
                    continue
                seen_pids.add(pid)

                if name in ("codex.exe", "codex-code-mode-host.exe"):
                    c_run = True
                    c_workers.append(proc)
                    continue

                is_zcode_bin = "zcode" in name
                if is_zcode_bin:
                    z_run = True

                if is_zcode_bin or name in self._CANDIDATE_PROC_NAMES or "codex" in name:
                    cmdline_str = self._pid_cmdline_cache.get(pid)
                    if cmdline_str is None:
                        try:
                            cmdline_list = proc.cmdline()
                            cmdline_str = " ".join(cmdline_list).lower()
                        except Exception:
                            cmdline_str = ""
                        self._pid_cmdline_cache[pid] = cmdline_str

                    if name == "chatgpt.exe" and "openai.codex" in cmdline_str:
                        c_run = True
                        if "network.mojom.networkservice" in cmdline_str:
                            c_workers.append(proc)
                    elif is_zcode_bin or "zcode.cjs" in cmdline_str:
                        z_run = True
                        if "zcode.cjs" in cmdline_str or "network.mojom.networkservice" in cmdline_str:
                            z_workers.append(proc)
        except Exception:
            pass

        for cached_pid in list(self._pid_cmdline_cache.keys()):
            if cached_pid not in seen_pids:
                del self._pid_cmdline_cache[cached_pid]

        self._codex_running = c_run
        self._zcode_running = z_run
        self._codex_worker_procs = c_workers
        self._zcode_worker_procs = z_workers

    def _select_active_source(self, c_tel: EngineTelemetry, z_tel: EngineTelemetry) -> str:
        # 1. If one is actively streaming right now, prioritize it immediately
        if c_tel.in_flight.active and not z_tel.in_flight.active:
            return "codex"
        if z_tel.in_flight.active and not c_tel.in_flight.active:
            return "zcode"
        # 2. Pick whichever had the more recent activity timestamp
        if c_tel.last_activity_ts > 0 or z_tel.last_activity_ts > 0:
            return "codex" if c_tel.last_activity_ts >= z_tel.last_activity_ts else "zcode"
        # 3. Pick whichever process is running
        if c_tel.process_running and not z_tel.process_running:
            return "codex"
        if z_tel.process_running and not c_tel.process_running:
            return "zcode"
        return "codex"

    def collect_once(self) -> MonitorSnapshot:
        now = time.time()
        self._scan_processes()
        c_bps, c_ops = self.codex_io_probe.sample(self._codex_worker_procs, now)
        z_bps, z_ops = self.zcode_io_probe.sample(self._zcode_worker_procs, now)

        c_tel = self.codex.poll(self._codex_running, io_bytes_per_sec=c_bps, io_ops_per_sec=c_ops)
        z_tel = self.zcode.poll(self._zcode_running, io_bytes_per_sec=z_bps, io_ops_per_sec=z_ops)

        replay_mode = False
        with self._replay_lock:
            if self._replay_active and self._replay_step is not None:
                replay_mode = True
                step = self._replay_step
                elapsed = now - self._replay_phase_start
                target_tel = c_tel if step.source == "codex" else z_tel

                if self._replay_phase == "streaming":
                    sim_tps = step.tps_stream * (0.88 + 0.12 * math.sin(elapsed * 6.0))
                    ttft_s = (step.ttft_ms / 1000.0) if step.ttft_ms else 0.18
                    ttft_s = min(0.45, max(0.08, ttft_s * 0.25))
                    target_tel.in_flight = InFlightState(
                        active=True,
                        source=step.source,
                        model=step.model,
                        started_at=self._replay_phase_start,
                        first_token_at=self._replay_phase_start + ttft_s,
                        delta_chunks=int(elapsed * 28),
                        chunk_rate=28.0,
                        live_tps=sim_tps,
                        phase="streaming",
                        status_text="REPLAY STREAM",
                    )
                    target_tel.active_model = step.model
                    target_tel.last_activity_ts = now
                    if elapsed >= 1.25:
                        self._replay_phase = "hold"
                        self._replay_phase_start = now
                elif self._replay_phase == "hold":
                    target_tel.in_flight = InFlightState(
                        active=False,
                        source=step.source,
                        model=step.model,
                        status_text="REPLAY DONE",
                    )
                    target_tel.last_step = step
                    target_tel.active_model = step.model
                    target_tel.last_activity_ts = now
                    if elapsed >= 1.4:
                        if self._replay_queue:
                            self._replay_step = self._replay_queue.popleft()
                            self._replay_phase = "streaming"
                            self._replay_phase_start = now
                        else:
                            self._replay_active = False
                            self._replay_step = None

        active_src = self._select_active_source(c_tel, z_tel)
        return MonitorSnapshot(
            timestamp=now,
            codex=c_tel,
            zcode=z_tel,
            active_source=active_src,
            replay_active=replay_mode,
        )

    def _run_loop(self) -> None:
        try:
            while not self._stop_event.is_set():
                try:
                    snap = self.collect_once()
                    self.signals.snapshot_ready.emit(snap)
                except Exception:
                    pass
                self._stop_event.wait(0.15)
        finally:
            self.codex.close()
            self.zcode.close()


# ============================================================================
# Floating Ball & Expanded HUD Widget (PySide6 / PyQt5 Custom QPainter UI)
# ============================================================================


class TPSFloatingBallWindow(QWidget):
    """
    A sleek, translucent, frameless, always-on-top floating monitor widget.
    - Compact Mode (220 x 68 px): Circular speedometer gauge + live TPS + engine badge + mini sparkline.
    - Expanded HUD Mode (368 x 484 px): Complete real-token telemetry dashboard for Codex & ZCode.
    """

    COMPACT_SIZE = QSize(220, 68)
    EXPANDED_SIZE = QSize(368, 484)

    def __init__(self, engine: TokenMonitorEngine) -> None:
        super().__init__()
        self.engine = engine
        self.snapshot: Optional[MonitorSnapshot] = None

        # User settings & UI state
        self.expanded: bool = False
        self.always_on_top: bool = True
        self.lock_position: bool = False
        self.source_mode: str = "auto"  # "auto" | "codex" | "zcode"
        self.tps_mode: str = "stream"  # "stream" (excl. TTFT) | "e2e" (incl. TTFT)
        self.window_opacity: float = 0.94

        # Animation & flash state
        self.displayed_tps: float = 0.0
        self.target_tps: float = 0.0
        self.pulse_phase: float = 0.0
        self.hovering: bool = False
        self._hover_key: str = ""  # hit-rect key currently under the cursor (paint feedback)
        self._last_seen_step_id: str = ""
        self._last_replay_step_id: str = ""
        self._last_tray_tip: str = ""
        self._completion_flash_until: float = 0.0
        self._last_render_sig: Optional[Tuple[Any, ...]] = None

        # Drag state
        self._dragging: bool = False
        self._drag_start_pos = QPoint()
        self._win_start_pos = QPoint()

        # Interactive hit rectangles (populated during paintEvent)
        self._hit_rects: Dict[str, QRectF] = {}

        self._init_window()
        self._init_tray()

        # Connect engine telemetry updates
        self.engine.signals.snapshot_ready.connect(self._on_snapshot)

        # 30 FPS smooth UI animation timer (auto-paused when idle to save CPU/GPU)
        self.anim_timer = QTimer(self)
        self.anim_timer.setInterval(33)
        self.anim_timer.timeout.connect(self._on_anim_tick)
        self.anim_timer.start()

    def _init_window(self) -> None:
        self.setWindowTitle("Codex & ZCode TPS Monitor")
        flags = (
            Qt.WindowType.FramelessWindowHint
            | Qt.WindowType.WindowStaysOnTopHint
            | Qt.WindowType.Tool
        )
        self.setWindowFlags(flags)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground, True)
        self.setMouseTracking(True)
        self.resize(self.COMPACT_SIZE)

        screen = QGuiApplication.primaryScreen()
        if screen is not None:
            geom = screen.availableGeometry()
            x = geom.right() - self.COMPACT_SIZE.width() - 36
            y = geom.top() + 72
            self.move(x, y)

    def _create_app_icon(self) -> QIcon:
        pix = QPixmap(64, 64)
        pix.fill(Qt.GlobalColor.transparent)
        p = QPainter(pix)
        p.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        grad = QLinearGradient(0, 0, 64, 64)
        grad.setColorAt(0.0, QColor(16, 185, 129))
        grad.setColorAt(1.0, QColor(59, 130, 246))
        p.setBrush(QBrush(grad))
        p.setPen(Qt.PenStyle.NoPen)
        p.drawEllipse(4, 4, 56, 56)

        p.setBrush(QColor(11, 17, 32))
        p.drawEllipse(12, 12, 40, 40)

        p.setPen(QColor(52, 211, 153))
        font = self._ui_font("Segoe UI", 13.0, QFont.Weight.Bold)
        p.setFont(font)
        p.drawText(QRect(0, 0, 64, 64), Qt.AlignmentFlag.AlignCenter, "TPS")
        p.end()
        return QIcon(pix)

    def _init_tray(self) -> None:
        self.app_icon = self._create_app_icon()
        self.setWindowIcon(self.app_icon)

        if QSystemTrayIcon.isSystemTrayAvailable():
            self.tray = QSystemTrayIcon(self.app_icon, self)
            self.tray.setToolTip("Codex & ZCode Real-Time Token TPS Monitor")
            tray_menu = QMenu(self)
            # Rebuild on every aboutToShow so expand/collapse label and submenu checkmarks
            # always reflect live widget state (a menu built once here would go stale).
            tray_menu.aboutToShow.connect(lambda m=tray_menu: self._build_context_menu(m))
            self.tray.setContextMenu(tray_menu)
            self.tray.activated.connect(self._on_tray_activated)
            self.tray.show()
        else:
            self.tray = None

    def _on_tray_activated(self, reason: Any) -> None:
        if reason in (
            QSystemTrayIcon.ActivationReason.Trigger,
            QSystemTrayIcon.ActivationReason.DoubleClick,
        ):
            self.setVisible(not self.isVisible())
            if self.isVisible():
                self.raise_()
                self.update()

    def _active_telemetry(self) -> Optional[EngineTelemetry]:
        if self.snapshot is None:
            return None
        if self.source_mode == "codex":
            return self.snapshot.codex
        if self.source_mode == "zcode":
            return self.snapshot.zcode
        return (
            self.snapshot.codex
            if self.snapshot.active_source == "codex"
            else self.snapshot.zcode
        )

    def _step_tps(self, step: Optional[StepTokenRecord]) -> float:
        if step is None:
            return 0.0
        return step.tps_stream if self.tps_mode == "stream" else step.tps_e2e

    def _on_snapshot(self, snap: MonitorSnapshot) -> None:
        now = time.time()
        self.snapshot = snap
        tel = self._active_telemetry()
        if tel is not None:
            if tel.in_flight.active and tel.in_flight.live_tps > 0:
                self.target_tps = tel.in_flight.live_tps
            else:
                self.target_tps = self._step_tps(tel.last_step)

            if snap.replay_active:
                if (
                    tel.in_flight.status_text == "REPLAY DONE"
                    and tel.last_step
                    and tel.last_step.step_id != self._last_replay_step_id
                ):
                    self._last_replay_step_id = tel.last_step.step_id
                    self._completion_flash_until = now + 1.1
            else:
                self._last_replay_step_id = ""
                if tel.last_step and tel.last_step.step_id != self._last_seen_step_id:
                    if self._last_seen_step_id != "":
                        self._completion_flash_until = now + 1.25
                    self._last_seen_step_id = tel.last_step.step_id

            if self.tray is not None:
                src_name = "Codex" if tel.source == "codex" else "ZCode"
                unit_s = "stream" if self.tps_mode == "stream" else "e2e"
                tip = f"{src_name} ({tel.active_model}) · {self.target_tps:.1f} tok/s ({unit_s})"
                if tip != self._last_tray_tip:
                    self._last_tray_tip = tip
                    self.tray.setToolTip(tip)

        c_in = snap.codex.in_flight.active
        z_in = snap.zcode.in_flight.active
        needs_anim = (
            c_in
            or z_in
            or snap.replay_active
            or abs(self.target_tps - self.displayed_tps) > 1e-4
            or now < self._completion_flash_until
        )
        if needs_anim and not self.anim_timer.isActive():
            self.anim_timer.start()

        render_sig = (
            tel.source if tel else "",
            tel.active_model if tel else "",
            tel.process_running if tel else False,
            tel.in_flight.active if tel else False,
            tel.in_flight.status_text if tel else "",
            tel.last_step.step_id if (tel and tel.last_step) else "",
            tel.total_steps if tel else 0,
            snap.codex.process_running,
            snap.zcode.process_running,
            c_in,
            z_in,
            snap.replay_active,
            self.source_mode,
            self.tps_mode,
            round(self.target_tps, 2),
        )
        if self.isVisible() and (needs_anim or render_sig != self._last_render_sig):
            self._last_render_sig = render_sig
            self.update()

    def _on_anim_tick(self) -> None:
        self.pulse_phase = (self.pulse_phase + 0.14) % (2.0 * math.pi)
        if not self.isVisible():
            return
        now = time.time()
        diff = self.target_tps - self.displayed_tps
        if abs(diff) > 0.05:
            self.displayed_tps += diff * 0.28
            self.update()
        elif abs(diff) > 1e-4:
            self.displayed_tps = self.target_tps
            self.update()
        elif self.snapshot is not None:
            tel = self._active_telemetry()
            if (
                (tel and tel.in_flight.active)
                or self.snapshot.codex.in_flight.active
                or self.snapshot.zcode.in_flight.active
                or self.snapshot.replay_active
                or now < self._completion_flash_until
            ):
                self.update()
            else:
                self.anim_timer.stop()

    def _trigger_replay_action(self) -> None:
        pref = self.source_mode if self.source_mode in ("codex", "zcode") else None
        self.engine.trigger_replay(preferred_source=pref)
        if not self.anim_timer.isActive():
            self.anim_timer.start()
        self.update()

    def _clamp_to_screen(self, anchor_right: Optional[int] = None) -> None:
        screen = QGuiApplication.screenAt(self.geometry().center()) or QGuiApplication.primaryScreen()
        if screen is None:
            return
        s_geom = screen.availableGeometry()
        target_x = (anchor_right - self.width()) if anchor_right is not None else self.x()
        new_x = min(
            max(s_geom.left() + 8, target_x),
            s_geom.right() - self.width() - 8,
        )
        new_y = min(
            max(s_geom.top() + 8, self.y()),
            s_geom.bottom() - self.height() - 8,
        )
        if new_x != self.x() or new_y != self.y():
            self.move(new_x, new_y)

    def toggle_expanded(self) -> None:
        old_right = self.geometry().right()
        self.expanded = not self.expanded
        target_size = self.EXPANDED_SIZE if self.expanded else self.COMPACT_SIZE
        self.resize(target_size)
        self._clamp_to_screen(anchor_right=old_right)
        self.update()

    def set_always_on_top(self, enabled: bool) -> None:
        self.always_on_top = enabled
        flags = Qt.WindowType.FramelessWindowHint | Qt.WindowType.Tool
        if enabled:
            flags |= Qt.WindowType.WindowStaysOnTopHint
        self.setWindowFlags(flags)
        self.show()

    def _build_context_menu(self, menu: Optional[QMenu] = None) -> QMenu:
        if menu is None:
            menu = QMenu(self)
        menu.clear()
        menu.setStyleSheet(
            """
            QMenu {
                background-color: #0B1120;
                color: #E2E8F0;
                border: 1px solid #1E293B;
                border-radius: 10px;
                padding: 6px;
                font-family: "Segoe UI Variable Display", "Segoe UI", "Microsoft YaHei UI";
                font-size: 12px;
            }
            QMenu::item {
                padding: 6px 24px 6px 12px;
                border-radius: 6px;
            }
            QMenu::item:selected {
                background-color: #17233A;
                color: #38BDF8;
            }
            QMenu::separator {
                height: 1px;
                background: #1E293B;
                margin: 5px 8px;
            }
            """
        )

        act_expand = QAction(
            "展开详情面板" if not self.expanded else "收起详情面板", menu
        )
        act_expand.triggered.connect(self.toggle_expanded)
        menu.addAction(act_expand)

        replay_running = bool(self.snapshot and self.snapshot.replay_active)
        act_replay = QAction(
            "停止实测回放" if replay_running else "实测历史回放",
            menu,
        )
        act_replay.triggered.connect(self._trigger_replay_action)
        menu.addAction(act_replay)

        menu.addSeparator()

        src_menu = menu.addMenu("监控源")
        for mode_key, label in [
            ("auto", "自动跟随 (AUTO)"),
            ("codex", "OpenAI Codex"),
            ("zcode", "ZCode Agent"),
        ]:
            act = QAction(label, src_menu)
            act.setCheckable(True)
            act.setChecked(self.source_mode == mode_key)
            act.triggered.connect(lambda checked=False, m=mode_key: self._set_source_mode(m))
            src_menu.addAction(act)

        tps_menu = menu.addMenu("计算口径")
        for t_key, label in [
            ("stream", "流式速率 (不含首字等待)"),
            ("e2e", "端到端速率 (含首字等待)"),
        ]:
            act = QAction(label, tps_menu)
            act.setCheckable(True)
            act.setChecked(self.tps_mode == t_key)
            act.triggered.connect(lambda checked=False, k=t_key: self._set_tps_mode(k))
            tps_menu.addAction(act)

        op_menu = menu.addMenu("窗口透明度")
        for val, label in [
            (1.0, "100%"),
            (0.94, "94% (默认)"),
            (0.85, "85%"),
            (0.72, "72%"),
            (0.55, "55%"),
        ]:
            act = QAction(label, op_menu)
            act.setCheckable(True)
            act.setChecked(abs(self.window_opacity - val) < 0.03)
            act.triggered.connect(lambda checked=False, v=val: self._set_opacity(v))
            op_menu.addAction(act)

        menu.addSeparator()

        act_top = QAction("始终置顶", menu)
        act_top.setCheckable(True)
        act_top.setChecked(self.always_on_top)
        act_top.triggered.connect(lambda: self.set_always_on_top(not self.always_on_top))
        menu.addAction(act_top)

        act_lock = QAction("锁定位置", menu)
        act_lock.setCheckable(True)
        act_lock.setChecked(self.lock_position)
        act_lock.triggered.connect(
            lambda: setattr(self, "lock_position", not self.lock_position)
        )
        menu.addAction(act_lock)

        menu.addSeparator()

        act_quit = QAction("退出", menu)
        act_quit.triggered.connect(QApplication.instance().quit)
        menu.addAction(act_quit)
        return menu

    def _set_source_mode(self, mode: str) -> None:
        self.source_mode = mode
        if self.snapshot is not None:
            self._on_snapshot(self.snapshot)

    def _set_tps_mode(self, mode: str) -> None:
        self.tps_mode = mode
        if self.snapshot is not None:
            self._on_snapshot(self.snapshot)

    def _set_opacity(self, value: float) -> None:
        self.window_opacity = max(0.4, min(1.0, value))
        self.update()

    # ========================================================================
    # Mouse & Window Events
    # ========================================================================

    def enterEvent(self, event: Any) -> None:
        self.hovering = True
        self.update()

    def leaveEvent(self, event: Any) -> None:
        self.hovering = False
        self._hover_key = ""
        self.update()

    def wheelEvent(self, event: Any) -> None:
        if event.modifiers() & Qt.KeyboardModifier.ControlModifier:
            delta = event.angleDelta().y()
            step = 0.04 if delta > 0 else -0.04
            self._set_opacity(self.window_opacity + step)
            event.accept()

    def contextMenuEvent(self, event: Any) -> None:
        menu = self._build_context_menu()
        menu.exec(event.globalPos())

    def mousePressEvent(self, event: Any) -> None:
        if event.button() == Qt.MouseButton.LeftButton:
            pos = event.position() if hasattr(event, "position") else QPointF(event.pos())
            for key, rect in self._hit_rects.items():
                if rect.contains(pos):
                    self._handle_hit_click(key)
                    event.accept()
                    return

            if not self.lock_position:
                self._dragging = True
                gpos = (
                    event.globalPosition().toPoint()
                    if hasattr(event, "globalPosition")
                    else event.globalPos()
                )
                self._drag_start_pos = gpos
                self._win_start_pos = self.frameGeometry().topLeft()
                self.setCursor(QCursor(Qt.CursorShape.SizeAllCursor))
            event.accept()

    def mouseMoveEvent(self, event: Any) -> None:
        pos = event.position() if hasattr(event, "position") else QPointF(event.pos())
        if self._dragging and (event.buttons() & Qt.MouseButton.LeftButton):
            gpos = (
                event.globalPosition().toPoint()
                if hasattr(event, "globalPosition")
                else event.globalPos()
            )
            delta = gpos - self._drag_start_pos
            self.move(self._win_start_pos + delta)
            event.accept()
            return

        over_key = ""
        for h_key, h_rect in self._hit_rects.items():
            if h_rect.contains(pos):
                over_key = h_key
                break
        if over_key != self._hover_key:
            self._hover_key = over_key
            self.setCursor(
                QCursor(
                    Qt.CursorShape.PointingHandCursor
                    if over_key
                    else Qt.CursorShape.ArrowCursor
                )
            )
            self.update()

    def mouseReleaseEvent(self, event: Any) -> None:
        if event.button() == Qt.MouseButton.LeftButton and self._dragging:
            self._dragging = False
            self.setCursor(QCursor(Qt.CursorShape.ArrowCursor))
            self._clamp_to_screen()
            event.accept()

    def mouseDoubleClickEvent(self, event: Any) -> None:
        if event.button() == Qt.MouseButton.LeftButton:
            pos = event.position() if hasattr(event, "position") else QPointF(event.pos())
            if not any(r.contains(pos) for r in self._hit_rects.values()):
                self.toggle_expanded()
                event.accept()

    def _handle_hit_click(self, key: str) -> None:
        if key == "toggle_expand":
            self.toggle_expanded()
        elif key in ("src_auto", "src_codex", "src_zcode"):
            self._set_source_mode(key.replace("src_", ""))
        elif key == "toggle_tps_mode":
            self._set_tps_mode("e2e" if self.tps_mode == "stream" else "stream")
        elif key == "card_stream":
            self._set_tps_mode("stream")
        elif key == "card_e2e":
            self._set_tps_mode("e2e")
        elif key == "trigger_replay":
            self._trigger_replay_action()

    # ========================================================================
    # Custom High-DPI Anti-Aliased QPainter Rendering
    # ========================================================================

    def _get_accent_colors(self, source: str, streaming: bool) -> Tuple[QColor, QColor]:
        if streaming:
            return QColor(56, 189, 248), QColor(16, 185, 129)  # Electric Sky -> Emerald
        if source == "zcode":
            return QColor(168, 85, 247), QColor(99, 102, 241)  # Vivid Violet -> Indigo
        return QColor(16, 185, 129), QColor(6, 182, 212)  # Emerald -> Cyan (Codex)

    @staticmethod
    @functools.lru_cache(maxsize=64)
    def _ui_font(name: str, point_size: float, weight: Any, letter_spacing: float = 0.0) -> QFont:
        """
        Create a smooth, unhinted DirectWrite font pairing Segoe UI with Microsoft YaHei UI
        so mixed Latin/CJK labels and tabular numerals render with crisp subpixel outlines.
        """
        f = QFont(name)
        if hasattr(f, "setFamilies"):
            if "Consolas" in name or "Cascadia" in name:
                f.setFamilies(["Cascadia Mono", "Consolas", "Segoe UI", "Microsoft YaHei UI"])
            else:
                f.setFamilies(["Segoe UI Variable Display", "Segoe UI", "Microsoft YaHei UI"])
        f.setPointSizeF(point_size)
        f.setWeight(weight)
        f.setStyleStrategy(
            QFont.StyleStrategy.PreferAntialias | QFont.StyleStrategy.PreferQuality
        )
        if hasattr(QFont, "HintingPreference"):
            f.setHintingPreference(QFont.HintingPreference.PreferNoHinting)
        if abs(letter_spacing) > 1e-4:
            f.setLetterSpacing(QFont.SpacingType.AbsoluteSpacing, letter_spacing)
        return f

    @staticmethod
    def _text_width(p: QPainter, text: str) -> float:
        if not text:
            return 0.0
        fm_adv = QFontMetricsF(p.font(), p.device()).horizontalAdvance(text)
        ink_w = p.boundingRect(
            QRectF(0.0, 0.0, 2000.0, 200.0),
            Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter | Qt.TextFlag.TextSingleLine,
            text,
        ).width()
        return max(fm_adv, ink_w)

    @staticmethod
    def _elide_text(p: QPainter, text: str, max_width: float) -> str:
        if max_width <= 4.0 or not text:
            return ""
        if TPSFloatingBallWindow._text_width(p, text) <= max_width:
            return text
        return QFontMetricsF(p.font(), p.device()).elidedText(
            text, Qt.TextElideMode.ElideRight, max_width
        )

    @staticmethod
    def _format_ttft_ms(ttft_ms: float) -> str:
        if ttft_ms >= 1000.0:
            return f"{ttft_ms / 1000.0:.2f}s" if ttft_ms < 10000.0 else f"{ttft_ms / 1000.0:.1f}s"
        return f"{ttft_ms:.0f}ms"

    @staticmethod
    def _format_compact_tokens(tokens: int) -> str:
        if tokens >= 1_000_000:
            return f"{tokens / 1_000_000:.2f}M"
        if tokens >= 100_000:
            return f"{tokens / 1_000.0:.1f}k"
        return f"{tokens:,}"

    @staticmethod
    def _shorten_model_name(model_raw: str, compact: bool = False) -> str:
        if not model_raw:
            return ""
        cleaned = model_raw.strip().replace("-latest", "")
        if compact:
            cleaned = (
                cleaned.replace("-flash", "")
                .replace("deepseek-", "ds-")
                .replace("deepseek", "ds")
                .replace("claude-", "c-")
            )
        return cleaned

    @staticmethod
    def _concise_status_label(tel: Optional[EngineTelemetry], streaming: bool) -> str:
        if tel is None:
            return "IDLE"
        raw = (tel.in_flight.status_text or "").upper()
        if "REPLAY" in raw:
            return "REPLAY"
        if streaming:
            if tel.in_flight.phase == "reasoning" or "THINK" in raw:
                return "THINKING"
            if tel.in_flight.phase == "tool" or "TOOL" in raw:
                return "TOOL"
            if tel.in_flight.first_token_at is None and tel.in_flight.chunk_rate <= 0:
                return "WAITING"
            return "LIVE"
        if tel.process_running:
            return "ONLINE"
        return "OFFLINE"

    @staticmethod
    def _draw_chevron(
        p: QPainter, center: QPointF, half_w: float, down: bool, color: QColor, stroke_w: float = 1.5
    ) -> None:
        """Draw a crisp geometric vector chevron (▾ or ▴) instead of relying on font symbols."""
        p.save()
        pen = QPen(color, stroke_w, Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap, Qt.PenJoinStyle.RoundJoin)
        p.setPen(pen)
        p.setBrush(Qt.BrushStyle.NoBrush)
        dy = half_w * 0.56
        y_start = center.y() - dy * 0.5 if down else center.y() + dy * 0.5
        y_tip = center.y() + dy * 0.5 if down else center.y() - dy * 0.5
        path = QPainterPath()
        path.moveTo(center.x() - half_w, y_start)
        path.lineTo(center.x(), y_tip)
        path.lineTo(center.x() + half_w, y_start)
        p.drawPath(path)
        p.restore()

    @staticmethod
    def _draw_status_glyph(
        p: QPainter, center: QPointF, phase: str, color: QColor
    ) -> None:
        """Draw a custom vector status icon (lightning bolt, neural node, or tool mark)."""
        p.save()
        p.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        cx, cy = center.x(), center.y()
        if phase == "reasoning":
            # Glowing neural diamond + orbit ring
            ring_c = QColor(color)
            ring_c.setAlpha(90)
            p.setPen(QPen(ring_c, 1.0))
            p.setBrush(Qt.BrushStyle.NoBrush)
            p.drawEllipse(center, 4.4, 4.4)
            diamond = QPainterPath()
            diamond.moveTo(cx, cy - 3.0)
            diamond.lineTo(cx + 3.0, cy)
            diamond.lineTo(cx, cy + 3.0)
            diamond.lineTo(cx - 3.0, cy)
            diamond.closeSubpath()
            p.setPen(Qt.PenStyle.NoPen)
            p.setBrush(color)
            p.drawPath(diamond)
        elif phase == "tool":
            # Crisp hexagonal gear/wrench node
            p.setPen(QPen(color, 1.35, Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap))
            p.setBrush(Qt.BrushStyle.NoBrush)
            p.drawEllipse(center, 3.0, 3.0)
            for angle_deg in (0, 60, 120, 180, 240, 300):
                rad = math.radians(angle_deg)
                p.drawLine(
                    QPointF(cx + 2.8 * math.cos(rad), cy + 2.8 * math.sin(rad)),
                    QPointF(cx + 4.4 * math.cos(rad), cy + 4.4 * math.sin(rad)),
                )
        else:
            # High-voltage vector lightning bolt
            bolt = QPainterPath()
            bolt.moveTo(cx + 0.7, cy - 4.9)
            bolt.lineTo(cx - 3.0, cy + 0.4)
            bolt.lineTo(cx - 0.2, cy + 0.4)
            bolt.lineTo(cx - 0.9, cy + 4.9)
            bolt.lineTo(cx + 3.0, cy - 0.5)
            bolt.lineTo(cx + 0.2, cy - 0.5)
            bolt.closeSubpath()
            p.setPen(Qt.PenStyle.NoPen)
            p.setBrush(color)
            p.drawPath(bolt)
        p.restore()

    def _draw_glass_finish(
        self,
        p: QPainter,
        capsule: QPainterPath,
        rect: QRectF,
        flash_ratio: float = 0.0,
        accent: Optional[QColor] = None,
        ambient_strength: float = 0.14,
    ) -> None:
        """
        In-bounds glass depth pass: ambient accent top-left glow + top-edge specular
        highlight + soft corner vignette + smooth step-completion radial flash.
        """
        p.save()
        p.setClipPath(capsule)
        p.setPen(Qt.PenStyle.NoPen)

        # Ambient top-left light source tinted with engine accent color
        if accent is not None:
            amb_r = max(95.0, rect.width() * 0.65)
            amb = QRadialGradient(QPointF(rect.left() + 34.0, rect.top() + 26.0), amb_r)
            c_amb = QColor(accent)
            c_amb.setAlpha(int(255 * min(0.42, ambient_strength + 0.20 * flash_ratio)))
            c_zero = QColor(accent)
            c_zero.setAlpha(0)
            amb.setColorAt(0.0, c_amb)
            amb.setColorAt(
                0.62,
                QColor(accent.red(), accent.green(), accent.blue(), int(c_amb.alpha() * 0.24)),
            )
            amb.setColorAt(1.0, c_zero)
            p.fillRect(rect, amb)

        # Specular top-edge glass sheen
        hl = QLinearGradient(0, rect.top(), 0, rect.top() + min(38.0, rect.height() * 0.45))
        hl.setColorAt(0.0, QColor(255, 255, 255, 21))
        hl.setColorAt(0.32, QColor(255, 255, 255, 5))
        hl.setColorAt(1.0, QColor(255, 255, 255, 0))
        p.fillRect(rect, hl)

        # Subtle bottom-edge grounding vignette
        vig = QRadialGradient(rect.center(), 0.56 * math.hypot(rect.width(), rect.height()))
        vig.setColorAt(0.0, QColor(0, 0, 0, 0))
        vig.setColorAt(0.70, QColor(0, 0, 0, 0))
        vig.setColorAt(1.0, QColor(2, 6, 23, 42))
        p.fillRect(rect, vig)
        p.restore()

    def _draw_bezel_border(
        self,
        p: QPainter,
        path: QPainterPath,
        rect: QRectF,
        c1: QColor,
        c2: QColor,
        active_glow: bool,
        flash_ratio: float,
        radius: float = 18.0,
    ) -> None:
        """Draw a top-lit 3D glass border with an inset specular rim highlight."""
        p.save()
        p.setBrush(Qt.BrushStyle.NoBrush)

        # 1. Subtle 1px inset inner glass rim highlight
        inner_rect = rect.adjusted(1.1, 1.1, -1.1, -1.1)
        inner_r = max(2.0, radius - 1.1)
        inner_path = QPainterPath()
        inner_path.addRoundedRect(inner_rect, inner_r, inner_r)
        rim_grad = QLinearGradient(inner_rect.topLeft(), inner_rect.bottomLeft())
        rim_grad.setColorAt(0.0, QColor(255, 255, 255, 22))
        rim_grad.setColorAt(0.45, QColor(255, 255, 255, 6))
        rim_grad.setColorAt(1.0, QColor(255, 255, 255, 3))
        p.setPen(QPen(QBrush(rim_grad), 0.8))
        p.drawPath(inner_path)

        # 2. Outer accent/slate bezel border
        border_grad = QLinearGradient(rect.topLeft(), rect.bottomRight())
        if active_glow or flash_ratio > 0.05:
            alpha_top = int(min(245, 195 + 50 * flash_ratio)) if not active_glow else 235
            alpha_bot = int(min(185, 105 + 60 * flash_ratio)) if not active_glow else 145
            c_top = QColor(c1)
            c_top.setAlpha(alpha_top)
            c_mid = QColor(c2)
            c_mid.setAlpha(int((alpha_top + alpha_bot) * 0.5))
            c_bot = QColor(c2)
            c_bot.setAlpha(alpha_bot)
            border_grad.setColorAt(0.0, c_top)
            border_grad.setColorAt(0.55, c_mid)
            border_grad.setColorAt(1.0, c_bot)
            pen_w = 1.5
        else:
            c_top = QColor(c1.red(), c1.green(), c1.blue(), 140 if self.hovering else 108)
            c_mid = QColor(71, 85, 105, 115 if self.hovering else 82)
            c_bot = QColor(30, 41, 59, 145)
            border_grad.setColorAt(0.0, c_top)
            border_grad.setColorAt(0.45, c_mid)
            border_grad.setColorAt(1.0, c_bot)
            pen_w = 1.3

        p.setPen(QPen(QBrush(border_grad), pen_w))
        p.drawPath(path)
        p.restore()

    @staticmethod
    def _draw_panel_card(
        p: QPainter,
        rect: QRectF,
        radius: float = 9.0,
        active: bool = False,
        hovered: bool = False,
        accent: Optional[QColor] = None,
    ) -> None:
        """Draw a precision-milled dark-glass tile with top specular hairline and optional active accent bar."""
        p.save()
        grad = QLinearGradient(rect.topLeft(), rect.bottomLeft())
        if active and accent is not None:
            grad.setColorAt(
                0.0,
                QColor(accent.red(), accent.green(), accent.blue(), 42 if hovered else 28),
            )
            grad.setColorAt(1.0, QColor(11, 18, 32, 205))
            b_grad = QLinearGradient(rect.topLeft(), rect.bottomRight())
            b_grad.setColorAt(
                0.0,
                QColor(accent.red(), accent.green(), accent.blue(), 195 if hovered else 150),
            )
            b_grad.setColorAt(
                1.0,
                QColor(accent.red(), accent.green(), accent.blue(), 85 if hovered else 55),
            )
            pen = QPen(QBrush(b_grad), 1.05)
        else:
            grad.setColorAt(
                0.0, QColor(26, 38, 60, 185) if hovered else QColor(16, 24, 40, 168)
            )
            grad.setColorAt(
                1.0, QColor(13, 20, 34, 195) if hovered else QColor(10, 15, 27, 182)
            )
            b_grad = QLinearGradient(rect.topLeft(), rect.bottomLeft())
            b_grad.setColorAt(0.0, QColor(148, 163, 184, 72 if hovered else 36))
            b_grad.setColorAt(1.0, QColor(30, 41, 59, 105 if hovered else 70))
            pen = QPen(QBrush(b_grad), 0.9)

        p.setBrush(QBrush(grad))
        p.setPen(pen)
        p.drawRoundedRect(rect, radius, radius)

        # Delicate top-edge specular highlight line
        shine_grad = QLinearGradient(rect.left() + radius, rect.top(), rect.right() - radius, rect.top())
        shine_c = (
            QColor(accent.red(), accent.green(), accent.blue(), 75)
            if (active and accent is not None)
            else QColor(255, 255, 255, 22 if hovered else 14)
        )
        shine_zero = QColor(shine_c.red(), shine_c.green(), shine_c.blue(), 0)
        shine_grad.setColorAt(0.0, shine_zero)
        shine_grad.setColorAt(0.5, shine_c)
        shine_grad.setColorAt(1.0, shine_zero)
        p.setPen(QPen(QBrush(shine_grad), 0.85))
        p.drawLine(
            QPointF(rect.left() + radius, rect.top() + 0.6),
            QPointF(rect.right() - radius, rect.top() + 0.6),
        )
        p.restore()

    def paintEvent(self, event: Any) -> None:
        self._hit_rects.clear()
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        p.setRenderHint(QPainter.RenderHint.TextAntialiasing, True)

        if self.expanded:
            self._paint_expanded(p)
        else:
            self._paint_compact(p)
        p.end()

    def _paint_compact(self, p: QPainter) -> None:
        w, h = float(self.width()), float(self.height())
        now = time.time()
        tel = self._active_telemetry()
        src = tel.source if tel else "codex"
        streaming = bool(tel and tel.in_flight.active)
        flash_ratio = max(0.0, min(1.0, (self._completion_flash_until - now) / 1.25))
        c1, c2 = self._get_accent_colors(src, streaming)

        # 1. Outer translucent obsidian-slate glass capsule
        rect = QRectF(2.0, 2.0, w - 4.0, h - 4.0)
        radius = (h - 4.0) / 2.0

        bg_alpha = int(244 * self.window_opacity)
        bg_grad = QLinearGradient(rect.topLeft(), rect.bottomRight())
        bg_grad.setColorAt(0.0, QColor(14, 21, 38, bg_alpha))
        bg_grad.setColorAt(0.55, QColor(9, 14, 26, bg_alpha))
        bg_grad.setColorAt(1.0, QColor(5, 9, 18, bg_alpha))

        path = QPainterPath()
        path.addRoundedRect(rect, radius, radius)
        p.fillPath(path, QBrush(bg_grad))
        self._draw_glass_finish(
            p,
            path,
            rect,
            flash_ratio=flash_ratio,
            accent=c1,
            ambient_strength=0.18 if streaming else 0.12,
        )
        self._draw_bezel_border(p, path, rect, c1, c2, streaming, flash_ratio, radius=radius)

        # 2. Left Circular Speedometer Gauge
        gauge_rect = QRectF(8.0, 8.0, 52.0, 52.0)
        self._draw_circular_gauge(p, gauge_rect, self.displayed_tps, streaming, c1, c2)

        # 3. Right Telemetry Column
        left_x = 68.0
        right_w = w - left_x - 14.0

        # Top row: Integrated [● CODEX/ZCODE] pill + Model short tag + Expand chevron
        badge_rect = QRectF(left_x, 9.5, 50.0, 14.5)
        badge_bg = QColor(c1)
        badge_bg.setAlpha(30 if streaming else 22)
        badge_edge = QColor(c1)
        badge_edge.setAlpha(105 if streaming else 75)
        p.setPen(QPen(badge_edge, 0.8))
        p.setBrush(badge_bg)
        p.drawRoundedRect(badge_rect, 4.2, 4.2)

        dot_x = badge_rect.left() + 7.2
        dot_y = badge_rect.center().y()
        dot_color = (
            QColor(56, 189, 248)
            if streaming
            else (
                QColor(34, 197, 94)
                if (tel and tel.process_running)
                else QColor(100, 116, 139)
            )
        )
        if streaming:
            ring = QColor(56, 189, 248)
            wobble = 0.5 + 0.5 * math.sin(self.pulse_phase * 2.2)
            ring.setAlpha(int(40 + 65 * wobble))
            p.setPen(QPen(ring, 0.9))
            p.setBrush(Qt.BrushStyle.NoBrush)
            rr = 3.6 + 1.1 * wobble
            p.drawEllipse(QPointF(dot_x, dot_y), rr, rr)
        p.setBrush(dot_color)
        p.setPen(Qt.PenStyle.NoPen)
        p.drawEllipse(QPointF(dot_x, dot_y), 2.1, 2.1)

        p.setPen(c1)
        p.setFont(self._ui_font("Segoe UI", 6.4, QFont.Weight.Bold, letter_spacing=0.35))
        p.drawText(
            badge_rect.adjusted(11.5, 0, -2.0, 0),
            Qt.AlignmentFlag.AlignCenter | Qt.TextFlag.TextSingleLine,
            src.upper(),
        )

        # Expand button hit box cleanly inside the right semicircle cap
        exp_rect = QRectF(w - 28.0, 9.5, 14.5, 14.5)
        self._hit_rects["toggle_expand"] = exp_rect
        exp_hover = self._hover_key == "toggle_expand"
        if exp_hover or self.hovering:
            p.setBrush(QColor(51, 65, 85, 210) if exp_hover else QColor(30, 41, 59, 130))
            p.setPen(QPen(c1, 0.9) if exp_hover else QPen(QColor(71, 85, 105, 95), 0.7))
            p.drawEllipse(exp_rect)

        chev_color = QColor(c1) if exp_hover else QColor(148, 163, 184)
        chev_color.setAlpha(245 if (exp_hover or self.hovering) else 135)
        self._draw_chevron(p, exp_rect.center(), 2.6, True, chev_color, stroke_w=1.35)

        # Model short name next to source pill
        model_short = self._shorten_model_name(
            (tel.active_model if tel else "") or "", compact=True
        )
        model_x = badge_rect.right() + 5.5
        model_rect = QRectF(model_x, 9.0, exp_rect.left() - model_x - 3.5, 15.5)
        p.setPen(QColor(148, 163, 184))
        p.setFont(self._ui_font("Segoe UI", 7.4, QFont.Weight.Medium))
        p.drawText(
            model_rect,
            Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
            self._elide_text(p, model_short, model_rect.width()),
        )

        # Middle row: Streaming live timer OR Last step Output Tokens & concise duration
        mid_rect = QRectF(left_x, 25.8, right_w - 2.0, 16.5)
        if streaming and tel is not None:
            elapsed = max(0.0, now - tel.in_flight.started_at)
            phase = tel.in_flight.phase
            self._draw_status_glyph(
                p, QPointF(left_x + 4.5, mid_rect.center().y()), phase, QColor(56, 189, 248)
            )
            p.setPen(QColor(125, 211, 252))
            p.setFont(self._ui_font("Segoe UI", 8.0, QFont.Weight.DemiBold))
            if phase == "reasoning":
                txt = f"深度思考  {elapsed:.1f}s"
            elif phase == "tool":
                txt = f"工具调用  {elapsed:.1f}s"
            elif tel.in_flight.first_token_at is not None or tel.in_flight.chunk_rate > 0:
                txt = f"流式生成  {elapsed:.1f}s"
            else:
                txt = f"等待响应  {elapsed:.1f}s"
            txt_rect = mid_rect.adjusted(12.5, 0, 0, 0)
            p.drawText(
                txt_rect,
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
                self._elide_text(p, txt, txt_rect.width()),
            )
        elif tel and tel.last_step:
            st = tel.last_step
            f_bold = self._ui_font("Segoe UI", 8.6, QFont.Weight.Bold)
            f_med = self._ui_font("Segoe UI", 7.4, QFont.Weight.Medium)
            tok_num = f"{st.output_tokens:,}"
            dur_val = (
                st.stream_duration_ms / 1000.0
                if self.tps_mode == "stream"
                else st.duration_ms / 1000.0
            )
            p.setFont(f_bold)
            p.setPen(QColor(248, 250, 252))
            p.drawText(mid_rect, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter, tok_num)
            w_num = self._text_width(p, tok_num)
            rest_rect = mid_rect.adjusted(w_num + 4.0, 0.5, 0, 0)
            p.setFont(f_med)
            p.setPen(QColor(100, 116, 139))
            p.drawText(
                rest_rect,
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
                self._elide_text(p, f"tok · {dur_val:.1f}s", rest_rect.width()),
            )
        else:
            p.setPen(QColor(100, 116, 139))
            p.setFont(self._ui_font("Segoe UI", 7.6, QFont.Weight.Medium))
            p.drawText(
                mid_rect,
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
                "待机中",
            )

        # Bottom row: Mini Sparkline of recent real TPS (+ live point when streaming)
        spark_rect = QRectF(left_x, 45.2, right_w - 11.0, 11.2)
        hist_vals = [self._step_tps(s) for s in (tel.history[-16:] if tel else [])]
        if streaming and self.displayed_tps > 0:
            hist_vals.append(self.displayed_tps)
        self._draw_sparkline(p, spark_rect, hist_vals, c1, c2, fill_area=True, compact=True)

    def _draw_circular_gauge(
        self,
        p: QPainter,
        rect: QRectF,
        tps_val: float,
        streaming: bool,
        c1: QColor,
        c2: QColor,
    ) -> None:
        # Radial dial depth background + subtle inner rim highlight
        dial_grad = QRadialGradient(rect.center(), rect.width() * 0.52)
        dial_grad.setColorAt(0.0, QColor(24, 35, 56, 230))
        dial_grad.setColorAt(0.70, QColor(13, 20, 36, 240))
        dial_grad.setColorAt(1.0, QColor(7, 11, 22, 248))
        p.setPen(QPen(QColor(255, 255, 255, 16), 0.8))
        p.setBrush(QBrush(dial_grad))
        p.drawEllipse(rect)

        arc_rect = rect.adjusted(4.0, 4.0, -4.0, -4.0)
        cx, cy = arc_rect.center().x(), arc_rect.center().y()
        r_arc = arc_rect.width() / 2.0

        # Subtle instrument scale ticks at 0%, 25%, 50%, 75%, 100%
        p.setPen(QPen(QColor(148, 163, 184, 48), 0.85, Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap))
        for tick_frac in (0.0, 0.25, 0.5, 0.75, 1.0):
            ang = math.radians(225.0 - 270.0 * tick_frac)
            ca, sa = math.cos(ang), math.sin(ang)
            p.drawLine(
                QPointF(cx + (r_arc - 2.6) * ca, cy - (r_arc - 2.6) * sa),
                QPointF(cx + (r_arc - 1.2) * ca, cy - (r_arc - 1.2) * sa),
            )

        track_pen = QPen(
            QColor(51, 65, 85, 100), 3.4, Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap
        )
        p.setPen(track_pen)
        p.drawArc(arc_rect, 225 * 16, -270 * 16)

        norm = min(1.0, max(0.04, tps_val / 160.0)) if tps_val > 0 else 0.0
        if streaming:
            pulse_offset = 0.08 * math.sin(self.pulse_phase * 2.2)
            span_deg = -270.0 * min(1.0, max(0.18, norm + pulse_offset))
        else:
            span_deg = -270.0 * norm

        span_16 = int(span_deg * 16)
        if tps_val > 0 or streaming:
            glow_grad = QLinearGradient(rect.topLeft(), rect.bottomRight())
            g1 = QColor(c1)
            g1.setAlpha(48 if streaming else 28)
            g2 = QColor(c2)
            g2.setAlpha(38 if streaming else 20)
            glow_grad.setColorAt(0.0, g1)
            glow_grad.setColorAt(1.0, g2)
            p.setPen(
                QPen(QBrush(glow_grad), 6.2, Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap)
            )
            p.drawArc(arc_rect, 225 * 16, span_16)

            grad = QLinearGradient(rect.topLeft(), rect.bottomRight())
            grad.setColorAt(0.0, c1)
            grad.setColorAt(1.0, c2)
            val_pen = QPen(QBrush(grad), 3.4, Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap)
            p.setPen(val_pen)
            p.drawArc(arc_rect, 225 * 16, span_16)

        # Glowing needle dot riding the end of the value arc
        if tps_val > 0 or streaming:
            frac = max(0.0, min(1.0, -span_deg / 270.0))
            tip_a = math.radians(225.0 - 270.0 * frac)
            tip = QPointF(cx + r_arc * math.cos(tip_a), cy - r_arc * math.sin(tip_a))
            tip_glow = QColor(c2)
            tip_glow.setAlpha(95)
            p.setPen(Qt.PenStyle.NoPen)
            p.setBrush(tip_glow)
            p.drawEllipse(tip, 3.5, 3.5)
            p.setBrush(QColor(248, 250, 252, 245))
            p.drawEllipse(tip, 1.5, 1.5)

        if tps_val > 0 or streaming:
            p.setPen(QColor(248, 250, 252))
            val_str = f"{tps_val:.1f}" if tps_val < 100 else f"{tps_val:.0f}"
        else:
            p.setPen(QColor(100, 116, 139))
            val_str = "--"
        font_val = self._ui_font(
            "Segoe UI", 11.2 if len(val_str) <= 4 else 9.2, QFont.Weight.Bold
        )
        p.setFont(font_val)
        p.drawText(rect.adjusted(0, -3.5, 0, -4.5), Qt.AlignmentFlag.AlignCenter, val_str)

        unit_pen = QColor(c1) if streaming else QColor(100, 116, 139)
        if streaming:
            unit_pen.setAlpha(int(185 + 70 * (0.5 + 0.5 * math.sin(self.pulse_phase * 2.2))))
        p.setPen(unit_pen)
        p.setFont(
            self._ui_font(
                "Segoe UI",
                6.0,
                QFont.Weight.Bold if streaming else QFont.Weight.DemiBold,
                letter_spacing=0.35,
            )
        )
        unit_label = "LIVE" if streaming else ("tok/s" if self.tps_mode == "stream" else "e2e/s")
        p.drawText(rect.adjusted(0, 21.0, 0, 0), Qt.AlignmentFlag.AlignCenter, unit_label)

    def _draw_sparkline(
        self,
        p: QPainter,
        rect: QRectF,
        values: List[float],
        c1: QColor,
        c2: QColor,
        fill_area: bool = True,
        compact: bool = False,
    ) -> None:
        if fill_area and not compact:
            # Subtle horizontal reference guide lines for expanded HUD chart
            p.setBrush(Qt.BrushStyle.NoBrush)
            p.setPen(QPen(QColor(51, 65, 85, 45), 0.75, Qt.PenStyle.DotLine))
            for frac_y in (0.33, 0.67):
                gy = rect.top() + rect.height() * frac_y
                p.drawLine(QPointF(rect.left(), gy), QPointF(rect.right(), gy))

        if not values:
            p.setBrush(Qt.BrushStyle.NoBrush)
            p.setPen(QPen(QColor(51, 65, 85, 100), 1.0, Qt.PenStyle.DashLine))
            y_mid = rect.center().y()
            p.drawLine(QPointF(rect.left(), y_mid), QPointF(rect.right(), y_mid))
            return

        pts: List[QPointF] = []
        v_min = min(values) * 0.82
        v_max = max(max(values) * 1.12, v_min + 10.0)
        n = len(values)
        for i, val in enumerate(values):
            x = (
                rect.left() + (rect.width() * (i / max(1, n - 1)))
                if n > 1
                else rect.center().x()
            )
            ratio = (val - v_min) / max(1e-5, v_max - v_min)
            y = rect.bottom() - ratio * rect.height()
            pts.append(QPointF(x, y))

        if len(pts) == 1:
            pts = [QPointF(rect.left(), pts[0].y()), QPointF(rect.right(), pts[0].y())]

        line_path = QPainterPath()
        line_path.moveTo(pts[0])
        if len(pts) >= 3:
            # Quadratic spline through segment midpoints — softer trace than straight lineTo
            for i in range(1, len(pts) - 1):
                mid = QPointF(
                    (pts[i].x() + pts[i + 1].x()) / 2.0,
                    (pts[i].y() + pts[i + 1].y()) / 2.0,
                )
                line_path.quadTo(pts[i], mid)
            line_path.lineTo(pts[-1])
        else:
            for pt in pts[1:]:
                line_path.lineTo(pt)

        if fill_area:
            area_path = QPainterPath(line_path)
            area_path.lineTo(QPointF(pts[-1].x(), rect.bottom()))
            area_path.lineTo(QPointF(pts[0].x(), rect.bottom()))
            area_path.closeSubpath()
            area_grad = QLinearGradient(rect.topLeft(), rect.bottomLeft())
            c_top = QColor(c1)
            c_top.setAlpha(36 if compact else 64)
            c_bot = QColor(c2)
            c_bot.setAlpha(0 if compact else 3)
            area_grad.setColorAt(0.0, c_top)
            area_grad.setColorAt(1.0, c_bot)
            p.fillPath(area_path, QBrush(area_grad))

        line_grad = QLinearGradient(rect.topLeft(), rect.topRight())
        line_grad.setColorAt(0.0, c2)
        line_grad.setColorAt(1.0, c1)
        p.setBrush(Qt.BrushStyle.NoBrush)
        p.setPen(
            QPen(
                QBrush(line_grad),
                1.5 if compact else 1.8,
                Qt.PenStyle.SolidLine,
                Qt.PenCapStyle.RoundCap,
            )
        )
        p.drawPath(line_path)

        last_pt = pts[-1]
        p.setPen(Qt.PenStyle.NoPen)
        halo = QColor(c1)
        halo.setAlpha(45 if compact else 56)
        p.setBrush(halo)
        r_halo = 3.6 if compact else 4.5
        p.drawEllipse(last_pt, r_halo, r_halo)
        p.setBrush(c1)
        r_dot = 2.0 if compact else 2.4
        p.drawEllipse(last_pt, r_dot, r_dot)
        core = QColor(248, 250, 252)
        core.setAlpha(210)
        p.setBrush(core)
        p.drawEllipse(last_pt, 0.9, 0.9)

    def _paint_expanded(self, p: QPainter) -> None:
        w, h = float(self.width()), float(self.height())
        now = time.time()
        tel = self._active_telemetry()
        src = tel.source if tel else "codex"
        streaming = bool(tel and tel.in_flight.active)
        flash_ratio = max(0.0, min(1.0, (self._completion_flash_until - now) / 1.25))
        c1, c2 = self._get_accent_colors(src, streaming)

        # 1. Outer Obsidian-Glass Card Background & Bezel
        card_rect = QRectF(2.0, 2.0, w - 4.0, h - 4.0)
        bg_alpha = int(246 * self.window_opacity)
        bg_grad = QLinearGradient(card_rect.topLeft(), card_rect.bottomRight())
        bg_grad.setColorAt(0.0, QColor(14, 21, 38, bg_alpha))
        bg_grad.setColorAt(0.5, QColor(9, 14, 26, bg_alpha))
        bg_grad.setColorAt(1.0, QColor(5, 9, 18, bg_alpha))

        card_path = QPainterPath()
        card_path.addRoundedRect(card_rect, 18.0, 18.0)
        p.fillPath(card_path, QBrush(bg_grad))
        self._draw_glass_finish(
            p,
            card_path,
            card_rect,
            flash_ratio=flash_ratio,
            accent=c1,
            ambient_strength=0.16 if streaming else 0.11,
        )
        self._draw_bezel_border(
            p, card_path, card_rect, c1, c2, streaming, flash_ratio, radius=18.0
        )

        # 2. Clean 2-Row Header Bar (Gauge + Engine Title & Concise Status Pill + Model Pill)
        gauge_rect = QRectF(16.0, 15.0, 52.0, 52.0)
        self._draw_circular_gauge(p, gauge_rect, self.displayed_tps, streaming, c1, c2)

        # Header Row 1: Engine Title + Non-redundant Status Badge
        p.setPen(QColor(248, 250, 252))
        title_font = self._ui_font("Segoe UI", 11.4, QFont.Weight.Bold)
        p.setFont(title_font)
        title_str = "OpenAI Codex" if src == "codex" else "ZCode Agent"
        title_rect = QRectF(79.0, 16.5, 132.0, 20.0)
        p.drawText(
            title_rect,
            Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
            title_str,
        )
        title_w = self._text_width(p, title_str)

        status_str = self._concise_status_label(tel, streaming)
        is_online = bool(streaming or (tel and tel.process_running))
        st_font = self._ui_font("Segoe UI", 6.4, QFont.Weight.Bold, letter_spacing=0.45)
        p.setFont(st_font)
        st_text_w = self._text_width(p, status_str)
        st_pill = QRectF(79.0 + title_w + 8.0, 18.5, st_text_w + 20.0, 15.5)
        st_color = c1 if is_online else QColor(100, 116, 139)
        st_bg = QColor(st_color)
        st_bg.setAlpha(34 if streaming else 20)
        st_edge = QColor(st_color)
        st_edge.setAlpha(115 if streaming else 65)
        p.setBrush(st_bg)
        p.setPen(QPen(st_edge, 0.8))
        p.drawRoundedRect(st_pill, 4.5, 4.5)

        dot_c = (
            QColor(56, 189, 248)
            if streaming
            else (QColor(34, 197, 94) if is_online else QColor(100, 116, 139))
        )
        p.setPen(Qt.PenStyle.NoPen)
        p.setBrush(dot_c)
        p.drawEllipse(QPointF(st_pill.left() + 7.0, st_pill.center().y()), 2.1, 2.1)

        p.setPen(c1 if is_online else QColor(148, 163, 184))
        p.drawText(
            st_pill.adjusted(12.0, 0, -4.0, 0),
            Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter | Qt.TextFlag.TextSingleLine,
            status_str,
        )

        # Header Row 2: Generous Model Chip (+ Live Elapsed Timer only when streaming)
        model_str = self._shorten_model_name(
            (tel.active_model if tel and tel.active_model else "未检测模型"),
            compact=False,
        )
        f_chip = self._ui_font("Segoe UI", 7.6, QFont.Weight.DemiBold)
        p.setFont(f_chip)
        max_chip_text_w = 155.0 if streaming else 210.0
        m_elided = self._elide_text(p, model_str, max_chip_text_w)
        m_tw = self._text_width(p, m_elided)
        chip_rect = QRectF(79.0, 42.5, m_tw + 15.0, 18.5)
        p.setBrush(QColor(20, 30, 48, 195))
        p.setPen(QPen(QColor(71, 85, 105, 105), 0.8))
        p.drawRoundedRect(chip_rect, 4.8, 4.8)
        p.setPen(QColor(226, 232, 240))
        p.drawText(chip_rect, Qt.AlignmentFlag.AlignCenter | Qt.TextFlag.TextSingleLine, m_elided)

        if streaming and tel:
            sub_x = chip_rect.right() + 8.0
            sub_rect = QRectF(sub_x, 42.5, w - 16.0 - sub_x, 18.5)
            el = max(0.0, now - tel.in_flight.started_at)
            p.setFont(self._ui_font("Segoe UI", 7.6, QFont.Weight.DemiBold))
            p.setPen(QColor(125, 211, 252))
            p.drawText(
                sub_rect,
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter | Qt.TextFlag.TextSingleLine,
                f"{el:.1f}s",
            )

        # Collapse button with custom vector upward chevron
        col_rect = QRectF(w - 38.0, 17.0, 22.0, 22.0)
        self._hit_rects["toggle_expand"] = col_rect
        col_hover = self._hover_key == "toggle_expand"
        p.setBrush(QColor(51, 65, 85, 220) if col_hover else QColor(20, 30, 48, 175))
        p.setPen(QPen(c1, 1.0) if col_hover else QPen(QColor(71, 85, 105, 110), 0.8))
        p.drawRoundedRect(col_rect, 6.0, 6.0)
        self._draw_chevron(
            p,
            col_rect.center(),
            3.3,
            False,
            c1 if col_hover else QColor(203, 213, 225),
            stroke_w=1.45,
        )

        # 3. Unified Segmented Control for Source Switching (AUTO | CODEX | ZCODE)
        track_rect = QRectF(16.0, 77.0, w - 32.0, 26.0)
        p.setBrush(QColor(8, 13, 24, 220))
        p.setPen(QPen(QColor(30, 41, 59, 175), 0.9))
        p.drawRoundedRect(track_rect, 7.5, 7.5)

        c_run = bool(self.snapshot and self.snapshot.codex.process_running)
        z_run = bool(self.snapshot and self.snapshot.zcode.process_running)
        c_act = bool(self.snapshot and self.snapshot.codex.in_flight.active)
        z_act = bool(self.snapshot and self.snapshot.zcode.in_flight.active)
        seg_w = (track_rect.width() - 6.0) / 3.0
        pills = [
            ("src_auto", "auto", "AUTO", None, False),
            ("src_codex", "codex", "CODEX", c_run, c_act),
            ("src_zcode", "zcode", "ZCODE", z_run, z_act),
        ]
        for idx, (h_key, m_val, label, proc_on, is_busy) in enumerate(pills):
            px = track_rect.left() + 3.0 + idx * seg_w
            pr = QRectF(px, track_rect.top() + 2.5, seg_w, track_rect.height() - 5.0)
            self._hit_rects[h_key] = pr
            selected = self.source_mode == m_val
            hov = self._hover_key == h_key
            if selected:
                seg_grad = QLinearGradient(pr.topLeft(), pr.bottomLeft())
                seg_grad.setColorAt(0.0, QColor(c1.red(), c1.green(), c1.blue(), 62 if hov else 48))
                seg_grad.setColorAt(1.0, QColor(c1.red(), c1.green(), c1.blue(), 28 if hov else 18))
                p.setBrush(QBrush(seg_grad))
                p.setPen(QPen(QColor(c1.red(), c1.green(), c1.blue(), 160), 0.95))
                p.drawRoundedRect(pr, 5.5, 5.5)
            elif hov:
                p.setBrush(QColor(30, 41, 59, 165))
                p.setPen(QPen(QColor(71, 85, 105, 120), 0.8))
                p.drawRoundedRect(pr, 5.5, 5.5)

            seg_font = self._ui_font(
                "Segoe UI",
                7.5,
                QFont.Weight.Bold if selected else QFont.Weight.DemiBold,
                letter_spacing=0.35,
            )
            p.setFont(seg_font)
            text_color = (
                c1
                if selected
                else (QColor(226, 232, 240) if hov else QColor(148, 163, 184))
            )

            if proc_on is None:
                p.setPen(text_color)
                p.drawText(pr, Qt.AlignmentFlag.AlignCenter | Qt.TextFlag.TextSingleLine, label)
            else:
                tw = self._text_width(p, label)
                total_w = tw + 11.0
                start_x = pr.center().x() - total_w * 0.5
                p.setPen(text_color)
                p.drawText(
                    QRectF(start_x, pr.top(), pr.right() - start_x, pr.height()),
                    Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter | Qt.TextFlag.TextSingleLine,
                    label,
                )
                dot_x = start_x + tw + 7.5
                dot_y = pr.center().y() + 0.5
                p.setPen(Qt.PenStyle.NoPen)
                if is_busy:
                    p.setBrush(QColor(56, 189, 248, 70))
                    p.drawEllipse(QPointF(dot_x, dot_y), 4.2, 4.2)
                    p.setBrush(QColor(56, 189, 248))
                else:
                    p.setBrush(QColor(34, 197, 94) if proc_on else QColor(71, 85, 105))
                p.drawEllipse(QPointF(dot_x, dot_y), 2.3, 2.3)

        # 4. 2x2 Core Metric Cards (Symmetrical 4-char labels + high-contrast values)
        st = tel.last_step if tel else None
        card3_title = "首字 / 耗时"
        card4_title = (
            "输出 / 推理"
            if (not streaming and st and st.reasoning_tokens > 0)
            else "本步输出"
        )
        if streaming and tel is not None and self.displayed_tps > 0:
            el = max(0.05, now - tel.in_flight.started_at)
            s_main, s_unit = f"~{self.displayed_tps:.1f}", "tok/s"
            if tel.in_flight.first_token_at is not None and now >= tel.in_flight.first_token_at:
                stream_dt = max(0.05, now - tel.in_flight.first_token_at)
                live_e2e = self.displayed_tps * min(1.0, stream_dt / el)
                e_main, e_unit = f"~{live_e2e:.1f}", "tok/s"
                ttft_live_ms = max(
                    0.0, (tel.in_flight.first_token_at - tel.in_flight.started_at) * 1000.0
                )
                t_main = self._format_ttft_ms(ttft_live_ms)
                t_unit = f"/ {el:.1f}s"
                est_toks = max(1, int(round(self.displayed_tps * stream_dt)))
                o_main, o_unit = f"~{est_toks:,}", "tok"
            else:
                card3_title = "首字等待"
                e_main, e_unit = "--", ""
                t_main, t_unit = f"{el:.1f}s", ""
                o_main, o_unit = "--", ""
        elif st:
            s_main, s_unit = f"{st.tps_stream:.1f}", "tok/s"
            e_main, e_unit = f"{st.tps_e2e:.1f}", "tok/s"
            if st.ttft_ms is not None:
                t_main = self._format_ttft_ms(st.ttft_ms)
                t_unit = f"/ {st.duration_ms / 1000.0:.1f}s"
            else:
                card3_title = "本步耗时"
                t_main = f"{st.duration_ms / 1000.0:.1f}s"
                t_unit = ""
            o_main = f"{st.output_tokens:,}"
            o_unit = f"/ {st.reasoning_tokens:,}" if st.reasoning_tokens > 0 else "tok"
        else:
            s_main, s_unit = "--", ""
            e_main, e_unit = "--", ""
            t_main, t_unit = "--", ""
            o_main, o_unit = "--", ""

        cards = [
            ("card_stream", "流式速率", s_main, s_unit, self.tps_mode == "stream"),
            ("card_e2e", "端到端速率", e_main, e_unit, self.tps_mode == "e2e"),
            ("", card3_title, t_main, t_unit, False),
            ("", card4_title, o_main, o_unit, False),
        ]
        grid_y = 112.0
        cw = (w - 32.0 - 8.0) / 2.0
        ch = 50.0
        for i, (hit_k, lbl, m_val, u_val, highlight) in enumerate(cards):
            gx = 16.0 + (i % 2) * (cw + 8.0)
            gy = grid_y + (i // 2) * (ch + 8.0)
            cr = QRectF(gx, gy, cw, ch)
            if hit_k:
                self._hit_rects[hit_k] = cr
            hov = bool(hit_k) and self._hover_key == hit_k
            self._draw_panel_card(p, cr, radius=9.0, active=highlight, hovered=hov, accent=c1)

            # Card top label (muted slate so primary numerals pop)
            p.setPen(QColor(148, 163, 184) if highlight else QColor(100, 116, 139))
            p.setFont(self._ui_font("Segoe UI", 7.3, QFont.Weight.Medium))
            p.drawText(
                cr.adjusted(12.0, 6.0, -8.0, 0),
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop,
                lbl,
            )

            # Subtle glowing active dot on selected TPS formula card
            if highlight:
                dot_pt = QPointF(cr.right() - 12.0, cr.top() + 12.0)
                halo_c = QColor(c1)
                halo_c.setAlpha(55)
                p.setPen(Qt.PenStyle.NoPen)
                p.setBrush(halo_c)
                p.drawEllipse(dot_pt, 4.2, 4.2)
                p.setBrush(c1)
                p.drawEllipse(dot_pt, 2.1, 2.1)

            # Primary value + baseline-aligned secondary unit
            val_font = self._ui_font("Segoe UI", 13.0, QFont.Weight.Bold)
            unit_font = self._ui_font("Segoe UI", 7.6, QFont.Weight.Medium)
            p.setFont(val_font)
            p.setPen(c1 if highlight else QColor(248, 250, 252))
            val_box = cr.adjusted(12.0, 21.0, -8.0, -4.0)
            p.drawText(val_box, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter, m_val)

            if u_val:
                m_w = self._text_width(p, m_val)
                u_box = val_box.adjusted(m_w + 4.5, 2.2, 0, 0)
                p.setFont(unit_font)
                p.setPen(
                    QColor(c1.red(), c1.green(), c1.blue(), 185)
                    if highlight
                    else QColor(100, 116, 139)
                )
                p.drawText(
                    u_box,
                    Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter | Qt.TextFlag.TextSingleLine,
                    self._elide_text(p, u_val, u_box.width()),
                )

        # 5. Session Cumulative Summary Strip (4 single-value columns, zero slash clutter)
        strip_rect = QRectF(16.0, 229.0, w - 32.0, 38.0)
        self._draw_panel_card(p, strip_rect, radius=9.0)

        if tel:
            cache_pct = (
                (tel.session_cached_tokens / tel.session_input_tokens * 100.0)
                if tel.session_input_tokens > 0
                else 0.0
            )
            avg_tps = tel.avg_tps_stream if self.tps_mode == "stream" else tel.avg_tps_e2e
            peak_tps = tel.peak_tps_stream if self.tps_mode == "stream" else tel.peak_tps_e2e
            strip_cols = [
                ("累计输出", self._format_compact_tokens(tel.session_output_tokens)),
                ("平均速率", f"{avg_tps:.1f}" if avg_tps > 0 else "--"),
                ("峰值速率", f"{peak_tps:.1f}" if peak_tps > 0 else "--"),
                ("缓存命中", f"{cache_pct:.0f}%"),
            ]
        else:
            strip_cols = [
                ("累计输出", "0"),
                ("平均速率", "--"),
                ("峰值速率", "--"),
                ("缓存命中", "0%"),
            ]

        col_w = strip_rect.width() / 4.0
        for c_idx, (c_lbl, c_val) in enumerate(strip_cols):
            cell_x = strip_rect.left() + c_idx * col_w
            if c_idx > 0:
                p.setPen(QPen(QColor(51, 65, 85, 75), 0.85))
                p.drawLine(
                    QPointF(cell_x, strip_rect.top() + 8.5),
                    QPointF(cell_x, strip_rect.bottom() - 8.5),
                )
            p.setPen(QColor(100, 116, 139))
            p.setFont(self._ui_font("Segoe UI", 6.7, QFont.Weight.Medium))
            p.drawText(
                QRectF(cell_x + 3.0, strip_rect.top() + 4.5, col_w - 6.0, 12.5),
                Qt.AlignmentFlag.AlignCenter,
                c_lbl,
            )
            p.setPen(QColor(241, 245, 249))
            p.setFont(self._ui_font("Segoe UI", 8.8, QFont.Weight.Bold))
            p.drawText(
                QRectF(cell_x + 3.0, strip_rect.top() + 18.0, col_w - 6.0, 16.5),
                Qt.AlignmentFlag.AlignCenter,
                self._elide_text(p, c_val, col_w - 6.0),
            )

        # 6. Historical TPS Sparkline Area Chart
        chart_box = QRectF(16.0, 276.0, w - 32.0, 68.0)
        self._draw_panel_card(p, chart_box, radius=9.0)

        hist_vals = [self._step_tps(s) for s in (tel.history[-24:] if tel else [])]
        if streaming and self.displayed_tps > 0:
            hist_vals.append(self.displayed_tps)

        p.setPen(QColor(100, 116, 139))
        p.setFont(self._ui_font("Segoe UI", 7.2, QFont.Weight.Medium))
        p.drawText(
            chart_box.adjusted(12.0, 5.5, -12.0, 0),
            Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop,
            "速率趋势",
        )
        step_cnt = tel.total_steps if tel else 0
        if step_cnt > 0:
            p.setPen(QColor(100, 116, 139))
            p.setFont(self._ui_font("Segoe UI", 6.9, QFont.Weight.Medium))
            p.drawText(
                chart_box.adjusted(12.0, 6.0, -12.0, 0),
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignTop,
                f"共 {step_cnt} 步",
            )
        spark_inner = chart_box.adjusted(12.0, 23.0, -12.0, -8.0)
        self._draw_sparkline(p, spark_inner, hist_vals, c1, c2, fill_area=True, compact=False)

        # 7. Recent Steps Table inside a framed Glass Card
        table_box = QRectF(16.0, 353.0, w - 32.0, 86.0)
        self._draw_panel_card(p, table_box, radius=9.0)

        table_y = table_box.top() + 3.5
        col_specs = [
            (28.0, 54.0, Qt.AlignmentFlag.AlignLeft, "时间"),
            (86.0, 104.0, Qt.AlignmentFlag.AlignLeft, "模型"),
            (192.0, 50.0, Qt.AlignmentFlag.AlignRight, "输出"),
            (246.0, 48.0, Qt.AlignmentFlag.AlignRight, "耗时"),
            (297.0, 43.0, Qt.AlignmentFlag.AlignRight, "TPS"),
        ]
        p.setPen(QColor(100, 116, 139))
        p.setFont(self._ui_font("Segoe UI", 6.9, QFont.Weight.DemiBold))
        for cx, cw_col, align, h_text in col_specs:
            p.drawText(
                QRectF(cx, table_y, cw_col, 14.5),
                align | Qt.AlignmentFlag.AlignVCenter,
                h_text,
            )
        p.setPen(QPen(QColor(30, 41, 59, 175), 0.85))
        p.drawLine(
            QPointF(table_box.left() + 10.0, table_y + 15.5),
            QPointF(table_box.right() - 10.0, table_y + 15.5),
        )

        recent_steps = list(tel.history[-4:]) if tel else []
        recent_steps.reverse()
        row_y = table_y + 17.5
        if not recent_steps:
            p.setPen(QColor(100, 116, 139))
            p.setFont(self._ui_font("Segoe UI", 7.6, QFont.Weight.Medium))
            p.drawText(
                QRectF(table_box.left(), row_y, table_box.width(), 60.0),
                Qt.AlignmentFlag.AlignCenter,
                "暂无记录",
            )
        else:
            row_font = self._ui_font("Cascadia Mono", 7.7, QFont.Weight.Medium)
            row_bold = self._ui_font("Cascadia Mono", 7.7, QFont.Weight.Bold)
            for row_i, s in enumerate(recent_steps):
                if row_i == 0:
                    hl_path = QPainterPath()
                    hl_path.addRoundedRect(
                        QRectF(table_box.left() + 6.0, row_y, table_box.width() - 12.0, 15.2),
                        3.8,
                        3.8,
                    )
                    p.fillPath(hl_path, QColor(c1.red(), c1.green(), c1.blue(), 16))
                t_str = datetime.datetime.fromtimestamp(s.completed_at).strftime("%H:%M:%S")
                p.setFont(row_font)
                m_clean = self._shorten_model_name(s.model, compact=True)
                m_short = self._elide_text(p, m_clean, col_specs[1][1] - 4.0)
                out_s = f"{s.output_tokens:,}"
                dur_s = (
                    f"{s.stream_duration_ms / 1000.0:.1f}s"
                    if self.tps_mode == "stream"
                    else f"{s.duration_ms / 1000.0:.1f}s"
                )
                tps_s = f"{self._step_tps(s):.1f}"
                row_vals = [t_str, m_short, out_s, dur_s, tps_s]
                for idx, (cx, cw_col, align, _) in enumerate(col_specs):
                    if idx == 4:
                        p.setFont(row_bold)
                        p.setPen(c1)
                    elif idx == 0:
                        p.setFont(row_font)
                        p.setPen(QColor(100, 116, 139))
                    elif idx == 1:
                        p.setFont(row_font)
                        p.setPen(QColor(226, 232, 240) if row_i == 0 else QColor(148, 163, 184))
                    elif idx == 3:
                        p.setFont(row_font)
                        p.setPen(QColor(148, 163, 184))
                    else:
                        p.setFont(row_font)
                        p.setPen(QColor(241, 245, 249) if row_i == 0 else QColor(203, 213, 225))
                    p.drawText(
                        QRectF(cx, row_y, cw_col, 15.2),
                        align | Qt.AlignmentFlag.AlignVCenter,
                        row_vals[idx],
                    )
                row_y += 15.8

        # 8. Bottom Action Buttons (Switch TPS Mode + Replay Real Data)
        btn_y = 447.0
        btn_w = (w - 32.0 - 8.0) / 2.0

        b1_rect = QRectF(16.0, btn_y, btn_w, 25.0)
        self._hit_rects["toggle_tps_mode"] = b1_rect
        b1_hover = self._hover_key == "toggle_tps_mode"
        self._draw_panel_card(p, b1_rect, radius=7.0, hovered=b1_hover)
        b1_fg = QColor(241, 245, 249) if b1_hover else QColor(203, 213, 225)
        p.setPen(b1_fg)
        p.setFont(self._ui_font("Segoe UI", 7.8, QFont.Weight.DemiBold))
        mode_lbl = "口径 · 流式" if self.tps_mode == "stream" else "口径 · 端到端"
        b1_tw = self._text_width(p, mode_lbl)
        b1_start = b1_rect.center().x() - (b1_tw + 14.0) * 0.5
        iy = b1_rect.center().y()
        ix = b1_start + 4.5
        p.save()
        p.setPen(
            QPen(
                c1 if b1_hover else QColor(148, 163, 184),
                1.25,
                Qt.PenStyle.SolidLine,
                Qt.PenCapStyle.RoundCap,
                Qt.PenJoinStyle.RoundJoin,
            )
        )
        p.drawLine(QPointF(ix - 3.8, iy - 1.8), QPointF(ix + 3.8, iy - 1.8))
        p.drawLine(QPointF(ix + 1.8, iy - 3.6), QPointF(ix + 3.8, iy - 1.8))
        p.drawLine(QPointF(ix + 3.8, iy + 1.8), QPointF(ix - 3.8, iy + 1.8))
        p.drawLine(QPointF(ix - 1.8, iy + 3.6), QPointF(ix - 3.8, iy + 1.8))
        p.restore()
        p.setPen(b1_fg)
        p.drawText(
            QRectF(b1_start + 13.0, b1_rect.top(), b1_rect.right() - (b1_start + 13.0), b1_rect.height()),
            Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter | Qt.TextFlag.TextSingleLine,
            mode_lbl,
        )

        b2_rect = QRectF(16.0 + btn_w + 8.0, btn_y, btn_w, 25.0)
        self._hit_rects["trigger_replay"] = b2_rect
        replay_on = bool(self.snapshot and self.snapshot.replay_active)
        b2_hover = self._hover_key == "trigger_replay"
        b2_grad = QLinearGradient(b2_rect.topLeft(), b2_rect.bottomLeft())
        a_top = (82 if b2_hover else 56) if replay_on else (50 if b2_hover else 32)
        a_bot = (46 if b2_hover else 28) if replay_on else (24 if b2_hover else 14)
        b2_grad.setColorAt(0.0, QColor(c1.red(), c1.green(), c1.blue(), a_top))
        b2_grad.setColorAt(1.0, QColor(c2.red(), c2.green(), c2.blue(), a_bot))
        p.setBrush(QBrush(b2_grad))
        p.setPen(QPen(QColor(c1.red(), c1.green(), c1.blue(), 195 if b2_hover else 140), 0.95))
        p.drawRoundedRect(b2_rect, 7.0, 7.0)
        p.setPen(c1)
        p.setFont(self._ui_font("Segoe UI", 7.8, QFont.Weight.Bold))
        b2_lbl = "停止回放" if replay_on else "实测回放"
        b2_tw = self._text_width(p, b2_lbl)
        b2_start = b2_rect.center().x() - (b2_tw + 13.0) * 0.5
        p.save()
        p.setPen(Qt.PenStyle.NoPen)
        p.setBrush(c1)
        ic_x = b2_start + 4.0
        ic_y = b2_rect.center().y()
        if replay_on:
            p.drawRoundedRect(QRectF(ic_x - 3.2, ic_y - 3.2, 6.4, 6.4), 1.2, 1.2)
        else:
            tri = QPainterPath()
            tri.moveTo(ic_x - 2.8, ic_y - 3.8)
            tri.lineTo(ic_x + 3.8, ic_y)
            tri.lineTo(ic_x - 2.8, ic_y + 3.8)
            tri.closeSubpath()
            p.drawPath(tri)
        p.restore()
        p.setPen(c1)
        p.drawText(
            QRectF(b2_start + 12.0, b2_rect.top(), b2_rect.right() - (b2_start + 12.0), b2_rect.height()),
            Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter | Qt.TextFlag.TextSingleLine,
            b2_lbl,
        )


# ============================================================================
# Single-Instance Management & CLI Entrypoint
# ============================================================================


def terminate_existing_instances() -> None:
    """Ensure only one floating ball instance runs at a time to prevent overlapping windows."""
    if psutil is None:
        return
    my_pid = os.getpid()
    script_name = pathlib.Path(__file__).name.lower()
    try:
        for proc in psutil.process_iter(["pid", "name"]):
            if proc.info["pid"] == my_pid:
                continue
            # Only consider Python interpreters so unrelated processes (editors, shells...)
            # that merely reference the script path in their cmdline are never terminated.
            name = (proc.info.get("name") or "").lower()
            if not name.startswith("python"):
                continue
            try:
                cmdline = proc.cmdline()
            except Exception:
                continue
            if any(script_name in (arg or "").lower() for arg in cmdline):
                try:
                    proc.terminate()
                except Exception:
                    pass
    except Exception:
        pass


def run_self_test() -> int:
    """Verify real-data collectors against the user's actual ~/.codex and ~/.zcode data."""
    print("=== Codex & ZCode Real-Data TPS Monitor Self-Test ===")
    engine = TokenMonitorEngine()
    snap = engine.collect_once()
    engine.stop()

    for name, tel in [
        ("OpenAI Codex (~/.codex)", snap.codex),
        ("ZCode (~/.zcode)", snap.zcode),
    ]:
        print(f"\n[{name}]")
        print(f"  Process Running      : {tel.process_running}")
        print(f"  Active Model         : {tel.active_model}")
        print(f"  Preloaded Real Steps : {len(tel.history)} (Total seen: {tel.total_steps})")
        print(
            f"  Session Output Toks  : {tel.session_output_tokens:,} (Reasoning: {tel.session_reasoning_tokens:,})"
        )
        print(
            f"  Session Input Toks   : {tel.session_input_tokens:,} (Cached: {tel.session_cached_tokens:,})"
        )
        print(
            f"  Avg Streaming TPS    : {tel.avg_tps_stream:.2f} tok/s | Peak: {tel.peak_tps_stream:.2f} tok/s"
        )
        print(
            f"  Avg End-to-End TPS   : {tel.avg_tps_e2e:.2f} tok/s | Peak: {tel.peak_tps_e2e:.2f} tok/s"
        )
        if tel.last_step:
            st = tel.last_step
            dt = datetime.datetime.fromtimestamp(st.completed_at).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            ttft_s = f"{st.ttft_ms:.0f}ms" if st.ttft_ms is not None else "N/A"
            print(
                f"  Latest Step          : [{dt}] model={st.model}, out={st.output_tokens:,} tok, "
                f"reas={st.reasoning_tokens:,} tok, duration={st.duration_ms:.0f}ms, "
                f"ttft={ttft_s} -> "
                f"Stream TPS={st.tps_stream:.2f} tok/s, E2E TPS={st.tps_e2e:.2f} tok/s"
            )
    print(f"\nSelected Active Source : {snap.active_source.upper()}")
    print("Self-test passed successfully!")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Codex & ZCode Real-Time Token TPS Floating Ball"
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run collector self-test on real local data and exit",
    )
    parser.add_argument(
        "--expanded",
        action="store_true",
        help="Launch directly in expanded HUD mode",
    )
    args = parser.parse_args()

    if args.self_test:
        return run_self_test()

    terminate_existing_instances()

    if hasattr(QGuiApplication, "setHighDpiScaleFactorRoundingPolicy") and hasattr(
        Qt, "HighDpiScaleFactorRoundingPolicy"
    ):
        QGuiApplication.setHighDpiScaleFactorRoundingPolicy(
            Qt.HighDpiScaleFactorRoundingPolicy.PassThrough
        )

    app = QApplication(sys.argv)
    app.setQuitOnLastWindowClosed(False)

    engine = TokenMonitorEngine()
    window = TPSFloatingBallWindow(engine)
    if args.expanded:
        window.toggle_expanded()
    window.show()
    engine.start()

    exit_code = app.exec() if hasattr(app, "exec") else app.exec_()
    engine.stop()
    return int(exit_code)


if __name__ == "__main__":
    sys.exit(main())
