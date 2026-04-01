
import calendar
import io
import json
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
import streamlit as st
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

APP_DIR = Path(__file__).resolve().parent
DB_FILE = APP_DIR / "rota.db"
DATA_FILE = APP_DIR / "rota_saved_inputs.json"
ROTA_FILE = APP_DIR / "rota_saved_output.json"
ROTA_EXCEL_FILE = APP_DIR / "rota_saved_output.xlsx"
ROTA_CSV_FILE = APP_DIR / "rota_saved_matrix.csv"
STATE_KEY_INPUTS = "saved_inputs"
STATE_KEY_ROTA = "saved_rota"

SHIFT_MORNING = "Morning"
SHIFT_AFTERNOON = "Afternoon"
SHIFT_NIGHT = "Night"
SHIFT_WEEKOFF = "Week Off"
SHIFT_LEAVE = "Leave"
SHIFT_UNASSIGNED = "Unassigned"

SHIFT_CODE_MAP = {
    SHIFT_MORNING: "M",
    SHIFT_AFTERNOON: "A",
    SHIFT_NIGHT: "N",
    SHIFT_WEEKOFF: "WO",
    SHIFT_LEAVE: "L",
    SHIFT_UNASSIGNED: "-",
}

SHIFT_COLOR_MAP = {
    SHIFT_MORNING: "#D9EAF7",
    SHIFT_AFTERNOON: "#FDE9D9",
    SHIFT_NIGHT: "#D9D2E9",
    SHIFT_WEEKOFF: "#E2F0D9",
    SHIFT_LEAVE: "#F4CCCC",
    SHIFT_UNASSIGNED: "#EDEDED",
    "BANK_HOLIDAY_HEADER": "#CFE2F3",
}

# GMT shift timings
SHIFT_TIME_MAP = {
    SHIFT_MORNING: (time(1, 0), time(10, 30)),
    SHIFT_AFTERNOON: (time(7, 30), time(17, 0)),
    SHIFT_NIGHT: (time(15, 30), time(1, 0)),  # ends next day
}

MIN_MORNING = 2
MIN_AFTERNOON = 2
MIN_NIGHT = 2
MAX_CONTINUOUS_NIGHT = 5
MANDATORY_OFF_AFTER_NIGHT = 2
MAX_CONTINUOUS_WORKING_DAYS = 6


def init_database():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS app_state (
                state_key TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.commit()


def json_default(value: Any) -> str:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def save_state(state_key: str, payload: dict):
    init_database()
    serialized = json.dumps(payload, indent=2, default=json_default)
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute(
            """
            INSERT INTO app_state(state_key, payload, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(state_key) DO UPDATE SET
                payload = excluded.payload,
                updated_at = excluded.updated_at
            """,
            (state_key, serialized, datetime.utcnow().isoformat(timespec="seconds")),
        )
        conn.commit()


def load_state(state_key: str) -> Optional[dict]:
    init_database()
    with sqlite3.connect(DB_FILE) as conn:
        row = conn.execute(
            "SELECT payload FROM app_state WHERE state_key = ?",
            (state_key,),
        ).fetchone()
    if row is None:
        return None
    return json.loads(row[0])


def delete_state(state_key: str):
    init_database()
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("DELETE FROM app_state WHERE state_key = ?", (state_key,))
        conn.commit()


def migrate_legacy_json_if_needed(state_key: str, legacy_file: Path):
    if load_state(state_key) is not None or not legacy_file.exists():
        return
    try:
        payload = json.loads(legacy_file.read_text())
        save_state(state_key, payload)
    except Exception:
        pass


@dataclass
class Member:
    name: str
    dept: str
    file_id: str
    afternoon_only: bool = False

    @property
    def weekend_off_if_afternoon_only(self) -> bool:
        return self.afternoon_only


def month_key(dt: date) -> Tuple[int, int]:
    return dt.year, dt.month


def dates_in_range(start_date: date, end_date: date) -> List[date]:
    return [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]


def month_segment_days(start_date: date, end_date: date) -> Dict[Tuple[int, int], List[date]]:
    grouped: Dict[Tuple[int, int], List[date]] = {}
    for dt in dates_in_range(start_date, end_date):
        grouped.setdefault(month_key(dt), []).append(dt)
    return grouped


def prorated_target(global_weekoffs_per_month: int, start_date: date, end_date: date) -> Dict[Tuple[int, int], int]:
    targets: Dict[Tuple[int, int], int] = {}
    for mk, days in month_segment_days(start_date, end_date).items():
        year, month = mk
        days_in_month = calendar.monthrange(year, month)[1]
        targets[mk] = round(global_weekoffs_per_month * len(days) / days_in_month)
    return targets


def ensure_date_columns(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    df = df.copy()
    for col in cols:
        if col not in df.columns:
            df[col] = pd.NaT
        df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def sample_team_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"name": "Aarav", "dept": "Ops", "file_id": "F001", "afternoon_only": "No"},
            {"name": "Bhavna", "dept": "Ops", "file_id": "F002", "afternoon_only": "No"},
            {"name": "Charan", "dept": "Support", "file_id": "F003", "afternoon_only": "Yes"},
            {"name": "Divya", "dept": "Support", "file_id": "F004", "afternoon_only": "No"},
            {"name": "Eshan", "dept": "Ops", "file_id": "F005", "afternoon_only": "No"},
            {"name": "Farah", "dept": "Ops", "file_id": "F006", "afternoon_only": "No"},
            {"name": "Gautham", "dept": "Support", "file_id": "F007", "afternoon_only": "No"},
            {"name": "Harini", "dept": "Support", "file_id": "F008", "afternoon_only": "No"},
        ]
    )


def sample_leaves_df() -> pd.DataFrame:
    today = date.today()
    df = pd.DataFrame(
        [
            {"name": "Bhavna", "leave_start_date": today + timedelta(days=4), "leave_end_date": today + timedelta(days=5)},
            {"name": "Divya", "leave_start_date": today + timedelta(days=9), "leave_end_date": today + timedelta(days=10)},
        ]
    )
    return ensure_date_columns(df, ["leave_start_date", "leave_end_date"])


def sample_bank_holidays_df() -> pd.DataFrame:
    df = pd.DataFrame([{"bank_holiday_date": pd.NaT}])
    return ensure_date_columns(df, ["bank_holiday_date"])


def sample_sync_groups_df() -> pd.DataFrame:
    return pd.DataFrame([
        {"sync_group": ""}
    ])


def load_inputs():
    migrate_legacy_json_if_needed(STATE_KEY_INPUTS, DATA_FILE)
    data = load_state(STATE_KEY_INPUTS)
    if data is None:
        return sample_team_df(), sample_leaves_df(), sample_bank_holidays_df(), sample_sync_groups_df()

    try:
        team_df = pd.DataFrame(data.get("team", []))
        if team_df.empty:
            team_df = sample_team_df()

        leaves_df = pd.DataFrame(data.get("leaves", []))
        if leaves_df.empty:
            leaves_df = sample_leaves_df()
        leaves_df = ensure_date_columns(leaves_df, ["leave_start_date", "leave_end_date"])

        bank_df = pd.DataFrame(data.get("bank_holidays", []))
        if bank_df.empty:
            bank_df = sample_bank_holidays_df()
        bank_df = ensure_date_columns(bank_df, ["bank_holiday_date"])

        sync_df = pd.DataFrame(data.get("sync_groups", []))
        if sync_df.empty:
            sync_df = sample_sync_groups_df()
        if "sync_group" not in sync_df.columns:
            sync_df["sync_group"] = ""
        return team_df, leaves_df, bank_df, sync_df
    except Exception:
        return sample_team_df(), sample_leaves_df(), sample_bank_holidays_df(), sample_sync_groups_df()


def serialize_dates_for_json(records: List[dict], date_cols: List[str]) -> List[dict]:
    out = []
    for row in records:
        row = dict(row)
        for c in date_cols:
            v = row.get(c)
            if pd.isna(v) or v in ("", None):
                row[c] = None
            else:
                row[c] = pd.to_datetime(v).date().isoformat()
        out.append(row)
    return out


def save_inputs(team_df: pd.DataFrame, leaves_df: pd.DataFrame, bank_df: pd.DataFrame, sync_df: pd.DataFrame):
    payload = {
        "team": team_df.fillna("").to_dict(orient="records"),
        "leaves": serialize_dates_for_json(leaves_df.to_dict(orient="records"), ["leave_start_date", "leave_end_date"]),
        "bank_holidays": serialize_dates_for_json(bank_df.to_dict(orient="records"), ["bank_holiday_date"]),
        "sync_groups": sync_df.fillna("").to_dict(orient="records"),
    }
    save_state(STATE_KEY_INPUTS, payload)


def save_generated_rota(matrix_df: pd.DataFrame, full_df: pd.DataFrame, daywise_df: pd.DataFrame,
                        summary_df: pd.DataFrame, warnings_df: pd.DataFrame, bank_holidays: Set[date],
                        start_date: date, end_date: date, excel_bytes: bytes):
    payload = {
        "metadata": {
            "saved_at": datetime.utcnow().isoformat(timespec="seconds"),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "bank_holidays": sorted([d.isoformat() for d in bank_holidays]),
        },
        "matrix_df": matrix_df.to_dict(orient="records"),
        "full_df": full_df.to_dict(orient="records"),
        "daywise_df": daywise_df.to_dict(orient="records"),
        "summary_df": summary_df.to_dict(orient="records"),
        "warnings_df": warnings_df.to_dict(orient="records"),
    }
    save_state(STATE_KEY_ROTA, payload)
    ROTA_CSV_FILE.write_text(matrix_df.to_csv(index=False))
    ROTA_EXCEL_FILE.write_bytes(excel_bytes)


def load_saved_rota():
    migrate_legacy_json_if_needed(STATE_KEY_ROTA, ROTA_FILE)
    payload = load_state(STATE_KEY_ROTA)
    if payload is None:
        return None
    try:
        return {
            "metadata": payload.get("metadata", {}),
            "matrix_df": pd.DataFrame(payload.get("matrix_df", [])),
            "full_df": pd.DataFrame(payload.get("full_df", [])),
            "daywise_df": pd.DataFrame(payload.get("daywise_df", [])),
            "summary_df": pd.DataFrame(payload.get("summary_df", [])),
            "warnings_df": pd.DataFrame(payload.get("warnings_df", [])),
            "bank_holidays": {datetime.strptime(d, "%Y-%m-%d").date() for d in payload.get("metadata", {}).get("bank_holidays", [])},
        }
    except Exception:
        return None


def parse_members(df: pd.DataFrame) -> List[Member]:
    colmap = {c.lower(): c for c in df.columns}
    required = {"name", "dept", "file_id", "afternoon_only"}
    if not required.issubset(colmap):
        raise ValueError("Team sheet must contain columns: name, dept, file_id, afternoon_only")

    members: List[Member] = []
    seen: Set[str] = set()
    for _, row in df.iterrows():
        name = str(row[colmap["name"]]).strip()
        if not name or name.lower() == "nan":
            continue
        if name.lower() in seen:
            raise ValueError(f"Duplicate member found: {name}")
        seen.add(name.lower())

        dept = str(row[colmap["dept"]]).strip()
        if dept.lower() == "nan":
            dept = ""

        file_id = str(row[colmap["file_id"]]).strip()
        if file_id.lower() == "nan":
            file_id = ""

        afternoon_only = str(row[colmap["afternoon_only"]]).strip().lower() in {"yes", "y", "true", "1"}
        members.append(Member(name=name, dept=dept, file_id=file_id, afternoon_only=afternoon_only))

    if not members:
        raise ValueError("Please add at least one team member.")
    return members


def parse_leaves(df: pd.DataFrame, valid_names: Set[str]) -> Dict[str, Set[date]]:
    if df.empty:
        return {}

    colmap = {c.lower(): c for c in df.columns}
    required = {"name", "leave_start_date", "leave_end_date"}
    if not required.issubset(colmap):
        raise ValueError("Leaves sheet must contain columns: name, leave_start_date, leave_end_date")

    leave_map: Dict[str, Set[date]] = {}
    for _, row in df.iterrows():
        name = str(row[colmap["name"]]).strip()
        if not name or name.lower() == "nan":
            continue
        if name not in valid_names:
            raise ValueError(f"Leave entered for unknown member: {name}")

        start_value = row[colmap["leave_start_date"]]
        end_value = row[colmap["leave_end_date"]]
        if pd.isna(start_value) or pd.isna(end_value):
            continue

        start_dt = pd.to_datetime(start_value).date()
        end_dt = pd.to_datetime(end_value).date()
        if end_dt < start_dt:
            raise ValueError(f"Leave end date cannot be before start date for {name}.")

        current = start_dt
        while current <= end_dt:
            leave_map.setdefault(name, set()).add(current)
            current += timedelta(days=1)
    return leave_map


def parse_specific_bank_holidays(df: pd.DataFrame) -> Set[date]:
    if df.empty:
        return set()
    if "bank_holiday_date" not in df.columns:
        raise ValueError("Bank holiday sheet must contain column: bank_holiday_date")

    holiday_dates: Set[date] = set()
    for _, row in df.iterrows():
        value = row["bank_holiday_date"]
        if pd.isna(value):
            continue
        holiday_dates.add(pd.to_datetime(value).date())
    return holiday_dates


def parse_sync_groups(df: pd.DataFrame, valid_names: Set[str]) -> Dict[str, List[str]]:
    if df.empty:
        return {}
    if "sync_group" not in df.columns:
        raise ValueError("Sync groups sheet must contain column: sync_group")

    primary_to_followers: Dict[str, List[str]] = {}
    follower_to_primary: Dict[str, str] = {}

    for _, row in df.iterrows():
        raw = str(row.get("sync_group", "")).strip()
        if not raw or raw.lower() == "nan":
            continue
        members = [name.strip() for name in raw.split(",") if name.strip()]
        # de-duplicate while preserving order
        seen = set()
        members = [m for m in members if not (m.lower() in seen or seen.add(m.lower()))]
        if len(members) < 2:
            continue
        for name in members:
            if name not in valid_names:
                raise ValueError(f"Sync group contains unknown member: {name}")
        primary = members[0]
        followers = members[1:]
        primary_to_followers.setdefault(primary, [])
        for follower in followers:
            existing_primary = follower_to_primary.get(follower)
            if existing_primary and existing_primary != primary:
                raise ValueError(f"{follower} cannot be synced to more than one primary member.")
            if follower == primary:
                continue
            follower_to_primary[follower] = primary
            if follower not in primary_to_followers[primary]:
                primary_to_followers[primary].append(follower)
    return primary_to_followers


def generate_auto_bank_holidays(start_date: date, end_date: date, count_per_month: int) -> Set[date]:
    holidays: Set[date] = set()
    if count_per_month <= 0:
        return holidays

    for _, days in month_segment_days(start_date, end_date).items():
        weekdays = [d for d in days if d.weekday() < 5]
        preferred = sorted(weekdays, key=lambda d: (abs(d.day - 15), d.day))
        holidays.update(preferred[: min(count_per_month, len(preferred))])
    return holidays


def is_working_shift(shift: str) -> bool:
    return shift in {SHIFT_MORNING, SHIFT_AFTERNOON, SHIFT_NIGHT}


def apply_night_block_offs(schedule: Dict[str, Dict[date, str]], member: str, dates: List[date], start_index: int, block_len: int):
    for j in range(start_index + block_len, min(start_index + block_len + MANDATORY_OFF_AFTER_NIGHT, len(dates))):
        dt = dates[j]
        if schedule[member].get(dt, SHIFT_UNASSIGNED) == SHIFT_UNASSIGNED:
            schedule[member][dt] = SHIFT_WEEKOFF


def compute_stats_before_day(schedule: Dict[str, Dict[date, str]], dates: List[date], day_index: int, member: str):
    prev_day = dates[day_index - 1] if day_index > 0 else None
    prev_shift = schedule[member].get(prev_day) if prev_day else None

    continuous_work = 0
    idx = day_index - 1
    while idx >= 0 and is_working_shift(schedule[member].get(dates[idx], SHIFT_UNASSIGNED)):
        continuous_work += 1
        idx -= 1

    continuous_night = 0
    idx = day_index - 1
    while idx >= 0 and schedule[member].get(dates[idx]) == SHIFT_NIGHT:
        continuous_night += 1
        idx -= 1

    month = month_key(dates[day_index])
    month_wo = sum(1 for d in dates[:day_index] if month_key(d) == month and schedule[member].get(d) == SHIFT_WEEKOFF)

    return {
        "prev_shift": prev_shift,
        "continuous_work": continuous_work,
        "continuous_night": continuous_night,
        "month_wo": month_wo,
    }


def choose_weekoff_candidate(candidates: List[str], stats_map: Dict[str, dict], target_wo: int, month_wo_count: Dict[str, int]) -> str | None:
    if not candidates:
        return None
    ranked = sorted(
        candidates,
        key=lambda n: (
            month_wo_count[n] >= target_wo,
            month_wo_count[n],
            -stats_map[n]["continuous_work"],
            n.lower(),
        ),
    )
    return ranked[0]


def choose_shift_candidates(
    candidates: List[str],
    shift: str,
    needed: int,
    stats_map: Dict[str, dict],
    shift_counts: Dict[str, Dict[str, int]],
) -> List[str]:
    def score(name: str):
        s = stats_map[name]
        continuity_bonus = -100 if s["prev_shift"] == shift else 0
        night_limit_penalty = 10_000 if shift == SHIFT_NIGHT and s["continuous_night"] >= MAX_CONTINUOUS_NIGHT else 0
        total_assigned = shift_counts[SHIFT_MORNING][name] + shift_counts[SHIFT_AFTERNOON][name] + shift_counts[SHIFT_NIGHT][name]
        return (
            night_limit_penalty,
            continuity_bonus,
            shift_counts[shift][name],
            s["continuous_work"],
            total_assigned,
            name.lower(),
        )

    eligible = []
    for name in candidates:
        s = stats_map[name]
        if s["continuous_work"] >= MAX_CONTINUOUS_WORKING_DAYS:
            continue
        if shift == SHIFT_NIGHT and s["continuous_night"] >= MAX_CONTINUOUS_NIGHT:
            continue
        eligible.append(name)

    return sorted(eligible, key=score)[:needed]


def can_assign_shift(name: str, shift: str, dt: date, schedule: Dict[str, Dict[date, str]], stats_map: Dict[str, dict], member_map: Dict[str, Member]) -> bool:
    if schedule[name][dt] != SHIFT_UNASSIGNED:
        return False
    member = member_map[name]
    if member.afternoon_only:
        if dt.weekday() >= 5:
            return False
        if shift != SHIFT_AFTERNOON:
            return False
    if stats_map[name]["continuous_work"] >= MAX_CONTINUOUS_WORKING_DAYS:
        return False
    if shift == SHIFT_NIGHT and stats_map[name]["continuous_night"] >= MAX_CONTINUOUS_NIGHT:
        return False
    return True


def assign_shift_with_sync(
    shift: str,
    selected_names: List[str],
    dt: date,
    schedule: Dict[str, Dict[date, str]],
    shift_counts: Dict[str, Dict[str, int]],
    stats_map: Dict[str, dict],
    member_map: Dict[str, Member],
    sync_groups: Dict[str, List[str]],
    warnings: List[dict],
):
    for primary in selected_names:
        if not can_assign_shift(primary, shift, dt, schedule, stats_map, member_map):
            continue
        schedule[primary][dt] = shift
        shift_counts[shift][primary] += 1
        for follower in sync_groups.get(primary, []):
            if can_assign_shift(follower, shift, dt, schedule, stats_map, member_map):
                schedule[follower][dt] = shift
                shift_counts[shift][follower] += 1
            else:
                current = schedule[follower][dt]
                reason = "rule constraint"
                if current == SHIFT_LEAVE:
                    reason = "leave"
                elif current == SHIFT_WEEKOFF:
                    reason = "week off"
                elif member_map[follower].afternoon_only and shift != SHIFT_AFTERNOON:
                    reason = "afternoon-only exception"
                warnings.append({
                    "date": dt.isoformat(),
                    "warning": f"Sync not possible: {follower} could not match {primary}'s {shift} shift due to {reason}."
                })


def style_matrix(df: pd.DataFrame, bank_holidays: Set[date]):
    date_cols = [c for c in df.columns if isinstance(c, str) and c[:4].isdigit()]

    def color_cell(val):
        for shift, code in SHIFT_CODE_MAP.items():
            if val == code:
                return f"background-color: {SHIFT_COLOR_MAP[shift]}; text-align:center;"
        return ""

    styled = df.style.map(color_cell, subset=date_cols)
    header_styles = []
    for col in df.columns:
        if col in date_cols:
            dt = datetime.strptime(col, "%Y-%m-%d").date()
            if dt in bank_holidays:
                header_styles.append(
                    {"selector": f"th.col_heading.level0.col{df.columns.get_loc(col)}", "props": "background-color: #CFE2F3;"}
                )
    if header_styles:
        styled = styled.set_table_styles(header_styles, overwrite=False)
    return styled


def to_excel_bytes(matrix_df: pd.DataFrame, full_df: pd.DataFrame, daywise_df: pd.DataFrame, summary_df: pd.DataFrame, warnings_df: pd.DataFrame, bank_holidays: Set[date]) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        matrix_df.to_excel(writer, index=False, sheet_name="ROTA Matrix")
        full_df.to_excel(writer, index=False, sheet_name="Full Shift Matrix")
        daywise_df.to_excel(writer, index=False, sheet_name="Day Wise Schedule")
        summary_df.to_excel(writer, index=False, sheet_name="Summary")
        warnings_df.to_excel(writer, index=False, sheet_name="Warnings")

        for sheet_name, df in {
            "ROTA Matrix": matrix_df,
            "Full Shift Matrix": full_df,
            "Day Wise Schedule": daywise_df,
            "Summary": summary_df,
            "Warnings": warnings_df,
        }.items():
            ws = writer.sheets[sheet_name]
            for cell in ws[1]:
                cell.fill = PatternFill(fill_type="solid", fgColor="1F4E78")
                cell.font = Font(color="FFFFFF", bold=True)
                cell.alignment = Alignment(horizontal="center", vertical="center")
            for idx, col in enumerate(df.columns, start=1):
                max_len = max(len(str(col)), *(len(str(v)) for v in df[col].fillna(""))) + 2
                ws.column_dimensions[get_column_letter(idx)].width = min(max_len, 22)

            if sheet_name in {"ROTA Matrix", "Full Shift Matrix"}:
                for col_idx, col_name in enumerate(df.columns, start=1):
                    if isinstance(col_name, str) and col_name[:4].isdigit():
                        dt = datetime.strptime(col_name, "%Y-%m-%d").date()
                        if dt in bank_holidays:
                            ws.cell(row=1, column=col_idx).fill = PatternFill(fill_type="solid", fgColor="CFE2F3")
                        for row_idx in range(2, ws.max_row + 1):
                            val = ws.cell(row=row_idx, column=col_idx).value
                            shift_name = next((k for k, v in SHIFT_CODE_MAP.items() if v == val), None)
                            if shift_name:
                                ws.cell(row=row_idx, column=col_idx).fill = PatternFill(fill_type="solid", fgColor=SHIFT_COLOR_MAP[shift_name].replace("#", ""))
                                ws.cell(row=row_idx, column=col_idx).alignment = Alignment(horizontal="center")
    return output.getvalue()


def shift_interval_for_date(shift_name: str, shift_date: date) -> Tuple[datetime, datetime] | Tuple[None, None]:
    if shift_name not in SHIFT_TIME_MAP:
        return None, None
    start_t, end_t = SHIFT_TIME_MAP[shift_name]
    start_dt = datetime.combine(shift_date, start_t)
    end_dt = datetime.combine(shift_date, end_t)
    if end_dt <= start_dt:
        end_dt += timedelta(days=1)
    return start_dt, end_dt


def compute_change_availability(full_df: pd.DataFrame, change_start: datetime, change_end: datetime, max_per_shift: int = 3) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if full_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    date_cols = [c for c in full_df.columns if isinstance(c, str) and c[:4].isdigit()]
    detail_rows = []

    for _, row in full_df.iterrows():
        name = row["name"]
        dept = row.get("dept", "")
        file_id = row.get("file_id", "")

        for col in date_cols:
            shift_name = row[col]
            if shift_name not in SHIFT_TIME_MAP:
                continue

            shift_date = datetime.strptime(col, "%Y-%m-%d").date()
            shift_start, shift_end = shift_interval_for_date(shift_name, shift_date)
            overlap_start = max(change_start, shift_start)
            overlap_end = min(change_end, shift_end)
            overlap_seconds = (overlap_end - overlap_start).total_seconds()

            if overlap_seconds > 0:
                detail_rows.append({
                    "name": name,
                    "dept": dept,
                    "file_id": file_id,
                    "rota_date": col,
                    "shift": shift_name,
                    "shift_start_gmt": shift_start.strftime("%Y-%m-%d %H:%M"),
                    "shift_end_gmt": shift_end.strftime("%Y-%m-%d %H:%M"),
                    "overlap_start_gmt": overlap_start.strftime("%Y-%m-%d %H:%M"),
                    "overlap_end_gmt": overlap_end.strftime("%Y-%m-%d %H:%M"),
                    "overlap_hours": round(overlap_seconds / 3600, 2),
                })

    details_df = pd.DataFrame(detail_rows)
    empty_summary = pd.DataFrame(columns=["name", "dept", "file_id", "shift", "rota_date", "overlap_hours", "shift_start_gmt", "shift_end_gmt"])
    if details_df.empty:
        return details_df, empty_summary

    details_df = details_df.sort_values(
        by=["rota_date", "shift", "overlap_hours", "name"],
        ascending=[True, True, False, True]
    ).copy()
    details_df["rank_within_shift"] = details_df.groupby(["rota_date", "shift"]).cumcount() + 1
    allocated_df = details_df[details_df["rank_within_shift"] <= max_per_shift].copy()

    per_shift_summary = allocated_df[[
        "name", "dept", "file_id", "shift", "rota_date", "overlap_hours", "shift_start_gmt", "shift_end_gmt"
    ]].sort_values(by=["rota_date", "shift", "overlap_hours", "name"], ascending=[True, True, False, True])

    return allocated_df.sort_values(by=["rota_date", "shift", "name"]), per_shift_summary


def generate_rota(
    members: List[Member],
    leaves: Dict[str, Set[date]],
    start_date: date,
    end_date: date,
    global_weekoffs_per_month: int,
    bank_holidays: Set[date],
    sync_groups: Dict[str, List[str]],
):
    dates = dates_in_range(start_date, end_date)
    member_names = [m.name for m in members]
    member_map = {m.name: m for m in members}
    targets = prorated_target(global_weekoffs_per_month, start_date, end_date)
    follower_to_primary = {follower: primary for primary, followers in sync_groups.items() for follower in followers}

    schedule: Dict[str, Dict[date, str]] = {m.name: {d: SHIFT_UNASSIGNED for d in dates} for m in members}
    shift_counts = {
        SHIFT_MORNING: {m.name: 0 for m in members},
        SHIFT_AFTERNOON: {m.name: 0 for m in members},
        SHIFT_NIGHT: {m.name: 0 for m in members},
    }
    warnings: List[dict] = []

    for name in member_names:
        for d in leaves.get(name, set()):
            if d in schedule[name]:
                schedule[name][d] = SHIFT_LEAVE

    for day_index, dt in enumerate(dates):
        month = month_key(dt)
        target_wo = targets[month]
        stats_map = {name: compute_stats_before_day(schedule, dates, day_index, name) for name in member_names}
        month_wo_count = {name: stats_map[name]["month_wo"] for name in member_names}

        available = []
        forced_wo = []
        for name in member_names:
            current_status = schedule[name][dt]
            if current_status in {SHIFT_LEAVE, SHIFT_WEEKOFF}:
                continue
            if member_map[name].afternoon_only and dt.weekday() >= 5:
                forced_wo.append(name)
                continue
            if stats_map[name]["continuous_work"] >= MAX_CONTINUOUS_WORKING_DAYS:
                forced_wo.append(name)
            else:
                available.append(name)

        for name in forced_wo:
            schedule[name][dt] = SHIFT_WEEKOFF

        available = [n for n in member_names if schedule[n][dt] == SHIFT_UNASSIGNED]
        minimum_needed = MIN_MORNING + MIN_AFTERNOON + MIN_NIGHT
        surplus = max(0, len(available) - minimum_needed)

        if surplus > 0:
            candidates_for_wo = [n for n in available if not member_map[n].afternoon_only and n not in follower_to_primary]
            for _ in range(surplus):
                pick = choose_weekoff_candidate(candidates_for_wo, stats_map, target_wo, month_wo_count)
                if pick is None:
                    break
                schedule[pick][dt] = SHIFT_WEEKOFF
                month_wo_count[pick] += 1
                if pick in available:
                    available.remove(pick)
                # if primary gets WO, try keeping synced followers available for staffing; sync can break if needed
                candidates_for_wo = [n for n in available if not member_map[n].afternoon_only and n not in follower_to_primary]

        available = [n for n in member_names if schedule[n][dt] == SHIFT_UNASSIGNED]
        if len(available) < minimum_needed:
            warnings.append({"date": dt.isoformat(), "warning": f"Only {len(available)} available resources. Minimum {minimum_needed} required."})

        # Forced afternoon-only members go first.
        afternoon_only_available = [n for n in available if member_map[n].afternoon_only and stats_map[n]["continuous_work"] < MAX_CONTINUOUS_WORKING_DAYS]
        assign_shift_with_sync(
            SHIFT_AFTERNOON,
            afternoon_only_available,
            dt,
            schedule,
            shift_counts,
            stats_map,
            member_map,
            sync_groups,
            warnings,
        )

        available = [n for n in member_names if schedule[n][dt] == SHIFT_UNASSIGNED]

        # Prefer primary members for initial mandatory slots so followers can sync behind them.
        night_pool = [n for n in available if n not in follower_to_primary]
        if len(night_pool) < MIN_NIGHT:
            night_pool = available
        night_selected = choose_shift_candidates(night_pool, SHIFT_NIGHT, MIN_NIGHT, stats_map, shift_counts)
        assign_shift_with_sync(SHIFT_NIGHT, night_selected, dt, schedule, shift_counts, stats_map, member_map, sync_groups, warnings)

        available = [n for n in member_names if schedule[n][dt] == SHIFT_UNASSIGNED]
        morning_pool = [n for n in available if n not in follower_to_primary]
        if len(morning_pool) < MIN_MORNING:
            morning_pool = available
        morning_selected = choose_shift_candidates(morning_pool, SHIFT_MORNING, MIN_MORNING, stats_map, shift_counts)
        assign_shift_with_sync(SHIFT_MORNING, morning_selected, dt, schedule, shift_counts, stats_map, member_map, sync_groups, warnings)

        available = [n for n in member_names if schedule[n][dt] == SHIFT_UNASSIGNED]
        current_afternoon_count = sum(schedule[n][dt] == SHIFT_AFTERNOON for n in member_names)
        afternoon_need = max(0, MIN_AFTERNOON - current_afternoon_count)
        afternoon_pool = [n for n in available if n not in follower_to_primary]
        if len(afternoon_pool) < afternoon_need:
            afternoon_pool = available
        afternoon_selected = choose_shift_candidates(afternoon_pool, SHIFT_AFTERNOON, afternoon_need, stats_map, shift_counts)
        assign_shift_with_sync(SHIFT_AFTERNOON, afternoon_selected, dt, schedule, shift_counts, stats_map, member_map, sync_groups, warnings)

        available = [n for n in member_names if schedule[n][dt] == SHIFT_UNASSIGNED]
        # Assign remaining members, preferring continuity with yesterday's shift.
        continuity_order = {SHIFT_NIGHT: 0, SHIFT_MORNING: 1, SHIFT_AFTERNOON: 2, None: 3, SHIFT_WEEKOFF: 4, SHIFT_LEAVE: 5}
        remaining_sorted = sorted(
            available,
            key=lambda n: (
                continuity_order.get(stats_map[n]["prev_shift"], 99),
                stats_map[n]["continuous_work"],
                n.lower(),
            )
        )
        for n in remaining_sorted:
            prev_shift = stats_map[n]["prev_shift"]
            preferred_shift = prev_shift if prev_shift in {SHIFT_MORNING, SHIFT_AFTERNOON, SHIFT_NIGHT} else SHIFT_AFTERNOON
            assigned = False
            for shift_option in [preferred_shift, SHIFT_AFTERNOON, SHIFT_MORNING, SHIFT_NIGHT]:
                if can_assign_shift(n, shift_option, dt, schedule, stats_map, member_map):
                    assign_shift_with_sync(shift_option, [n], dt, schedule, shift_counts, stats_map, member_map, sync_groups, warnings)
                    assigned = True
                    break
            if not assigned and schedule[n][dt] == SHIFT_UNASSIGNED:
                schedule[n][dt] = SHIFT_WEEKOFF
                month_wo_count[n] += 1

        # Final sweep: any truly unassigned person becomes WO.
        for n in member_names:
            if schedule[n][dt] == SHIFT_UNASSIGNED:
                schedule[n][dt] = SHIFT_WEEKOFF
                month_wo_count[n] += 1

        day_counts = {
            SHIFT_MORNING: sum(schedule[n][dt] == SHIFT_MORNING for n in member_names),
            SHIFT_AFTERNOON: sum(schedule[n][dt] == SHIFT_AFTERNOON for n in member_names),
            SHIFT_NIGHT: sum(schedule[n][dt] == SHIFT_NIGHT for n in member_names),
        }
        for shift_name, minimum in [(SHIFT_MORNING, MIN_MORNING), (SHIFT_AFTERNOON, MIN_AFTERNOON), (SHIFT_NIGHT, MIN_NIGHT)]:
            if day_counts[shift_name] < minimum:
                warnings.append({"date": dt.isoformat(), "warning": f"{shift_name}: assigned {day_counts[shift_name]} < required {minimum}"})

        # Mark mandatory WO after a maxed-out night streak.
        for n in member_names:
            if schedule[n][dt] == SHIFT_NIGHT:
                current_night_streak = 0
                idx = day_index
                while idx >= 0 and schedule[n][dates[idx]] == SHIFT_NIGHT:
                    current_night_streak += 1
                    idx -= 1
                if current_night_streak >= MAX_CONTINUOUS_NIGHT:
                    apply_night_block_offs(schedule, n, dates, day_index, 1)

        # If yesterday ended a night block, mark next 2 days WO.
        if day_index > 0:
            y = dates[day_index - 1]
            for n in member_names:
                if schedule[n][y] == SHIFT_NIGHT and schedule[n][dt] != SHIFT_NIGHT:
                    apply_night_block_offs(schedule, n, dates, day_index - 1, 1)

    for n in member_names:
        idx = 0
        while idx < len(dates):
            if schedule[n][dates[idx]] == SHIFT_NIGHT:
                while idx + 1 < len(dates) and schedule[n][dates[idx + 1]] == SHIFT_NIGHT:
                    idx += 1
                end_idx = idx
                for j in range(end_idx + 1, min(end_idx + 1 + MANDATORY_OFF_AFTER_NIGHT, len(dates))):
                    if schedule[n][dates[j]] == SHIFT_UNASSIGNED:
                        schedule[n][dates[j]] = SHIFT_WEEKOFF
                idx += 1
            else:
                idx += 1

    matrix_rows, full_rows, daywise_rows, summary_rows = [], [], [], []
    sync_map_text = {m.name: ", ".join(sync_groups.get(m.name, [])) for m in members}
    primary_text = {m.name: follower_to_primary.get(m.name, "") for m in members}
    for m in members:
        row_codes = {"name": m.name, "dept": m.dept, "file_id": m.file_id, "primary_for": sync_map_text[m.name], "synced_to": primary_text[m.name]}
        row_full = {"name": m.name, "dept": m.dept, "file_id": m.file_id, "primary_for": sync_map_text[m.name], "synced_to": primary_text[m.name]}
        for dt in dates:
            col = dt.isoformat()
            shift = schedule[m.name][dt]
            row_codes[col] = SHIFT_CODE_MAP[shift]
            row_full[col] = shift
        matrix_rows.append(row_codes)
        full_rows.append(row_full)

    for dt in dates:
        counts = {s: [] for s in [SHIFT_MORNING, SHIFT_AFTERNOON, SHIFT_NIGHT, SHIFT_WEEKOFF, SHIFT_LEAVE]}
        for n in member_names:
            counts[schedule[n][dt]].append(n)
        daywise_rows.append({
            "date": dt.isoformat(),
            "day": dt.strftime("%A"),
            "bank_holiday": "Yes" if dt in bank_holidays else "No",
            "morning_count": len(counts[SHIFT_MORNING]),
            "afternoon_count": len(counts[SHIFT_AFTERNOON]),
            "night_count": len(counts[SHIFT_NIGHT]),
            "weekoff_count": len(counts[SHIFT_WEEKOFF]),
            "leave_count": len(counts[SHIFT_LEAVE]),
            "morning_names": ", ".join(counts[SHIFT_MORNING]),
            "afternoon_names": ", ".join(counts[SHIFT_AFTERNOON]),
            "night_names": ", ".join(counts[SHIFT_NIGHT]),
            "weekoff_names": ", ".join(counts[SHIFT_WEEKOFF]),
            "leave_names": ", ".join(counts[SHIFT_LEAVE]),
            "status": "OK" if len(counts[SHIFT_MORNING]) >= MIN_MORNING and len(counts[SHIFT_AFTERNOON]) >= MIN_AFTERNOON and len(counts[SHIFT_NIGHT]) >= MIN_NIGHT else "Warning",
        })

    target_total = sum(prorated_target(global_weekoffs_per_month, start_date, end_date).values())
    for m in members:
        summary_rows.append({
            "name": m.name,
            "dept": m.dept,
            "file_id": m.file_id,
            "synced_to": primary_text[m.name],
            "primary_for": sync_map_text[m.name],
            "morning_shifts": sum(1 for dt in dates if schedule[m.name][dt] == SHIFT_MORNING),
            "afternoon_shifts": sum(1 for dt in dates if schedule[m.name][dt] == SHIFT_AFTERNOON),
            "night_shifts": sum(1 for dt in dates if schedule[m.name][dt] == SHIFT_NIGHT),
            "weekoffs_used": sum(1 for dt in dates if schedule[m.name][dt] == SHIFT_WEEKOFF),
            "weekoffs_target": target_total,
            "leave_days": sum(1 for dt in dates if schedule[m.name][dt] == SHIFT_LEAVE),
            "working_days": sum(1 for dt in dates if is_working_shift(schedule[m.name][dt])),
        })

    return (
        pd.DataFrame(matrix_rows),
        pd.DataFrame(full_rows),
        pd.DataFrame(daywise_rows),
        pd.DataFrame(summary_rows),
        pd.DataFrame(warnings or [{"date": "", "warning": "No warnings"}]),
    )



# -----------------------------
# Access control
# -----------------------------
AUTH_USERS = {
    "admin": {"password": "admin123", "role": "admin"},
    "dev": {"password": "dev123", "role": "dev"},
}

def init_auth_state():
    st.session_state.setdefault("auth_role", "general")
    st.session_state.setdefault("auth_user", "General User")
    st.session_state.setdefault("auth_logged_in", False)

def login_user(username: str, password: str) -> bool:
    user = AUTH_USERS.get(username.strip())
    if user and password == user["password"]:
        st.session_state["auth_role"] = user["role"]
        st.session_state["auth_user"] = username.strip()
        st.session_state["auth_logged_in"] = True
        return True
    return False

def logout_user():
    st.session_state["auth_role"] = "general"
    st.session_state["auth_user"] = "General User"
    st.session_state["auth_logged_in"] = False


# -----------------------------
# UI
# -----------------------------
st.set_page_config(page_title="ROTA Generator", layout="wide")
init_auth_state()

st.title("Team ROTA Generator")
st.caption("ROTA is saved after generation. Admin and Dev can manage and generate rota. Any user can use Change Support Availability once a rota is already generated. You can also sync 2 or more members by listing them in order, with the first member treated as the primary shift owner.")

team_default_df, leaves_default_df, bank_holidays_default_df, sync_groups_default_df = load_inputs()
saved_rota = load_saved_rota()

leaves_default_df = ensure_date_columns(leaves_default_df, ["leave_start_date", "leave_end_date"])
bank_holidays_default_df = ensure_date_columns(bank_holidays_default_df, ["bank_holiday_date"])

with st.sidebar:
    st.header("Access")
    access_mode = st.radio("Open as", ["General User", "Admin / Dev Login"], key="access_mode")

    if access_mode == "Admin / Dev Login":
        username = st.text_input("Username", key="login_username")
        password = st.text_input("Password", type="password", key="login_password")
        login_clicked = st.button("Login", use_container_width=True)
        if login_clicked:
            if login_user(username, password):
                st.success(f"Logged in as {st.session_state['auth_role'].title()}.")
            else:
                st.error("Invalid username or password.")
        if st.session_state.get("auth_logged_in"):
            st.info(f"Current access: {st.session_state['auth_role'].title()} ({st.session_state['auth_user']})")
            if st.button("Logout", use_container_width=True):
                logout_user()
                st.success("Logged out.")
    else:
        if st.session_state.get("auth_logged_in"):
            st.info(f"Current access: {st.session_state['auth_role'].title()} ({st.session_state['auth_user']})")
            if st.button("Logout", use_container_width=True):
                logout_user()
                st.success("Logged out.")
        st.caption("General users can view change-support availability from the last saved rota.")

    can_manage = st.session_state.get("auth_role") in {"admin", "dev"}

    st.divider()
    st.header("Schedule Setup")
    start_date = st.date_input("Start date", value=date.today(), disabled=not can_manage)
    end_date = st.date_input("End date", value=date.today() + timedelta(days=13), disabled=not can_manage)
    weekoffs_per_month = st.number_input(
        "Total week offs per member per month",
        min_value=0,
        max_value=15,
        value=8,
        step=1,
        disabled=not can_manage,
    )
    st.markdown("**Mandatory daily staffing:** 2 Morning, 2 Night, 2 Afternoon")
    st.markdown("**Rules:** max 5 continuous Night shifts, 2 compulsory WO after Night block, max 6 continuous working days")
    st.divider()
    st.caption(f"Database storage: {DB_FILE.name}")
    st.caption("Legacy JSON saves are imported automatically if they already exist.")

rota_bundle = None

if can_manage:
    st.subheader("1) Team Members")
    st.caption("Entered team details are stored locally unless you edit, add, remove, or reset them.")
    team_df = st.data_editor(
        team_default_df,
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            "name": st.column_config.TextColumn("Name"),
            "dept": st.column_config.TextColumn("Dept."),
            "file_id": st.column_config.TextColumn("File Id"),
            "afternoon_only": st.column_config.SelectboxColumn("Afternoon-only exception", options=["Yes", "No"]),
        },
        key="team_editor",
    )

    row1, row2 = st.columns(2)
    with row1:
        if st.button("Save Team Details", use_container_width=True):
            try:
                save_inputs(team_df, leaves_default_df, bank_holidays_default_df, sync_groups_default_df)
                st.success("Team details saved to the database.")
            except Exception as e:
                st.error(f"Could not save team details: {e}")
    with row2:
        if st.button("Reset Saved Data", use_container_width=True):
            delete_state(STATE_KEY_INPUTS)
            if DATA_FILE.exists():
                DATA_FILE.unlink()
            st.success("Saved input data cleared from the database. Refresh the app.")

    st.subheader("2) Leaves")
    st.caption("Enter one row per leave range.")
    leaves_df = st.data_editor(
        leaves_default_df,
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            "name": st.column_config.TextColumn("Name"),
            "leave_start_date": st.column_config.DateColumn("Leave start date"),
            "leave_end_date": st.column_config.DateColumn("Leave end date"),
        },
        key="leave_editor",
    )

    st.subheader("3) Bank Holidays")
    bh_mode = st.radio(
        "Select bank holiday input mode",
        ["No bank holidays", "By number of days", "By specific dates", "Both"],
        horizontal=True,
    )
    auto_bh_days = 0
    if bh_mode in {"By number of days", "Both"}:
        auto_bh_days = st.number_input("Number of bank holidays per month", min_value=0, max_value=10, value=1, step=1)

    specific_bh_df = bank_holidays_default_df.copy()
    if bh_mode in {"By specific dates", "Both"}:
        specific_bh_df = st.data_editor(
            bank_holidays_default_df,
            num_rows="dynamic",
            use_container_width=True,
            column_config={"bank_holiday_date": st.column_config.DateColumn("Bank holiday date")},
            key="bh_editor",
        )

    st.subheader("4) Shift Sync Groups")
    st.caption("Enter comma-separated member names in order. The first member is treated as the primary shift member, and the remaining members will follow the same shift whenever possible. Example: Aarav, Bhavna, Divya")
    sync_groups_df = st.data_editor(
        sync_groups_default_df,
        num_rows="dynamic",
        use_container_width=True,
        column_config={"sync_group": st.column_config.TextColumn("Sync group (primary first)")},
        key="sync_groups_editor",
    )

    col1, col2, col3 = st.columns(3)
    with col1:
        save_all = st.button("Save All Inputs", use_container_width=True)
    with col2:
        generate = st.button("Generate ROTA", type="primary", use_container_width=True)
    with col3:
        st.download_button(
            "Download Team Template CSV",
            data=sample_team_df().to_csv(index=False).encode("utf-8"),
            file_name="team_template.csv",
            mime="text/csv",
            use_container_width=True,
        )

    if save_all:
        try:
            save_inputs(team_df, leaves_df, specific_bh_df, sync_groups_df)
            st.success("Inputs saved to the database.")
        except Exception as e:
            st.error(f"Could not save inputs: {e}")

    if generate:
        try:
            if end_date < start_date:
                st.error("End date must be on or after start date.")
                st.stop()

            members = parse_members(team_df)
            valid_names = {m.name for m in members}
            leaves_map = parse_leaves(leaves_df, valid_names)
            sync_groups = parse_sync_groups(sync_groups_df, valid_names)
            auto_holidays = generate_auto_bank_holidays(start_date, end_date, int(auto_bh_days))
            specific_holidays = parse_specific_bank_holidays(specific_bh_df) if bh_mode in {"By specific dates", "Both"} else set()
            bank_holidays = auto_holidays | specific_holidays

            save_inputs(team_df, leaves_df, specific_bh_df, sync_groups_df)

            matrix_df, full_df, daywise_df, summary_df, warnings_df = generate_rota(
                members=members,
                leaves=leaves_map,
                start_date=start_date,
                end_date=end_date,
                global_weekoffs_per_month=int(weekoffs_per_month),
                bank_holidays=bank_holidays,
                sync_groups=sync_groups,
            )
            excel_bytes = to_excel_bytes(matrix_df, full_df, daywise_df, summary_df, warnings_df, bank_holidays)
            save_generated_rota(matrix_df, full_df, daywise_df, summary_df, warnings_df, bank_holidays, start_date, end_date, excel_bytes)

            rota_bundle = {
                "metadata": {
                    "saved_at": datetime.utcnow().isoformat(timespec="seconds"),
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                },
                "matrix_df": matrix_df,
                "full_df": full_df,
                "daywise_df": daywise_df,
                "summary_df": summary_df,
                "warnings_df": warnings_df,
                "bank_holidays": bank_holidays,
                "excel_bytes": excel_bytes,
            }
            st.session_state["rota_bundle"] = rota_bundle
            st.success("ROTA generated and saved to the database.")
        except Exception as e:
            st.error(str(e))

else:
    st.info("General user mode: rota management is restricted. You can view the last saved rota and use change-support allocation.")

if rota_bundle is None:
    rota_bundle = st.session_state.get("rota_bundle")

if rota_bundle is None and saved_rota is not None:
    try:
        excel_bytes = ROTA_EXCEL_FILE.read_bytes() if ROTA_EXCEL_FILE.exists() else to_excel_bytes(
            saved_rota["matrix_df"], saved_rota["full_df"], saved_rota["daywise_df"], saved_rota["summary_df"], saved_rota["warnings_df"], saved_rota["bank_holidays"]
        )
        rota_bundle = {
            "metadata": saved_rota["metadata"],
            "matrix_df": saved_rota["matrix_df"],
            "full_df": saved_rota["full_df"],
            "daywise_df": saved_rota["daywise_df"],
            "summary_df": saved_rota["summary_df"],
            "warnings_df": saved_rota["warnings_df"],
            "bank_holidays": saved_rota["bank_holidays"],
            "excel_bytes": excel_bytes,
        }
        st.info(f"Loaded saved rota from the database. Saved at {saved_rota['metadata'].get('saved_at', 'previous run')} UTC.")
    except Exception as e:
        st.warning(f"Could not load saved rota: {e}")

if rota_bundle is not None:
    matrix_df = rota_bundle["matrix_df"]
    full_df = rota_bundle["full_df"]
    daywise_df = rota_bundle["daywise_df"]
    summary_df = rota_bundle["summary_df"]
    warnings_df = rota_bundle["warnings_df"]
    bank_holidays = rota_bundle["bank_holidays"]
    excel_bytes = rota_bundle["excel_bytes"]
    csv_bytes = matrix_df.to_csv(index=False).encode("utf-8")

    if bank_holidays:
        st.info("Bank holiday dates are highlighted in the matrix headers. Staffing rules still remain active on those days.")

    tabs = st.tabs(["ROTA Matrix", "Full Shift Names", "Day Wise Schedule", "Summary", "Warnings", "Change Support Availability"])

    change_tab = tabs[-1]

    with tabs[0]:
        st.dataframe(style_matrix(matrix_df, bank_holidays), use_container_width=True, hide_index=True)

    with tabs[1]:
        st.dataframe(full_df, use_container_width=True, hide_index=True)

    with tabs[2]:
        st.dataframe(daywise_df, use_container_width=True, hide_index=True)

    with tabs[3]:
        st.dataframe(summary_df, use_container_width=True, hide_index=True)

    with tabs[4]:
        st.dataframe(warnings_df, use_container_width=True, hide_index=True)

    with change_tab:
        st.markdown("### Change Support Availability")
        st.caption("Select the change start and end in GMT. This tab shows rota-assigned resources whose shift overlaps the selected change window. A maximum of 3 resources per shift/date is allocated for a change.")

        col_a, col_b = st.columns(2)
        default_start = date.fromisoformat(rota_bundle["metadata"].get("start_date", date.today().isoformat()))
        default_end = date.fromisoformat(rota_bundle["metadata"].get("end_date", date.today().isoformat()))

        with col_a:
            chg_start_date = st.date_input("Change start date (GMT)", value=default_start, key="chg_start_date")
            chg_start_time = st.time_input("Change start time (GMT)", value=time(1, 0), key="chg_start_time")
        with col_b:
            chg_end_date = st.date_input("Change end date (GMT)", value=default_end, key="chg_end_date")
            chg_end_time = st.time_input("Change end time (GMT)", value=time(10, 30), key="chg_end_time")

        change_start = datetime.combine(chg_start_date, chg_start_time)
        change_end = datetime.combine(chg_end_date, chg_end_time)

        if change_end <= change_start:
            st.error("Change end datetime must be after start datetime.")
        else:
            detail_df, avail_summary_df = compute_change_availability(full_df, change_start, change_end, max_per_shift=3)

            top1, top2, top3 = st.columns(3)
            with top1:
                st.metric("Allocated resources", int(avail_summary_df.shape[0]))
            with top2:
                st.metric("Covering shift rows", int(detail_df.shape[0]))
            with top3:
                st.metric("Change duration (hrs)", round((change_end - change_start).total_seconds() / 3600, 2))

            st.markdown("#### Allocated resources by shift/date (max 3 per shift)")
            st.dataframe(avail_summary_df, use_container_width=True, hide_index=True)

            st.markdown("#### Detailed overlap by rota date and shift")
            st.dataframe(detail_df, use_container_width=True, hide_index=True)

            if not avail_summary_df.empty:
                change_csv = avail_summary_df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "Download available resources CSV",
                    data=change_csv,
                    file_name="change_allocated_resources.csv",
                    mime="text/csv",
                    use_container_width=True,
                )

        st.markdown("#### Shift timing reference (GMT)")
        st.dataframe(
            pd.DataFrame([
                {"shift": "Morning", "start_gmt": "01:00", "end_gmt": "10:30"},
                {"shift": "Afternoon", "start_gmt": "07:30", "end_gmt": "17:00"},
                {"shift": "Night", "start_gmt": "15:30", "end_gmt": "01:00 next day"},
            ]),
            use_container_width=True,
            hide_index=True,
        )

    if can_manage:
        d1, d2 = st.columns(2)
        with d1:
            st.download_button(
                "Download Excel",
                data=excel_bytes,
                file_name="rota_output.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        with d2:
            st.download_button(
                "Download Matrix CSV",
                data=csv_bytes,
                file_name="rota_matrix.csv",
                mime="text/csv",
                use_container_width=True,
            )
else:
    if can_manage:
        st.info("Generate a rota to see the rota tabs and change-support availability.")
    else:
        st.warning("No saved rota found yet. Ask an Admin or Dev to generate the rota first.")
