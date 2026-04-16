from __future__ import annotations

import html
import hashlib
import hmac
import json
import os
import smtplib
import sqlite3
from contextlib import closing
from datetime import date, datetime
from email.message import EmailMessage
from pathlib import Path
from typing import Any

import altair as alt
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DB_FILE = DATA_DIR / "expense_tracker.db"
LEGACY_TRANSACTIONS_FILE = DATA_DIR / "transactions.csv"
LEGACY_CATEGORIES_FILE = DATA_DIR / "categories.json"

TYPE_LABELS = {"expense": "Expense", "credit": "Credit"}
ADD_CUSTOM_OPTION = "Add a new category..."
ADMIN_NOTIFICATION_EMAILS = ["sricharan2209@gmail.com"]
DEV_ACCOUNT_EMAIL = os.getenv("DEV_ACCOUNT_EMAIL", "developer@local.app")
DEV_ACCOUNT_USERNAME = os.getenv("DEV_ACCOUNT_USERNAME", "developer")
DEV_ACCOUNT_PASSWORD = os.getenv("DEV_ACCOUNT_PASSWORD", "developer123")
DASHBOARD_SECTION_KEY = "dashboard_section"
ADMIN_SECTION_KEY = "admin_section"
DEFAULT_CATEGORIES = {
    "expense": [
        "Transport",
        "Petrol",
        "Food",
        "Clothing",
        "Shopping",
        "Home",
        "Home Office",
        "Office",
        "Relatives",
        "Groceries",
        "Utilities",
        "Medical",
        "Entertainment",
        "Travel",
        "Education",
        "Subscriptions",
        "Bills",
        "EMI",
        "Other Expense",
    ],
    "credit": [
        "Salary Credit",
        "Bonus",
        "Refund",
        "Freelance",
        "Interest",
        "Investment Return",
        "Gift Received",
        "Rental Income",
        "Reimbursement",
        "Other Credit",
    ],
}


def apply_custom_styles() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background: linear-gradient(180deg, #f7fafc 0%, #edf4fb 100%);
        }
        ::selection {
            background: #16a34a;
            color: #ffffff;
        }
        ::-moz-selection {
            background: #16a34a;
            color: #ffffff;
        }
        @media (prefers-color-scheme: dark) {
            .stApp {
                background: #000000 !important;
            }
            [data-testid="stAppViewContainer"] {
                background: #000000 !important;
            }
            [data-testid="stHeader"] {
                background: rgba(0, 0, 0, 0.92) !important;
            }
            .section-hint,
            .insight-card,
            .tx-card,
            div[data-testid="stForm"],
            div[data-testid="stExpander"] details,
            [data-testid="stMetric"] {
                background: #0b0b0b !important;
                border-color: #222222 !important;
                color: #f5f5f5 !important;
                box-shadow: none !important;
            }
            .muted-copy,
            .insight-label,
            .tx-meta {
                color: #b3b3b3 !important;
            }
            .insight-value,
            .tx-title,
            .stMarkdown,
            label,
            p,
            h1,
            h2,
            h3 {
                color: #f5f5f5 !important;
            }
            .chip {
                background: #111111 !important;
                color: #7dd3fc !important;
                border-color: #1f2937 !important;
            }
            [data-baseweb="tab"] {
                background: #111111 !important;
                color: #e5e7eb !important;
            }
            [data-baseweb="tab"][aria-selected="true"] {
                background: #16a34a !important;
                color: #ffffff !important;
            }
            div.stButton > button,
            div.stDownloadButton > button {
                background: #111111 !important;
                color: #f5f5f5 !important;
                border-color: #2a2a2a !important;
            }
        }
        .block-container {
            max-width: 1080px;
            padding-top: 1rem;
            padding-bottom: 4rem;
            padding-left: 1rem;
            padding-right: 1rem;
        }
        @media (max-width: 768px) {
            .block-container {
                padding-top: 0.75rem;
                padding-left: 0.8rem;
                padding-right: 0.8rem;
            }
        }
        [data-testid="stMetric"] {
            background: rgba(255, 255, 255, 0.95);
            border: 1px solid #d9e4f0;
            border-radius: 18px;
            padding: 0.95rem;
            box-shadow: 0 10px 28px rgba(15, 23, 42, 0.06);
        }
        div.stButton > button,
        div.stDownloadButton > button {
            min-height: 3rem;
            border-radius: 14px;
            font-weight: 600;
            border: 1px solid #cfd9e8;
        }
        [data-baseweb="tab-list"] {
            gap: 0.5rem;
            flex-wrap: wrap;
        }
        [data-baseweb="tab"] {
            height: auto;
            padding: 0.5rem 1rem;
            border-radius: 999px;
            background: #e8eff7;
        }
        [data-baseweb="tab"][aria-selected="true"] {
            background: #16a34a;
            color: white;
        }
        input[type="checkbox"] {
            accent-color: #16a34a;
        }
        div[data-testid="stForm"] {
            background: rgba(255, 255, 255, 0.92);
            border: 1px solid #dbe5ef;
            border-radius: 18px;
            padding: 1rem;
        }
        div[data-testid="stExpander"] details {
            border: 1px solid #dbe5ef;
            border-radius: 18px;
            background: rgba(255, 255, 255, 0.92);
        }
        .hero-card {
            background: linear-gradient(135deg, #0f172a 0%, #0f766e 100%);
            color: white;
            border-radius: 24px;
            padding: 1.25rem;
            margin-bottom: 1rem;
            box-shadow: 0 18px 40px rgba(15, 23, 42, 0.18);
        }
        .hero-eyebrow {
            text-transform: uppercase;
            letter-spacing: 0.12em;
            font-size: 0.78rem;
            opacity: 0.8;
            margin-bottom: 0.35rem;
        }
        .hero-title {
            font-size: 1.9rem;
            line-height: 1.1;
            font-weight: 700;
            margin: 0;
        }
        .hero-copy {
            margin-top: 0.55rem;
            color: rgba(255, 255, 255, 0.86);
            max-width: 38rem;
        }
        .hero-stats {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 0.75rem;
            margin-top: 1rem;
        }
        .hero-stat {
            background: rgba(255, 255, 255, 0.12);
            border: 1px solid rgba(255, 255, 255, 0.14);
            border-radius: 18px;
            padding: 0.85rem;
        }
        .hero-stat-label {
            font-size: 0.82rem;
            opacity: 0.82;
        }
        .hero-stat-value {
            font-size: 1.1rem;
            font-weight: 700;
            margin-top: 0.2rem;
        }
        .chip-row {
            display: flex;
            flex-wrap: wrap;
            gap: 0.45rem;
            margin-top: 0.7rem;
        }
        .chip {
            padding: 0.28rem 0.7rem;
            border-radius: 999px;
            background: #eef8f7;
            color: #0f766e;
            border: 1px solid #cde8e3;
            font-size: 0.85rem;
        }
        .insight-card,
        .tx-card {
            background: rgba(255, 255, 255, 0.96);
            border: 1px solid #dbe5ef;
            border-radius: 18px;
            padding: 0.95rem;
            box-shadow: 0 10px 26px rgba(15, 23, 42, 0.05);
        }
        .insight-card {
            height: 100%;
        }
        .insight-label,
        .tx-meta,
        .muted-copy {
            color: #526071;
            font-size: 0.92rem;
        }
        .insight-value {
            font-size: 1.05rem;
            font-weight: 700;
            color: #0f172a;
            margin-top: 0.2rem;
        }
        .tx-top {
            display: flex;
            justify-content: space-between;
            gap: 0.8rem;
            align-items: flex-start;
            margin-bottom: 0.4rem;
        }
        .tx-title {
            font-weight: 700;
            color: #0f172a;
        }
        .tx-amount {
            font-weight: 700;
            white-space: nowrap;
        }
        .tx-amount.expense {
            color: #b42318;
        }
        .tx-amount.credit {
            color: #0f766e;
        }
        .section-hint {
            background: rgba(255, 255, 255, 0.82);
            border: 1px solid #dbe5ef;
            border-radius: 16px;
            padding: 0.85rem 1rem;
            margin-bottom: 1rem;
            color: #334155;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def normalize_category(name: str) -> str:
    return " ".join(name.strip().split()).title()


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def verify_password(password: str, password_hash: str) -> bool:
    return hmac.compare_digest(hash_password(password), password_hash)


def get_connection() -> sqlite3.Connection:
    DATA_DIR.mkdir(exist_ok=True)
    connection = sqlite3.connect(DB_FILE, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    return connection


def table_has_column(connection: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(row["name"] == column_name for row in rows)


def init_db() -> None:
    with closing(get_connection()) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                full_name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'user',
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                last_recap_month TEXT
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                transaction_type TEXT NOT NULL,
                name TEXT NOT NULL,
                is_default INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                UNIQUE(user_id, transaction_type, name),
                FOREIGN KEY(user_id) REFERENCES users(id)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                transaction_date TEXT NOT NULL,
                transaction_type TEXT NOT NULL,
                category TEXT NOT NULL,
                amount REAL NOT NULL,
                notes TEXT DEFAULT '',
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                recipient_email TEXT NOT NULL,
                subject TEXT NOT NULL,
                body TEXT NOT NULL,
                status TEXT NOT NULL,
                error_message TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS app_settings (
                setting_key TEXT PRIMARY KEY,
                setting_value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

        if not table_has_column(connection, "users", "last_login_at"):
            connection.execute("ALTER TABLE users ADD COLUMN last_login_at TEXT")

        connection.commit()

    ensure_developer_account()
    migrate_legacy_files()


def ensure_developer_account() -> None:
    now = datetime.now().isoformat()
    with closing(get_connection()) as connection:
        existing = connection.execute(
            "SELECT id FROM users WHERE username = ?",
            (DEV_ACCOUNT_USERNAME,),
        ).fetchone()
        if not existing:
            connection.execute(
                """
                INSERT INTO users (username, full_name, email, password_hash, role, is_active, created_at)
                VALUES (?, ?, ?, ?, 'developer', 1, ?)
                """,
                (
                    DEV_ACCOUNT_USERNAME,
                    "Developer Account",
                    DEV_ACCOUNT_EMAIL,
                    hash_password(DEV_ACCOUNT_PASSWORD),
                    now,
                ),
            )
            connection.commit()

    dev_user = get_user_by_username(DEV_ACCOUNT_USERNAME)
    if dev_user:
        ensure_default_categories(dev_user["id"])


def migrate_legacy_files() -> None:
    if not LEGACY_TRANSACTIONS_FILE.exists() and not LEGACY_CATEGORIES_FILE.exists():
        return

    dev_user = get_user_by_username(DEV_ACCOUNT_USERNAME)
    if not dev_user:
        return

    with closing(get_connection()) as connection:
        existing_count = connection.execute(
            "SELECT COUNT(*) AS count FROM transactions WHERE user_id = ?",
            (dev_user["id"],),
        ).fetchone()["count"]

    if existing_count == 0 and LEGACY_TRANSACTIONS_FILE.exists():
        try:
            legacy_transactions = pd.read_csv(LEGACY_TRANSACTIONS_FILE)
        except Exception:
            legacy_transactions = pd.DataFrame()

        if not legacy_transactions.empty:
            legacy_transactions = legacy_transactions.fillna("")
            with closing(get_connection()) as connection:
                for _, row in legacy_transactions.iterrows():
                    transaction_date = pd.to_datetime(row.get("date"), errors="coerce")
                    transaction_type = str(row.get("type", "")).strip().lower()
                    category = normalize_category(str(row.get("category", "")).strip())
                    amount = pd.to_numeric(row.get("amount"), errors="coerce")
                    notes = str(row.get("notes", "")).strip()

                    if pd.isna(transaction_date) or transaction_type not in TYPE_LABELS:
                        continue
                    if pd.isna(amount):
                        continue

                    connection.execute(
                        """
                        INSERT INTO transactions
                        (user_id, transaction_date, transaction_type, category, amount, notes, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            dev_user["id"],
                            transaction_date.strftime("%Y-%m-%d"),
                            transaction_type,
                            category or "Other Expense",
                            float(amount),
                            notes,
                            datetime.now().isoformat(),
                        ),
                    )
                connection.commit()

    if LEGACY_CATEGORIES_FILE.exists():
        try:
            raw_categories = json.loads(LEGACY_CATEGORIES_FILE.read_text(encoding="utf-8"))
        except Exception:
            raw_categories = {}
        for transaction_type, names in raw_categories.items():
            for name in names:
                add_category_for_user(dev_user["id"], transaction_type, str(name), is_default=False)


def get_user_by_username(username: str) -> dict[str, Any] | None:
    with closing(get_connection()) as connection:
        row = connection.execute(
            "SELECT * FROM users WHERE lower(username) = lower(?)",
            (username,),
        ).fetchone()
    return dict(row) if row else None


def get_user_by_id(user_id: int) -> dict[str, Any] | None:
    with closing(get_connection()) as connection:
        row = connection.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    return dict(row) if row else None


def list_users() -> pd.DataFrame:
    with closing(get_connection()) as connection:
        rows = connection.execute(
            """
            SELECT id, username, full_name, email, role, is_active, created_at, last_login_at, last_recap_month
            FROM users
            ORDER BY created_at DESC
            """
        ).fetchall()
    return pd.DataFrame([dict(row) for row in rows])


def save_setting(setting_key: str, setting_value: str) -> None:
    with closing(get_connection()) as connection:
        connection.execute(
            """
            INSERT INTO app_settings (setting_key, setting_value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(setting_key) DO UPDATE SET
                setting_value = excluded.setting_value,
                updated_at = excluded.updated_at
            """,
            (setting_key, setting_value, datetime.now().isoformat()),
        )
        connection.commit()


def get_setting(setting_key: str, default: str = "") -> str:
    with closing(get_connection()) as connection:
        row = connection.execute(
            "SELECT setting_value FROM app_settings WHERE setting_key = ?",
            (setting_key,),
        ).fetchone()
    return row["setting_value"] if row else default


def get_smtp_config() -> dict[str, str]:
    return {
        "host": os.getenv("SMTP_HOST") or get_setting("smtp_host"),
        "port": os.getenv("SMTP_PORT") or get_setting("smtp_port", "587"),
        "username": os.getenv("SMTP_USERNAME") or get_setting("smtp_username"),
        "password": os.getenv("SMTP_PASSWORD") or get_setting("smtp_password"),
        "from_email": os.getenv("SMTP_FROM_EMAIL") or get_setting("smtp_from_email"),
    }


def ensure_default_categories(user_id: int) -> None:
    now = datetime.now().isoformat()
    with closing(get_connection()) as connection:
        for transaction_type, categories in DEFAULT_CATEGORIES.items():
            for name in categories:
                normalized = normalize_category(name)
                connection.execute(
                    """
                    INSERT OR IGNORE INTO categories
                    (user_id, transaction_type, name, is_default, created_at)
                    VALUES (?, ?, ?, 1, ?)
                    """,
                    (user_id, transaction_type, normalized, now),
                )
        connection.commit()


def get_categories_for_user(user_id: int) -> dict[str, list[str]]:
    ensure_default_categories(user_id)
    with closing(get_connection()) as connection:
        rows = connection.execute(
            """
            SELECT transaction_type, name
            FROM categories
            WHERE user_id = ?
            ORDER BY is_default DESC, name ASC
            """,
            (user_id,),
        ).fetchall()

    categories = {transaction_type: [] for transaction_type in TYPE_LABELS}
    for row in rows:
        categories[row["transaction_type"]].append(row["name"])
    return categories


def add_category_for_user(
    user_id: int, transaction_type: str, category_name: str, is_default: bool = False
) -> str:
    normalized = normalize_category(category_name)
    if not normalized or transaction_type not in TYPE_LABELS:
        return normalized

    with closing(get_connection()) as connection:
        connection.execute(
            """
            INSERT OR IGNORE INTO categories
            (user_id, transaction_type, name, is_default, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                user_id,
                transaction_type,
                normalized,
                1 if is_default else 0,
                datetime.now().isoformat(),
            ),
        )
        connection.commit()
    return normalized


def create_user(full_name: str, username: str, email: str, password: str) -> tuple[bool, str]:
    username = username.strip().lower()
    full_name = full_name.strip()
    email = email.strip().lower()

    if not full_name or not username or not email or not password:
        return False, "Please fill in all required fields."
    if "@" not in email:
        return False, "Please enter a valid email address."

    now = datetime.now().isoformat()
    try:
        with closing(get_connection()) as connection:
            connection.execute(
                """
                INSERT INTO users (username, full_name, email, password_hash, role, is_active, created_at)
                VALUES (?, ?, ?, ?, 'user', 1, ?)
                """,
                (username, full_name, email, hash_password(password), now),
            )
            connection.commit()
    except sqlite3.IntegrityError:
        return False, "Username or email already exists."

    user = get_user_by_username(username)
    if user:
        ensure_default_categories(user["id"])
        send_account_creation_notifications(user)
    return True, "Account created successfully. You can log in now."


def authenticate_user(username: str, password: str) -> tuple[bool, dict[str, Any] | None, str]:
    user = get_user_by_username(username.strip())
    if not user:
        return False, None, "User not found."
    if not user["is_active"]:
        return False, None, "This account has been disabled."
    if not verify_password(password, user["password_hash"]):
        return False, None, "Incorrect password."

    with closing(get_connection()) as connection:
        connection.execute(
            "UPDATE users SET last_login_at = ? WHERE id = ?",
            (datetime.now().isoformat(), user["id"]),
        )
        connection.commit()

    return True, get_user_by_id(user["id"]), "Login successful."


def update_user_role(user_id: int, role: str) -> None:
    with closing(get_connection()) as connection:
        connection.execute("UPDATE users SET role = ? WHERE id = ?", (role, user_id))
        connection.commit()


def update_user_status(user_id: int, is_active: bool) -> None:
    with closing(get_connection()) as connection:
        connection.execute(
            "UPDATE users SET is_active = ? WHERE id = ?",
            (1 if is_active else 0, user_id),
        )
        connection.commit()


def reset_user_password(user_id: int, new_password: str) -> None:
    with closing(get_connection()) as connection:
        connection.execute(
            "UPDATE users SET password_hash = ? WHERE id = ?",
            (hash_password(new_password), user_id),
        )
        connection.commit()


def delete_user_account(user_id: int) -> None:
    with closing(get_connection()) as connection:
        connection.execute("DELETE FROM transactions WHERE user_id = ?", (user_id,))
        connection.execute("DELETE FROM categories WHERE user_id = ?", (user_id,))
        connection.execute("DELETE FROM users WHERE id = ?", (user_id,))
        connection.commit()


def reset_user_account_data(user_id: int) -> None:
    with closing(get_connection()) as connection:
        connection.execute("DELETE FROM transactions WHERE user_id = ?", (user_id,))
        connection.execute(
            "DELETE FROM categories WHERE user_id = ? AND is_default = 0",
            (user_id,),
        )
        connection.commit()
    ensure_default_categories(user_id)


def delete_transaction(transaction_id: int, user_id: int) -> None:
    with closing(get_connection()) as connection:
        connection.execute(
            "DELETE FROM transactions WHERE id = ? AND user_id = ?",
            (transaction_id, user_id),
        )
        connection.commit()


def delete_transactions(transaction_ids: list[int], user_id: int) -> None:
    with closing(get_connection()) as connection:
        connection.executemany(
            "DELETE FROM transactions WHERE id = ? AND user_id = ?",
            [(transaction_id, user_id) for transaction_id in transaction_ids],
        )
        connection.commit()


def add_transaction(
    user_id: int,
    transaction_date: date,
    transaction_type: str,
    category: str,
    amount: float,
    notes: str,
) -> None:
    with closing(get_connection()) as connection:
        connection.execute(
            """
            INSERT INTO transactions
            (user_id, transaction_date, transaction_type, category, amount, notes, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                pd.Timestamp(transaction_date).strftime("%Y-%m-%d"),
                transaction_type,
                normalize_category(category),
                round(float(amount), 2),
                notes.strip(),
                datetime.now().isoformat(),
            ),
        )
        connection.commit()


def get_transactions_for_user(user_id: int) -> pd.DataFrame:
    with closing(get_connection()) as connection:
        rows = connection.execute(
            """
            SELECT id, user_id, transaction_date, transaction_type, category, amount, notes, created_at
            FROM transactions
            WHERE user_id = ?
            ORDER BY transaction_date DESC, id DESC
            """,
            (user_id,),
        ).fetchall()

    transactions = pd.DataFrame([dict(row) for row in rows])
    if transactions.empty:
        return pd.DataFrame(
            columns=[
                "id",
                "user_id",
                "transaction_date",
                "transaction_type",
                "category",
                "amount",
                "notes",
                "created_at",
            ]
        )

    transactions["transaction_date"] = pd.to_datetime(
        transactions["transaction_date"], errors="coerce"
    )
    transactions["amount"] = pd.to_numeric(transactions["amount"], errors="coerce").fillna(0)
    transactions["transaction_type"] = (
        transactions["transaction_type"].astype(str).str.strip().str.lower()
    )
    transactions["category"] = transactions["category"].fillna("").astype(str)
    transactions["notes"] = transactions["notes"].fillna("").astype(str)
    return transactions.dropna(subset=["transaction_date"]).reset_index(drop=True)


def format_currency(value: float) -> str:
    return f"Rs. {value:,.2f}"


def format_transaction_amount(amount: float, transaction_type: str) -> str:
    prefix = "-" if transaction_type == "expense" else "+"
    return f"{prefix} {format_currency(amount)}"


def get_top_expense_category(transactions: pd.DataFrame) -> tuple[str, float]:
    expense_data = transactions[transactions["transaction_type"] == "expense"]
    if expense_data.empty:
        return "No expenses yet", 0.0

    category_totals = expense_data.groupby("category")["amount"].sum().sort_values(ascending=False)
    top_category = category_totals.index[0]
    return str(top_category), float(category_totals.iloc[0])


def get_largest_transaction(transactions: pd.DataFrame) -> dict[str, Any] | None:
    if transactions.empty:
        return None
    row = transactions.sort_values("amount", ascending=False).iloc[0]
    return row.to_dict()


def get_date_defaults(transactions: pd.DataFrame) -> tuple[date, date]:
    if not transactions.empty:
        return transactions["transaction_date"].min().date(), transactions["transaction_date"].max().date()
    today = pd.Timestamp.today().date()
    return today, today


def set_active_section(section: str, admin_section: str | None = None) -> None:
    st.session_state[DASHBOARD_SECTION_KEY] = section
    if admin_section is not None:
        st.session_state[ADMIN_SECTION_KEY] = admin_section


def render_section_navigation(options: list[str], key: str) -> str:
    if st.session_state.get(key) not in options:
        st.session_state[key] = options[0]

    selected = st.segmented_control(
        "Section",
        options,
        default=st.session_state[key],
        key=key,
        width="stretch",
        label_visibility="collapsed",
    )
    if selected not in options:
        selected = options[0]
        st.session_state[key] = selected
    return str(selected)


def build_transactions_csv(transactions: pd.DataFrame) -> bytes:
    export_df = transactions.copy()
    if export_df.empty:
        export_df = pd.DataFrame(columns=["date", "type", "category", "amount", "notes"])
    else:
        export_df = export_df.rename(
            columns={
                "transaction_date": "date",
                "transaction_type": "type",
            }
        )[["date", "type", "category", "amount", "notes"]]
        export_df["date"] = pd.to_datetime(export_df["date"]).dt.strftime("%Y-%m-%d")
    return export_df.to_csv(index=False).encode("utf-8")


def build_transaction_template_csv() -> bytes:
    template_df = pd.DataFrame(
        [
            {
                "date": "2026-04-01",
                "type": "expense",
                "category": "Food",
                "amount": 250.00,
                "notes": "Lunch",
            },
            {
                "date": "2026-04-01",
                "type": "credit",
                "category": "Salary Credit",
                "amount": 50000.00,
                "notes": "Monthly salary",
            },
        ]
    )
    return template_df.to_csv(index=False).encode("utf-8")


def import_transactions_csv(user_id: int, uploaded_file) -> tuple[bool, str]:
    try:
        uploaded_file.seek(0)
        imported_df = pd.read_csv(uploaded_file)
    except Exception as exc:
        return False, f"Could not read CSV: {exc}"

    required_columns = {"date", "type", "category", "amount"}
    missing_columns = required_columns - set(imported_df.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        return False, f"CSV is missing required columns: {missing}"

    if "notes" not in imported_df.columns:
        imported_df["notes"] = ""

    imported_df = imported_df.fillna("")
    inserted = 0
    skipped = 0
    now = datetime.now().isoformat()

    with closing(get_connection()) as connection:
        category_rows: set[tuple[int, str, str, int, str]] = set()
        for _, row in imported_df.iterrows():
            transaction_date = pd.to_datetime(row.get("date"), errors="coerce")
            transaction_type = str(row.get("type", "")).strip().lower()
            category = normalize_category(str(row.get("category", "")).strip())
            amount = pd.to_numeric(row.get("amount"), errors="coerce")
            notes = str(row.get("notes", "")).strip()

            if (
                pd.isna(transaction_date)
                or transaction_type not in TYPE_LABELS
                or not category
                or pd.isna(amount)
                or float(amount) <= 0
            ):
                skipped += 1
                continue

            category_rows.add((user_id, transaction_type, category, 0, now))
            connection.execute(
                """
                INSERT INTO transactions
                (user_id, transaction_date, transaction_type, category, amount, notes, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    transaction_date.strftime("%Y-%m-%d"),
                    transaction_type,
                    category,
                    round(float(amount), 2),
                    notes,
                    now,
                ),
            )
            inserted += 1

        if category_rows:
            connection.executemany(
                """
                INSERT OR IGNORE INTO categories
                (user_id, transaction_type, name, is_default, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                list(category_rows),
            )
        connection.commit()

    if inserted == 0:
        return False, f"No transactions were imported. Skipped {skipped} row(s)."

    return True, f"Imported {inserted} transaction(s). Skipped {skipped} row(s)."


def render_hero_header(user: dict[str, Any], transactions: pd.DataFrame) -> None:
    month_start = pd.Timestamp.today().replace(day=1)
    month_transactions = transactions[transactions["transaction_date"] >= month_start]
    totals = summarize_totals(month_transactions)
    top_category, top_amount = get_top_expense_category(month_transactions)

    st.markdown(
        f"""
        <div class="hero-card">
          <div class="hero-eyebrow">Expense and Credit Tracker</div>
          <h1 class="hero-title">Welcome, {html.escape(user["full_name"])}</h1>
          <div class="hero-copy">
            A clearer mobile-first view of your money, with quick actions, simple filters,
            and easy-to-read summaries for spending, credits, and trends.
          </div>
          <div class="hero-stats">
            <div class="hero-stat">
              <div class="hero-stat-label">This month spent</div>
              <div class="hero-stat-value">{format_currency(totals["expense_total"])}</div>
            </div>
            <div class="hero-stat">
              <div class="hero-stat-label">This month credited</div>
              <div class="hero-stat-value">{format_currency(totals["credit_total"])}</div>
            </div>
            <div class="hero-stat">
              <div class="hero-stat-label">Top spending category</div>
              <div class="hero-stat-value">{html.escape(top_category)}</div>
              <div class="hero-stat-label">{format_currency(top_amount)}</div>
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_filter_panel(transactions: pd.DataFrame) -> pd.DataFrame:
    st.markdown(
        """
        <div class="section-hint">
          Use these filters to focus the overview, charts, and transaction list on the dates and activity type you care about.
        </div>
        """,
        unsafe_allow_html=True,
    )

    min_date, max_date = get_date_defaults(transactions)
    filter_col_1, filter_col_2 = st.columns([1, 1.4])
    with filter_col_1:
        transaction_filter = st.selectbox("Show", ["All", "Expense", "Credit"])
    with filter_col_2:
        selected_date_range = st.date_input(
            "Date range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )

    if isinstance(selected_date_range, tuple):
        date_range = selected_date_range
    else:
        date_range = (selected_date_range, selected_date_range)
    return get_filtered_transactions(transactions, transaction_filter, date_range)


def render_quick_insights(filtered_transactions: pd.DataFrame) -> None:
    st.subheader("Quick Insights")
    if filtered_transactions.empty:
        st.info("Add or filter transactions to see simple takeaways here.")
        return

    top_category, top_amount = get_top_expense_category(filtered_transactions)
    largest = get_largest_transaction(filtered_transactions)
    latest = filtered_transactions.iloc[0].to_dict()

    insights = [
        (
            "Top expense category",
            f"{top_category}",
            f"Spent {format_currency(top_amount)} in the current filtered view.",
        ),
        (
            "Largest transaction",
            format_transaction_amount(float(largest["amount"]), str(largest["transaction_type"]))
            if largest
            else "No transactions",
            f"{str(largest['transaction_type']).title()} in {largest['category']}" if largest else "",
        ),
        (
            "Latest activity",
            latest["category"],
            f"{latest['transaction_date'].strftime('%d %b %Y')} | {str(latest['transaction_type']).title()}",
        ),
    ]

    insight_columns = st.columns(len(insights))
    for column, (label, value, note) in zip(insight_columns, insights):
        with column:
            st.markdown(
                f"""
                <div class="insight-card">
                  <div class="insight-label">{html.escape(label)}</div>
                  <div class="insight-value">{html.escape(str(value))}</div>
                  <div class="muted-copy">{html.escape(str(note))}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def render_recent_transactions_cards(filtered_transactions: pd.DataFrame, limit: int = 6) -> None:
    st.subheader("Recent Transactions")
    if filtered_transactions.empty:
        st.info("No transactions match the current filter.")
        return

    for _, row in filtered_transactions.head(limit).iterrows():
        note = row["notes"] if str(row["notes"]).strip() else "No note added"
        st.markdown(
            f"""
            <div class="tx-card">
              <div class="tx-top">
                <div>
                  <div class="tx-title">{html.escape(str(row["category"]))}</div>
                  <div class="tx-meta">
                    {row["transaction_date"].strftime('%d %b %Y')} | {html.escape(str(row["transaction_type"]).title())}
                  </div>
                </div>
                <div class="tx-amount {html.escape(str(row["transaction_type"]))}">
                  {html.escape(format_transaction_amount(float(row["amount"]), str(row["transaction_type"])))}
                </div>
              </div>
              <div class="muted-copy">{html.escape(str(note))}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_recap_section(user: dict[str, Any]) -> None:
    st.subheader("Monthly Recap Email")
    st.caption(
        f"Send yourself a monthly summary at {user['email']}. SMTP must be configured first."
    )
    recap_month = st.date_input(
        "Send recap for month",
        value=(pd.Timestamp.today().replace(day=1) - pd.DateOffset(months=1)).date(),
        key="self_recap_month",
    )
    if st.button("Email My Monthly Recap", use_container_width=True):
        recap_start = pd.Timestamp(recap_month).replace(day=1)
        sent, message = send_monthly_recap_email(user, recap_start)
        if sent:
            st.success(f"Monthly recap sent to {user['email']}.")
        else:
            st.error(f"Monthly recap could not be sent: {message}")


def get_filtered_transactions(
    transactions: pd.DataFrame,
    transaction_filter: str,
    date_range: tuple[date, date],
) -> pd.DataFrame:
    filtered = transactions.copy()
    if transaction_filter != "All":
        filtered = filtered[filtered["transaction_type"] == transaction_filter.lower()]

    start_date = pd.Timestamp(date_range[0])
    end_date = pd.Timestamp(date_range[1])
    filtered = filtered[
        filtered["transaction_date"].between(
            start_date, end_date + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        )
    ]
    return filtered.sort_values(["transaction_date", "id"], ascending=[False, False]).reset_index(
        drop=True
    )


def summarize_totals(transactions: pd.DataFrame) -> dict[str, float]:
    expense_total = transactions.loc[
        transactions["transaction_type"] == "expense", "amount"
    ].sum()
    credit_total = transactions.loc[
        transactions["transaction_type"] == "credit", "amount"
    ].sum()
    return {
        "expense_total": float(expense_total),
        "credit_total": float(credit_total),
        "net_total": float(credit_total - expense_total),
    }


def build_period_stats(transactions: pd.DataFrame) -> pd.DataFrame:
    if transactions.empty:
        return pd.DataFrame(columns=["Period", "Transactions", "Expenses", "Credits", "Net"])

    today = pd.Timestamp.today().normalize()
    periods = {
        "Today": (today, today + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)),
        "This Week": (
            today - pd.Timedelta(days=today.weekday()),
            today + pd.Timedelta(days=1) - pd.Timedelta(seconds=1),
        ),
        "This Month": (
            today.replace(day=1),
            today + pd.Timedelta(days=1) - pd.Timedelta(seconds=1),
        ),
    }

    rows = []
    for label, (start, end) in periods.items():
        current = transactions[transactions["transaction_date"].between(start, end)]
        totals = summarize_totals(current)
        rows.append(
            {
                "Period": label,
                "Transactions": int(len(current)),
                "Expenses": totals["expense_total"],
                "Credits": totals["credit_total"],
                "Net": totals["net_total"],
            }
        )
    return pd.DataFrame(rows)


def build_monthly_summary(transactions: pd.DataFrame) -> pd.DataFrame:
    if transactions.empty:
        return pd.DataFrame(columns=["month", "Expense", "Credit", "Net"])

    monthly = (
        transactions.assign(month=transactions["transaction_date"].dt.to_period("M").dt.to_timestamp())
        .groupby(["month", "transaction_type"], as_index=False)["amount"]
        .sum()
    )

    pivot = (
        monthly.pivot(index="month", columns="transaction_type", values="amount")
        .fillna(0)
        .rename(columns={"expense": "Expense", "credit": "Credit"})
        .reset_index()
    )
    if "Expense" not in pivot:
        pivot["Expense"] = 0.0
    if "Credit" not in pivot:
        pivot["Credit"] = 0.0
    pivot["Net"] = pivot["Credit"] - pivot["Expense"]
    return pivot.sort_values("month")


def log_notification(
    event_type: str,
    recipient_email: str,
    subject: str,
    body: str,
    status: str,
    error_message: str | None = None,
) -> None:
    with closing(get_connection()) as connection:
        connection.execute(
            """
            INSERT INTO notifications
            (event_type, recipient_email, subject, body, status, error_message, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_type,
                recipient_email,
                subject,
                body,
                status,
                error_message,
                datetime.now().isoformat(),
            ),
        )
        connection.commit()


def send_email(recipient_email: str, subject: str, body: str, event_type: str) -> tuple[bool, str]:
    smtp_config = get_smtp_config()
    smtp_host = smtp_config["host"]
    smtp_port = smtp_config["port"] or "587"
    smtp_username = smtp_config["username"]
    smtp_password = smtp_config["password"]
    smtp_from_email = smtp_config["from_email"] or smtp_username or DEV_ACCOUNT_EMAIL

    if not smtp_host or not smtp_username or not smtp_password:
        message = (
            "SMTP is not configured. Set SMTP_HOST, SMTP_PORT, SMTP_USERNAME, "
            "SMTP_PASSWORD, and optionally SMTP_FROM_EMAIL."
        )
        log_notification(event_type, recipient_email, subject, body, "pending", message)
        return False, message

    email_message = EmailMessage()
    email_message["Subject"] = subject
    email_message["From"] = smtp_from_email
    email_message["To"] = recipient_email
    email_message.set_content(body)

    try:
        with smtplib.SMTP(smtp_host, int(smtp_port), timeout=20) as smtp:
            smtp.starttls()
            smtp.login(smtp_username, smtp_password)
            smtp.send_message(email_message)
        log_notification(event_type, recipient_email, subject, body, "sent")
        return True, "Email sent successfully."
    except Exception as exc:
        log_notification(event_type, recipient_email, subject, body, "failed", str(exc))
        return False, str(exc)


def send_account_creation_notifications(user: dict[str, Any]) -> list[str]:
    recipients = set(ADMIN_NOTIFICATION_EMAILS)
    if DEV_ACCOUNT_EMAIL:
        recipients.add(DEV_ACCOUNT_EMAIL)

    body = (
        "A new user account has been created.\n\n"
        f"Name: {user['full_name']}\n"
        f"Username: {user['username']}\n"
        f"Email: {user['email']}\n"
        f"Created at: {user['created_at']}\n"
    )

    messages = []
    for recipient in sorted(recipients):
        sent, message = send_email(
            recipient,
            subject=f"New account created: {user['username']}",
            body=body,
            event_type="account_created",
        )
        status = "sent" if sent else "queued"
        messages.append(f"{recipient}: {status}")
        if not sent:
            messages.append(message)
    return messages


def build_monthly_recap(user: dict[str, Any], month_start: pd.Timestamp) -> str:
    month_end = month_start + pd.offsets.MonthEnd(0)
    transactions = get_transactions_for_user(user["id"])
    monthly_transactions = transactions[
        transactions["transaction_date"].between(month_start, month_end)
    ]
    totals = summarize_totals(monthly_transactions)

    expense_breakdown = (
        monthly_transactions[monthly_transactions["transaction_type"] == "expense"]
        .groupby("category")["amount"]
        .sum()
        .sort_values(ascending=False)
    )

    top_categories = "\n".join(
        f"- {category}: {format_currency(amount)}"
        for category, amount in expense_breakdown.head(5).items()
    )
    if not top_categories:
        top_categories = "- No expenses recorded"

    return (
        f"Monthly expense recap for {user['full_name']} ({month_start.strftime('%B %Y')})\n\n"
        f"Transactions: {len(monthly_transactions)}\n"
        f"Total expenses: {format_currency(totals['expense_total'])}\n"
        f"Total credits: {format_currency(totals['credit_total'])}\n"
        f"Net balance: {format_currency(totals['net_total'])}\n\n"
        "Top expense categories:\n"
        f"{top_categories}\n"
    )


def send_monthly_recap_email(user: dict[str, Any], month_start: pd.Timestamp) -> tuple[bool, str]:
    subject = f"Your monthly expense recap - {month_start.strftime('%B %Y')}"
    body = build_monthly_recap(user, month_start)
    sent, message = send_email(user["email"], subject, body, "monthly_recap")

    if sent:
        with closing(get_connection()) as connection:
            connection.execute(
                "UPDATE users SET last_recap_month = ? WHERE id = ?",
                (month_start.strftime("%Y-%m"), user["id"]),
            )
            connection.commit()
    return sent, message


def send_due_monthly_recap_if_needed(user: dict[str, Any]) -> str | None:
    today = pd.Timestamp.today()
    previous_month = (today.replace(day=1) - pd.DateOffset(months=1)).to_period("M").to_timestamp()
    recap_key = previous_month.strftime("%Y-%m")

    if user.get("last_recap_month") == recap_key:
        return None

    sent, message = send_monthly_recap_email(user, previous_month)
    if sent:
        return f"Monthly recap sent to {user['email']} for {previous_month.strftime('%B %Y')}."
    return f"Monthly recap could not be sent automatically: {message}"


def get_notification_log() -> pd.DataFrame:
    with closing(get_connection()) as connection:
        rows = connection.execute(
            """
            SELECT event_type, recipient_email, subject, status, error_message, created_at
            FROM notifications
            ORDER BY created_at DESC
            LIMIT 200
            """
        ).fetchall()
    return pd.DataFrame([dict(row) for row in rows])


def render_google_ads(user: dict[str, Any]) -> None:
    if user["role"] == "developer":
        return

    st.markdown("### Sponsored")

    ads_client = os.getenv("GOOGLE_ADSENSE_CLIENT")
    ads_slot = os.getenv("GOOGLE_ADSENSE_SLOT")
    if ads_client and ads_slot:
        ad_html = f"""
        <div style="background:#f8fafc;border:1px solid #dbe4ee;padding:12px;border-radius:12px;margin:8px 0 20px 0;">
          <ins class="adsbygoogle"
               style="display:block"
               data-ad-client="{ads_client}"
               data-ad-slot="{ads_slot}"
               data-ad-format="auto"
               data-full-width-responsive="true"></ins>
          <script async src="https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js"></script>
          <script>(adsbygoogle = window.adsbygoogle || []).push({{}});</script>
        </div>
        """
        components.html(ad_html, height=140)
    else:
        st.info(
            "Google Ads placeholder. Set GOOGLE_ADSENSE_CLIENT and GOOGLE_ADSENSE_SLOT to show live ads."
        )
        with st.expander("How to enable live Google Ads"):
            st.code(
                'export GOOGLE_ADSENSE_CLIENT="ca-pub-xxxxxxxxxxxxxxxx"\n'
                'export GOOGLE_ADSENSE_SLOT="1234567890"\n'
                "streamlit run app.py",
                language="bash",
            )
            st.caption(
                "Replace the sample values with your own Google AdSense client ID and ad slot ID."
            )


def render_login_screen() -> None:
    st.markdown(
        """
        <div class="hero-card">
          <div class="hero-eyebrow">Personal Finance</div>
          <h1 class="hero-title">Track money without the clutter</h1>
          <div class="hero-copy">
            Sign in or create an account to record expenses, credits, trends, and monthly recaps from a simpler mobile-friendly dashboard.
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    center_column = st.columns([0.08, 1, 0.08])[1]
    with center_column:
        login_tab, signup_tab = st.tabs(["Login", "Create Account"])

        with login_tab:
            with st.form("login_form"):
                username = st.text_input("Username")
                password = st.text_input("Password", type="password")
                submitted = st.form_submit_button("Login", use_container_width=True)

            if submitted:
                success, user, message = authenticate_user(username, password)
                if success and user:
                    st.session_state["user_id"] = user["id"]
                    recap_message = send_due_monthly_recap_if_needed(user)
                    if recap_message:
                        st.session_state["flash_message"] = recap_message
                    st.rerun()
                st.error(message)

        with signup_tab:
            with st.form("signup_form"):
                full_name = st.text_input("Full name")
                username = st.text_input("Username", key="signup_username")
                email = st.text_input("Email")
                password = st.text_input("Password", type="password", key="signup_password")
                submitted = st.form_submit_button("Create Account", use_container_width=True)

            if submitted:
                success, message = create_user(full_name, username, email, password)
                if success:
                    st.success(message)
                    st.info(
                        "A notification email is attempted for the developer account and sricharan2209@gmail.com."
                    )
                else:
                    st.error(message)

   # with st.expander("Developer login note", expanded=False):
   #     st.warning(
   #         f"Default developer login: username `{DEV_ACCOUNT_USERNAME}` and password `{DEV_ACCOUNT_PASSWORD}`. "
   #         "Change these using DEV_ACCOUNT_USERNAME, DEV_ACCOUNT_PASSWORD, and DEV_ACCOUNT_EMAIL environment variables."
   #     )


def render_add_transaction_section(user: dict[str, Any], categories: dict[str, list[str]]) -> None:
    st.subheader("+ Add Transaction")
    st.caption("Record a new expense or credit in a few quick steps.")

    top_row = st.columns([1, 1])
    transaction_date = top_row[0].date_input("Date", key="add_tx_date")
    transaction_type_label = top_row[1].radio(
        "Type",
        list(TYPE_LABELS.values()),
        horizontal=True,
        key="add_tx_type_label",
    )
    transaction_type = transaction_type_label.lower()

    category_options = [*categories[transaction_type], ADD_CUSTOM_OPTION]
    if st.session_state.get("add_tx_category") not in category_options:
        st.session_state["add_tx_category"] = category_options[0]

    middle_row = st.columns([1, 1])
    amount = middle_row[0].number_input(
        "Amount",
        min_value=0.0,
        step=100.0,
        format="%.2f",
        key="add_tx_amount",
    )
    selected_category = middle_row[1].selectbox(
        "Category",
        category_options,
        key="add_tx_category",
    )

    custom_category = st.text_input("Custom category (optional)", key="add_tx_custom_category")
    notes = st.text_area(
        "Notes",
        placeholder="Add a short note like dinner, fuel refill, or salary credit",
        key="add_tx_notes",
    )
    submitted = st.button("+ Save Transaction", use_container_width=True, key="add_tx_submit")

    if not submitted:
        return

    if amount <= 0:
        st.error("Please enter an amount greater than 0.")
        return

    category = custom_category if custom_category.strip() else selected_category
    if category == ADD_CUSTOM_OPTION:
        st.error("Please enter a custom category name.")
        return

    category = add_category_for_user(user["id"], transaction_type, category)
    add_transaction(user["id"], transaction_date, transaction_type, category, amount, notes)
    st.session_state["flash_message"] = f"{TYPE_LABELS[transaction_type]} saved successfully."
    for key in ["add_tx_amount", "add_tx_category", "add_tx_custom_category", "add_tx_notes"]:
        st.session_state.pop(key, None)
    set_active_section("Add")
    st.rerun()


def render_category_manager(user: dict[str, Any]) -> None:
    st.subheader("Categories")
    st.caption("Keep your categories tidy so every transaction is easy to understand later.")

    with st.form("add_category_form", clear_on_submit=True):
        category_row = st.columns([1, 1])
        category_type_label = category_row[0].selectbox("Category type", list(TYPE_LABELS.values()))
        category_type = category_type_label.lower()
        new_category = category_row[1].text_input("New category")
        add_category_clicked = st.form_submit_button("+ Add Category", use_container_width=True)

    if add_category_clicked:
        if not new_category.strip():
            st.error("Please enter a category name.")
        else:
            category = add_category_for_user(user["id"], category_type, new_category)
            st.session_state["flash_message"] = f"Category '{category}' added."
            set_active_section("Settings")
            st.rerun()

    categories = get_categories_for_user(user["id"])
    for transaction_type, label in TYPE_LABELS.items():
        chip_markup = "".join(
            f"<span class='chip'>{html.escape(category)}</span>"
            for category in categories[transaction_type]
        )
        st.markdown(f"**{label} categories**")
        st.markdown(f"<div class='chip-row'>{chip_markup}</div>", unsafe_allow_html=True)


def render_metrics(filtered_transactions: pd.DataFrame) -> None:
    st.subheader("Filtered Summary")
    totals = summarize_totals(filtered_transactions)
    metric_columns = st.columns(4)
    metric_columns[0].metric("Total Expenses", format_currency(totals["expense_total"]))
    metric_columns[1].metric("Total Credits", format_currency(totals["credit_total"]))
    metric_columns[2].metric("Net Balance", format_currency(totals["net_total"]))
    metric_columns[3].metric("Transactions", f"{len(filtered_transactions)}")


def render_period_stats(transactions: pd.DataFrame) -> None:
    st.subheader("Daily, Weekly, and Monthly Stats")
    period_stats = build_period_stats(transactions)
    if period_stats.empty:
        st.info("Add transactions to see your daily, weekly, and monthly stats.")
        return

    columns = st.columns(3)
    for index, row in period_stats.iterrows():
        with columns[index]:
            st.markdown(f"**{row['Period']}**")
            st.metric("Expenses", format_currency(row["Expenses"]))
            st.metric("Credits", format_currency(row["Credits"]))
            st.metric("Net", format_currency(row["Net"]))
            st.caption(f"Transactions: {int(row['Transactions'])}")


def render_pie_chart(filtered_transactions: pd.DataFrame) -> None:
    st.subheader("Category Pie Chart")

    available_types = []
    if (filtered_transactions["transaction_type"] == "expense").any():
        available_types.append("Expense")
    if (filtered_transactions["transaction_type"] == "credit").any():
        available_types.append("Credit")

    if not available_types:
        st.info("Add a few transactions to see the category split.")
        return

    if st.session_state.get("pie_chart_type") not in available_types:
        st.session_state["pie_chart_type"] = available_types[0]

    chart_type = st.radio(
        "Show pie chart for",
        available_types,
        horizontal=True,
        key="pie_chart_type",
    ).lower()

    pie_data = (
        filtered_transactions[filtered_transactions["transaction_type"] == chart_type]
        .groupby("category", as_index=False)["amount"]
        .sum()
        .sort_values("amount", ascending=False)
    )

    pie_chart = (
        alt.Chart(pie_data, title=f"{TYPE_LABELS[chart_type]} distribution by category")
        .mark_arc(innerRadius=60)
        .encode(
            theta=alt.Theta("amount:Q", stack=True),
            color=alt.Color("category:N", legend=alt.Legend(title="Category")),
            tooltip=[
                alt.Tooltip("category:N", title="Category"),
                alt.Tooltip("amount:Q", title="Amount", format=",.2f"),
            ],
        )
    )
    st.altair_chart(pie_chart, use_container_width=True)


def render_monthly_trends(filtered_transactions: pd.DataFrame) -> None:
    st.subheader("Monthly Trends")

    monthly_summary = build_monthly_summary(filtered_transactions)
    if monthly_summary.empty:
        st.info("Monthly trends will appear once you add transactions.")
        return

    trend_data = monthly_summary.melt(
        id_vars="month",
        value_vars=["Expense", "Credit", "Net"],
        var_name="Series",
        value_name="Amount",
    )
    trend_chart = (
        alt.Chart(trend_data, title="Expenses, credits, and net balance by month")
        .mark_line(point=True)
        .encode(
            x=alt.X("month:T", title="Month"),
            y=alt.Y("Amount:Q", title="Amount"),
            color=alt.Color("Series:N", legend=alt.Legend(title="")),
            tooltip=[
                alt.Tooltip("month:T", title="Month"),
                alt.Tooltip("Series:N", title="Series"),
                alt.Tooltip("Amount:Q", title="Amount", format=",.2f"),
            ],
        )
    )
    st.altair_chart(trend_chart, use_container_width=True)

    expense_category_trend = filtered_transactions[
        filtered_transactions["transaction_type"] == "expense"
    ].copy()
    if not expense_category_trend.empty:
        expense_category_trend["month"] = (
            expense_category_trend["transaction_date"].dt.to_period("M").dt.to_timestamp()
        )
        category_chart_data = (
            expense_category_trend.groupby(["month", "category"], as_index=False)["amount"]
            .sum()
            .sort_values(["month", "amount"], ascending=[True, False])
        )
        category_chart = (
            alt.Chart(category_chart_data, title="Monthly expense trend by category")
            .mark_bar()
            .encode(
                x=alt.X("month:T", title="Month"),
                y=alt.Y("amount:Q", title="Amount", stack=True),
                color=alt.Color("category:N", legend=alt.Legend(title="Category")),
                tooltip=[
                    alt.Tooltip("month:T", title="Month"),
                    alt.Tooltip("category:N", title="Category"),
                    alt.Tooltip("amount:Q", title="Amount", format=",.2f"),
                ],
            )
        )
        st.altair_chart(category_chart, use_container_width=True)

    monthly_table = monthly_summary.copy()
    monthly_table["Month"] = monthly_table["month"].dt.strftime("%b %Y")
    monthly_table = monthly_table[["Month", "Expense", "Credit", "Net"]]
    st.dataframe(
        monthly_table,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Expense": st.column_config.NumberColumn(format="%.2f"),
            "Credit": st.column_config.NumberColumn(format="%.2f"),
            "Net": st.column_config.NumberColumn(format="%.2f"),
        },
    )


def render_transactions_table(filtered_transactions: pd.DataFrame, user: dict[str, Any]) -> None:
    st.subheader("Transactions")
    if filtered_transactions.empty:
        st.info("No transactions match the selected filters.")
        return

    table_data = filtered_transactions.copy()
    table_data["Date"] = table_data["transaction_date"].dt.strftime("%d %b %Y")
    table_data["Type"] = table_data["transaction_type"].str.title()
    table_data["Amount"] = table_data["amount"]
    table_data["Category"] = table_data["category"]
    table_data["Notes"] = table_data["notes"]

    render_recent_transactions_cards(filtered_transactions, limit=8)

    with st.expander("Open full table", expanded=False):
        st.dataframe(
            table_data[["Date", "Type", "Category", "Amount", "Notes"]],
            use_container_width=True,
            hide_index=True,
            column_config={"Amount": st.column_config.NumberColumn(format="%.2f")},
        )

    all_transactions = get_transactions_for_user(user["id"])
    with st.expander("Import / Export CSV", expanded=False):
        export_columns = st.columns(3)
        export_columns[0].download_button(
            "Download filtered CSV",
            data=build_transactions_csv(filtered_transactions),
            file_name="filtered_transactions.csv",
            mime="text/csv",
            use_container_width=True,
        )
        export_columns[1].download_button(
            "Download all CSV",
            data=build_transactions_csv(all_transactions),
            file_name="all_transactions.csv",
            mime="text/csv",
            use_container_width=True,
        )
        export_columns[2].download_button(
            "Download template",
            data=build_transaction_template_csv(),
            file_name="transactions_template.csv",
            mime="text/csv",
            use_container_width=True,
        )

        st.caption(
            "Template columns: `date`, `type`, `category`, `amount`, `notes`. Use `expense` or `credit` for the type."
        )
        uploaded_csv = st.file_uploader(
            "Upload transactions CSV",
            type=["csv"],
            key="transactions_csv_upload",
            help="Upload a CSV using the provided template or the same column format.",
        )
        if st.button("Upload Transactions CSV", use_container_width=True):
            if uploaded_csv is None:
                st.error("Choose a CSV file first.")
            else:
                success, message = import_transactions_csv(user["id"], uploaded_csv)
                if success:
                    st.session_state["flash_message"] = message
                    set_active_section("Transactions")
                    st.rerun()
                st.error(message)

    st.markdown("#### Delete Transactions")
    st.caption("Tick one or more entries below, then confirm removal.")
    selected_ids: list[int] = []
    with st.expander("Select transactions to remove", expanded=False):
        for _, row in filtered_transactions.iterrows():
            label = (
                f"{row['transaction_date'].strftime('%d %b %Y')} | "
                f"{str(row['transaction_type']).title()} | "
                f"{row['category']} | "
                f"{format_transaction_amount(float(row['amount']), str(row['transaction_type']))}"
            )
            is_selected = st.checkbox(label, key=f"tx_delete_{int(row['id'])}")
            if is_selected:
                selected_ids.append(int(row["id"]))
            if str(row["notes"]).strip():
                st.caption(f"Note: {row['notes']}")

    confirm_delete_transaction = st.checkbox("Confirm selected transaction delete")
    if st.button("- Remove Selected Transactions", use_container_width=True):
        if not selected_ids:
            st.error("Select at least one transaction.")
        elif not confirm_delete_transaction:
            st.error("Please confirm the delete action first.")
        else:
            delete_transactions(selected_ids, user["id"])
            st.session_state["flash_message"] = "Selected transactions deleted successfully."
            set_active_section("Transactions")
            st.rerun()

    if st.button("+ Add Transaction", use_container_width=True):
        set_active_section("Add")
        st.rerun()


def render_admin_panel(current_user: dict[str, Any]) -> None:
    if current_user["role"] != "developer":
        return

    st.subheader("Developer Admin")
    st.caption("Manage users, recaps, SMTP settings, and notification history from one place.")

    admin_section = render_section_navigation(
        ["Users", "Recaps", "SMTP", "Notifications"],
        ADMIN_SECTION_KEY,
    )

    users_df = list_users()

    if admin_section == "Users":
        if users_df.empty:
            st.info("No users available yet.")
        else:
            display_df = users_df.copy()
            display_df["created_at"] = pd.to_datetime(display_df["created_at"], errors="coerce")
            if "last_login_at" in display_df:
                display_df["last_login_at"] = pd.to_datetime(display_df["last_login_at"], errors="coerce")

            with st.expander("+ Add User", expanded=False):
                with st.form("developer_add_user_form", clear_on_submit=True):
                    new_full_name = st.text_input("Full name", key="dev_new_full_name")
                    new_username = st.text_input("Username", key="dev_new_username")
                    new_email = st.text_input("Email", key="dev_new_email")
                    new_password = st.text_input("Password", type="password", key="dev_new_password")
                    create_user_clicked = st.form_submit_button("+ Create User", use_container_width=True)

                if create_user_clicked:
                    success, message = create_user(new_full_name, new_username, new_email, new_password)
                    if success:
                        st.success(message)
                        set_active_section("Developer Admin", "Users")
                        st.rerun()
                    st.error(message)

            st.markdown("#### Select Users")
            st.caption("Tick users to manage them. Delete works on all selected rows.")
            selected_user_ids: list[int] = []
            for _, row in display_df.iterrows():
                username = html.escape(str(row["username"]))
                full_name = html.escape(str(row["full_name"]))
                email = html.escape(str(row["email"]))
                role = html.escape(str(row["role"]).title())
                status = "Active" if bool(row["is_active"]) else "Disabled"
                checkbox_label = f"{row['username']} | {row['email']} | {row['role']}"
                checked = st.checkbox(checkbox_label, key=f"user_pick_{int(row['id'])}")
                st.caption(f"{full_name} | {email} | {role} | {status}")
                if checked:
                    selected_user_ids.append(int(row["id"]))

            selected_user = get_user_by_id(selected_user_ids[0]) if selected_user_ids else None
            if len(selected_user_ids) > 1:
                st.caption("Using the first selected user for reset and role changes.")

            with st.expander("Open user table", expanded=False):
                st.dataframe(display_df, use_container_width=True, hide_index=True)

            if selected_user:
                st.markdown(f"#### Selected user: `{selected_user['username']}`")
                if selected_user["id"] != current_user["id"]:
                    status_columns = st.columns([1, 1])
                    desired_role = status_columns[0].selectbox(
                        "Role",
                        ["user", "developer"],
                        index=0 if selected_user["role"] == "user" else 1,
                    )
                    desired_status = status_columns[1].selectbox(
                        "Status",
                        ["active", "disabled"],
                        index=0 if selected_user["is_active"] else 1,
                    )
                    if st.button("Save Role and Status", use_container_width=True):
                        update_user_role(selected_user["id"], desired_role)
                        update_user_status(selected_user["id"], desired_status == "active")
                        st.session_state["flash_message"] = f"Updated user '{selected_user['username']}'."
                        set_active_section("Developer Admin", "Users")
                        st.rerun()
                else:
                    st.info("The active developer account cannot be role-edited or deleted from this screen.")

                reset_columns = st.columns([1, 1])
                with reset_columns[0]:
                    reset_password_value = st.text_input(
                        "New password",
                        type="password",
                        key=f"reset_password_{selected_user['id']}",
                    )
                    if st.button("Reset Password", key=f"btn_reset_password_{selected_user['id']}", use_container_width=True):
                        if selected_user["id"] == current_user["id"]:
                            st.error("Reset the developer password through environment variables or a dedicated secure flow.")
                        elif not reset_password_value.strip():
                            st.error("Enter a new password to reset.")
                        else:
                            reset_user_password(selected_user["id"], reset_password_value)
                            st.success(
                                f"Password reset for {selected_user['username']}. "
                                f"New temporary password: {reset_password_value}"
                            )

                with reset_columns[1]:
                    confirm_reset_data = st.checkbox(
                        "Confirm account data reset",
                        key=f"confirm_data_reset_{selected_user['id']}",
                    )
                    if st.button("Reset Account Data", key=f"btn_reset_data_{selected_user['id']}", use_container_width=True):
                        if selected_user["id"] == current_user["id"]:
                            st.error("You cannot reset the active developer account data from here.")
                        elif not confirm_reset_data:
                            st.error("Confirm data reset first.")
                        else:
                            reset_user_account_data(selected_user["id"])
                            st.success(f"Account data reset for {selected_user['username']}.")

            confirm_delete = st.checkbox("Confirm selected user delete", key="confirm_delete_selected_users")
            if st.button("- Remove Selected Users", key="btn_delete_selected_users", use_container_width=True):
                if not selected_user_ids:
                    st.error("Select at least one user.")
                elif not confirm_delete:
                    st.error("Confirm delete first.")
                else:
                    protected_ids = {current_user["id"]}
                    removable_ids = [user_id for user_id in selected_user_ids if user_id not in protected_ids]
                    if not removable_ids:
                        st.error("You cannot delete the active developer account.")
                    else:
                        for removable_id in removable_ids:
                            delete_user_account(removable_id)
                        st.success(f"Deleted {len(removable_ids)} selected user(s).")
                        set_active_section("Developer Admin", "Users")
                        st.rerun()

    if admin_section == "Recaps":
        recap_candidates = users_df[users_df["role"] != "developer"]["username"].tolist() if not users_df.empty else []
        if not recap_candidates:
            st.info("No non-developer users available for recap delivery.")
        else:
            recap_username = st.selectbox("Send monthly recap to user", recap_candidates, key="recap_user")
            recap_month = st.date_input(
                "Recap month",
                value=(pd.Timestamp.today().replace(day=1) - pd.DateOffset(months=1)).date(),
                key="recap_month",
            )
            if st.button("Send Monthly Recap Now", use_container_width=True):
                recap_user = get_user_by_username(recap_username)
                recap_start = pd.Timestamp(recap_month).replace(day=1)
                if recap_user:
                    sent, message = send_monthly_recap_email(recap_user, recap_start)
                    if sent:
                        st.success(f"Monthly recap sent to {recap_user['email']}.")
                    else:
                        st.error(f"Monthly recap failed: {message}")

    if admin_section == "SMTP":
        st.markdown("#### SMTP Settings")
        st.caption(
            "These settings are stored locally in the app database. Environment variables still override them if present."
        )
        smtp_config = get_smtp_config()
        with st.form("smtp_settings_form"):
            smtp_host = st.text_input("SMTP host", value=smtp_config["host"])
            smtp_port = st.text_input("SMTP port", value=smtp_config["port"] or "587")
            smtp_username = st.text_input("SMTP username", value=smtp_config["username"])
            smtp_password = st.text_input("SMTP password", value=smtp_config["password"], type="password")
            smtp_from_email = st.text_input(
                "From email",
                value=smtp_config["from_email"] or smtp_config["username"],
            )
            save_smtp = st.form_submit_button("Save SMTP Settings", use_container_width=True)

        if save_smtp:
            save_setting("smtp_host", smtp_host.strip())
            save_setting("smtp_port", smtp_port.strip())
            save_setting("smtp_username", smtp_username.strip())
            save_setting("smtp_password", smtp_password)
            save_setting("smtp_from_email", smtp_from_email.strip())
            st.success("SMTP settings saved.")

    if admin_section == "Notifications":
        st.markdown("#### Notification Log")
        notification_df = get_notification_log()
        if notification_df.empty:
            st.info("No notifications recorded yet.")
        else:
            notification_df["created_at"] = pd.to_datetime(notification_df["created_at"], errors="coerce")
            st.dataframe(notification_df, use_container_width=True, hide_index=True)


def render_user_dashboard(user: dict[str, Any]) -> None:
    categories = get_categories_for_user(user["id"])
    transactions = get_transactions_for_user(user["id"])

    header_columns = st.columns([4, 1])
    with header_columns[0]:
        render_hero_header(user, transactions)
    with header_columns[1]:
        st.write("")
        st.write("")
        if st.button("Logout", use_container_width=True):
            st.session_state.pop("user_id", None)
            st.rerun()

    flash_message = st.session_state.pop("flash_message", None)
    if flash_message:
        st.success(flash_message)

    if transactions.empty:
        st.info("Add your first expense or credit from the Add tab to populate your dashboard.")

    filtered_transactions = render_filter_panel(transactions)

    if user["role"] == "developer":
        dashboard_section = render_section_navigation(
            ["Overview", "Add", "Insights", "Transactions", "Settings", "Developer Admin"],
            DASHBOARD_SECTION_KEY,
        )
        if dashboard_section == "Overview":
            render_google_ads(user)
            render_metrics(filtered_transactions)
            render_quick_insights(filtered_transactions)
            render_period_stats(transactions)
            render_recent_transactions_cards(filtered_transactions)
        if dashboard_section == "Add":
            render_add_transaction_section(user, categories)
        if dashboard_section == "Insights":
            render_pie_chart(filtered_transactions)
            render_monthly_trends(filtered_transactions)
        if dashboard_section == "Transactions":
            render_transactions_table(filtered_transactions, user)
        if dashboard_section == "Settings":
            render_category_manager(user)
            render_recap_section(user)
        if dashboard_section == "Developer Admin":
            render_admin_panel(user)
        return

    dashboard_section = render_section_navigation(
        ["Overview", "Add", "Insights", "Transactions", "Settings"],
        DASHBOARD_SECTION_KEY,
    )

    if dashboard_section == "Overview":
        render_google_ads(user)
        render_metrics(filtered_transactions)
        render_quick_insights(filtered_transactions)
        render_period_stats(transactions)
        render_recent_transactions_cards(filtered_transactions)

    if dashboard_section == "Add":
        render_add_transaction_section(user, categories)

    if dashboard_section == "Insights":
        render_pie_chart(filtered_transactions)
        render_monthly_trends(filtered_transactions)

    if dashboard_section == "Transactions":
        render_transactions_table(filtered_transactions, user)

    if dashboard_section == "Settings":
        render_category_manager(user)
        render_recap_section(user)


def main() -> None:
    st.set_page_config(
        page_title="Expense and Credit Tracker",
        page_icon="$",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    apply_custom_styles()
    init_db()

    current_user = None
    user_id = st.session_state.get("user_id")
    if user_id:
        current_user = get_user_by_id(user_id)
        if not current_user or not current_user["is_active"]:
            st.session_state.pop("user_id", None)
            current_user = None

    if not current_user:
        render_login_screen()
        return

    render_user_dashboard(current_user)


if __name__ == "__main__":
    main()
