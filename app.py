from flask import Flask, render_template, request, redirect, session, jsonify, send_file
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from datetime import datetime
from io import BytesIO
import sqlite3
import pandas as pd
import os
import json

# =========================================================
# APP CONFIG
# =========================================================
app = Flask(__name__)
app.secret_key = "power-dashboard-secret"

UPLOAD_FOLDER = "static/uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

DB_PATH = "power.db"

# =========================================================
# DATABASE
# =========================================================
def db():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

# =========================================================
# INIT DATABASE (ONCE)
# =========================================================
def init_db():
    with db() as d:
        cur = d.cursor()

        # ---------- COMPANIES ----------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS pd_companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_code TEXT UNIQUE,
            company_name TEXT,
            status TEXT
        )
        """)

        # ---------- USERS ----------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS pd_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER,
            username TEXT,
            password TEXT,
            role TEXT
        )
        """)

        # ---------- METERS ----------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS meters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER,
            meter_id TEXT,
            load_type TEXT,
            location TEXT,
            unit TEXT
        )
        """)

        # ---------- READINGS ----------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER,
            meter_id TEXT,
            date TEXT,
            opening REAL,
            closing REAL,
            consumption REAL,
            entered_by TEXT,
            employee_id TEXT,
            image TEXT
        )
        """)

        # ---------- ALERTS ----------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER,
            meter_id TEXT,
            date TEXT,
            consumption REAL,
            average REAL,
            percentage REAL,
            status TEXT
        )
        """)

        d.commit()

init_db()

# =========================================================
# LOGIN
# =========================================================
@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        company_code = request.form["company_code"]
        username = request.form["username"]
        password = request.form["password"]

        with db() as d:
            cur = d.cursor()
            cur.execute("""
                SELECT u.id, u.password, u.role, c.id, c.status, c.company_name
                FROM pd_users u
                JOIN pd_companies c ON u.company_id = c.id
                WHERE c.company_code=? AND u.username=?
            """, (company_code, username))
            row = cur.fetchone()

        if not row:
            return render_template("login.html", error="Invalid credentials")

        user_id, pw_hash, role, company_id, status, company_name = row

        if status != "ACTIVE":
            return render_template("login.html", error="Company suspended")

        if not check_password_hash(pw_hash, password):
            return render_template("login.html", error="Wrong password")

        session["pd_user"] = user_id
        session["pd_role"] = role
        session["pd_company_id"] = company_id
        session["pd_company_name"] = company_name

        return redirect("/dashboard")

    return render_template("login.html")

# =========================================================
# DASHBOARD
# =========================================================
@app.route("/dashboard")
def dashboard():
    if not session.get("pd_user"):
        return redirect("/")

    return render_template(
        "dashboard.html",
        role=session["pd_role"],
        company=session["pd_company_name"]
    )

# =========================================================
# ADD METER
# =========================================================
@app.route("/add_meter", methods=["GET", "POST"])
def add_meter():
    if not session.get("pd_company_id"):
        return redirect("/")

    if request.method == "POST":
        with db() as d:
            d.execute("""
                INSERT INTO meters
                (company_id, meter_id, load_type, location, unit)
                VALUES (?,?,?,?,?)
            """, (
                session["pd_company_id"],
                request.form["meter_id"],
                request.form["load_type"],
                request.form["location"],
                request.form["unit"]
            ))
            d.commit()
        return redirect("/meters")

    return render_template("add_meter.html")

# =========================================================
# METERS LIST
# =========================================================
@app.route("/meters")
def meters():
    if not session.get("pd_company_id"):
        return redirect("/")

    with db() as d:
        rows = d.execute("""
            SELECT * FROM meters
            WHERE company_id=?
        """, (session["pd_company_id"],)).fetchall()

    return render_template("meters.html", meters=rows)

# =========================================================
# ADD READING
# =========================================================
@app.route("/add_reading", methods=["GET", "POST"])
def add_reading():
    if not session.get("pd_company_id"):
        return redirect("/")

    with db() as d:
        meters_list = d.execute("""
            SELECT meter_id FROM meters
            WHERE company_id=?
        """, (session["pd_company_id"],)).fetchall()

        if request.method == "POST":
            opening = float(request.form["opening"])
            closing = float(request.form["closing"])
            consumption = closing - opening

            image = None
            f = request.files.get("image")
            if f and f.filename:
                ext = os.path.splitext(f.filename)[1]
                image = secure_filename(
                    f"{request.form['meter_id']}_{datetime.now().strftime('%Y%m%d%H%M%S')}{ext}"
                )
                f.save(os.path.join(app.config["UPLOAD_FOLDER"], image))

            d.execute("""
                INSERT INTO readings
                (company_id, meter_id, date, opening, closing, consumption,
                 entered_by, employee_id, image)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (
                session["pd_company_id"],
                request.form["meter_id"],
                datetime.now().strftime("%Y-%m-%d %H:%M"),
                opening, closing, consumption,
                request.form["entered_by"],
                request.form["employee_id"],
                image
            ))
            d.commit()

            return redirect("/readings")

    return render_template("add_reading.html", meters=meters_list)

# =========================================================
# READINGS
# =========================================================
@app.route("/readings")
def readings():
    if not session.get("pd_company_id"):
        return redirect("/")

    with db() as d:
        rows = d.execute("""
            SELECT * FROM readings
            WHERE company_id=?
            ORDER BY date DESC
        """, (session["pd_company_id"],)).fetchall()

    return render_template("readings.html", rows=rows)

# =========================================================
# EXPORT ALL
# =========================================================
@app.route("/export_all")
def export_all():
    if not session.get("pd_company_id"):
        return redirect("/")

    df = pd.read_sql("""
        SELECT * FROM readings
        WHERE company_id=?
    """, db(), params=(session["pd_company_id"],))

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)

    output.seek(0)
    return send_file(output, as_attachment=True,
                     download_name="meter_readings.xlsx")

# =========================================================
# LOGOUT
# =========================================================
@app.route("/logout")
def logout():
    session.clear()
    return redirect("/")

# =========================================================
# RUN
# =========================================================
if __name__ == "__main__":
    app.run(debug=True)