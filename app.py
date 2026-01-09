import json
import sqlite3
import os
import pandas as pd
from flask import Flask, render_template, request, redirect, jsonify, send_file
from datetime import datetime
from werkzeug.utils import secure_filename

app = Flask(__name__)

UPLOAD_FOLDER = "static/uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

# ---------- DATABASE ----------
DB_PATH = "power.db"

def db():
    # Use check_same_thread=False for SQLite in Flask
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    with db() as d:
        d.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            meter_id TEXT,
            date TEXT,
            consumption REAL,
            average REAL,
            percentage REAL,
            status TEXT
        )""")
        d.execute("""
        CREATE TABLE IF NOT EXISTS meters(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            meter_id TEXT UNIQUE,
            load_type TEXT,
            location TEXT,
            unit TEXT
        )""")
        d.execute("""
        CREATE TABLE IF NOT EXISTS readings(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            meter_id TEXT,
            date TEXT,
            opening REAL,
            closing REAL,
            consumption REAL,
            entered_by TEXT,
            employee_id TEXT,
            image TEXT
        )""")
        d.commit()

init_db()

def check_abnormal(meter_id):
    d = db()
    try:
        # Last reading
        last = d.execute("""
            SELECT consumption FROM readings
            WHERE meter_id=?
            ORDER BY id DESC LIMIT 1
        """, (meter_id,)).fetchone()

        if not last: return

        today_val = last[0]

        # Average of last 7 readings (excluding today)
        avg_row = d.execute("""
            SELECT AVG(consumption) FROM (
                SELECT consumption FROM readings
                WHERE meter_id=?
                ORDER BY id DESC LIMIT 7 OFFSET 1
            )
        """, (meter_id,)).fetchone()

        avg = avg_row[0] if avg_row and avg_row[0] is not None else 0

        if avg <= 0: return

        percent = ((today_val - avg) / avg) * 100

        if percent >= 30:   # Threshold
            d.execute("""
                INSERT INTO alerts
                (meter_id, date, consumption, average, percentage, status)
                VALUES (?,?,?,?,?,?)
            """, (
                meter_id,
                datetime.now().strftime("%Y-%m-%d %H:%M"),
                today_val,
                round(avg, 2),
                round(percent, 2),
                "OPEN"
            ))
            d.commit()
    finally:
        d.close()

# ---------- HOME ----------
@app.route("/")
def home():
    d = db()
    recent_alerts = d.execute("""
    SELECT * FROM alerts
    WHERE status!='CLOSED'
    ORDER BY id DESC LIMIT 5
""").fetchall()
    try:
        # Get count of open alerts first
        open_alerts = d.execute(
            "SELECT COUNT(*) FROM alerts WHERE status='OPEN'"
        ).fetchone()[0]

        # KPI DATA
        total_meters = d.execute("SELECT COUNT(*) FROM meters").fetchone()[0]
        total_readings = d.execute("SELECT COUNT(*) FROM readings").fetchone()[0]

        today = datetime.now().strftime("%Y-%m-%d")
        month = datetime.now().strftime("%Y-%m")

        today_consumption = d.execute(
            "SELECT IFNULL(SUM(consumption),0) FROM readings WHERE date LIKE ?",
            (today+"%",)
        ).fetchone()[0]

        month_consumption = d.execute(
            "SELECT IFNULL(SUM(consumption),0) FROM readings WHERE date LIKE ?",
            (month+"%",)
        ).fetchone()[0]

        # DAILY GRAPH (LAST 7 DAYS)
        daily = d.execute("""
            SELECT SUBSTR(date,1,10), SUM(consumption)
            FROM readings
            GROUP BY SUBSTR(date,1,10)
            ORDER BY SUBSTR(date,1,10) DESC
            LIMIT 7
        """).fetchall()

        daily.reverse()
        daily_labels = [r[0] for r in daily]
        daily_values = [round(r[1], 2) for r in daily]

        # MONTHLY GRAPH
        monthly = d.execute("""
            SELECT SUBSTR(date,1,7), SUM(consumption)
            FROM readings
            WHERE date LIKE ?
            GROUP BY SUBSTR(date,1,7)
        """, (month+"%",)).fetchall()

        month_labels = [r[0] for r in monthly]
        month_values = [round(r[1], 2) for r in monthly]

        return render_template(
            "home.html",
            open_alerts=open_alerts,
            total_meters=total_meters,
            total_readings=total_readings,
            today_consumption=round(today_consumption, 2),
            month_consumption=round(month_consumption, 2),
            daily_labels=json.dumps(daily_labels),
            daily_values=json.dumps(daily_values),
            month_labels=json.dumps(month_labels),
            month_values=json.dumps(month_values)
        )
    finally:
        d.close()

# ---------- ADD METER ----------
@app.route("/add_meter", methods=["GET","POST"])
def add_meter():
    if request.method == "POST":
        d = db()
        try:
            d.execute("""
            INSERT INTO meters (meter_id, load_type, location, unit)
            VALUES (?,?,?,?)
            """, (
                request.form["meter_id"],
                request.form["load_type"],
                request.form["location"],
                request.form["unit"]
            ))
            d.commit()
        finally:
            d.close()
        return redirect("/meters")
    return render_template("add_meter.html")

# ---------- METER LIST ----------
@app.route("/meters")
def meters():
    d = db()
    try:
        rows = d.execute("SELECT * FROM meters").fetchall()
    finally:
        d.close()
    return render_template("meters.html", meters=rows)

# ---------- FETCH OPENING ----------
@app.route("/get_opening/<meter_id>")
def get_opening(meter_id):
    d = db()
    try:
        last = d.execute(
            "SELECT closing FROM readings WHERE meter_id=? ORDER BY id DESC LIMIT 1",
            (meter_id,)
        ).fetchone()
    finally:
        d.close()
    return jsonify({"opening": last[0] if last else 0})

# ---------- ADD READING ----------
@app.route("/add_reading", methods=["GET","POST"])
def add_reading():
    d = db()
    try:
        meters_list = d.execute("SELECT meter_id FROM meters").fetchall()

        if request.method == "POST":
            opening = float(request.form["opening"])
            closing = float(request.form["closing"])
            consumption = closing - opening

            image = None
            f = request.files.get("image")
            if f and f.filename:
                # Use secure_filename properly
                ext = os.path.splitext(f.filename)[1]
                image = secure_filename(f"{request.form['meter_id']}_{datetime.now().strftime('%Y%m%d%H%M%S')}{ext}")
                f.save(os.path.join(app.config["UPLOAD_FOLDER"], image))

            d.execute("""
            INSERT INTO readings
            (meter_id,date,opening,closing,consumption,entered_by,employee_id,image)
            VALUES (?,?,?,?,?,?,?,?)
            """, (
                request.form["meter_id"],
                datetime.now().strftime("%Y-%m-%d %H:%M"),
                opening, closing, consumption,
                request.form["entered_by"],
                request.form["employee_id"],
                image
            ))
            d.commit()
            check_abnormal(request.form["meter_id"])
            return redirect("/readings")
    finally:
        d.close()

    return render_template("add_reading.html", meters=meters_list)

# ---------- ALL READINGS ----------
@app.route("/readings")
def readings():
    d = db()
    try:
        rows = d.execute("SELECT * FROM readings ORDER BY date DESC").fetchall()
    finally:
        d.close()
    return render_template("readings.html", rows=rows)

# ---------- METER DETAIL ----------
@app.route("/meter/<meter_id>")
def meter_detail(meter_id):
    d = db()
    try:
        alerts = d.execute("""
            SELECT * FROM alerts
            WHERE meter_id=? AND status='OPEN'
        """, (meter_id,)).fetchall()
        meter = d.execute("SELECT * FROM meters WHERE meter_id=?", (meter_id,)).fetchone()
        readings_list = d.execute(
            "SELECT * FROM readings WHERE meter_id=? ORDER BY date DESC",
            (meter_id,)
        ).fetchall()
    finally:
        d.close()
    return render_template("meter_detail.html", meter=meter, readings=readings_list, alerts=alerts)

# ---------- EXPORT PER METER ----------
@app.route("/export/meter/<meter_id>")
def export_meter(meter_id):
    with db() as d:
        df = pd.read_sql(
            "SELECT date, opening, closing, consumption, entered_by, employee_id "
            "FROM readings WHERE meter_id=? ORDER BY date",
            d, params=(meter_id,)
        )
    file_path = f"{meter_id}_history.xlsx"
    df.to_excel(file_path, index=False)
    return send_file(file_path, as_attachment=True)

# ---------- EXPORT ALL ----------
@app.route("/export_all")
def export_all():
    with db() as d:
        df = pd.read_sql("SELECT * FROM readings", d)
    file_path = "all_meter_readings.xlsx"
    df.to_excel(file_path, index=False)
    return send_file(file_path, as_attachment=True)
# ---------- ACKNOWLEDGE ALERT ----------
@app.route("/alert/ack/<int:alert_id>")
def acknowledge_alert(alert_id):
    d = db()
    d.execute("""
        UPDATE alerts
        SET status='ACKNOWLEDGED',
            acknowledged_by='Operator',
            acknowledged_at=?
        WHERE id=?
    """, (
        datetime.now().strftime("%Y-%m-%d %H:%M"),
        alert_id
    ))
    d.commit()
    return redirect(request.referrer or "/")

# ---------- CLOSE ALERT ----------
@app.route("/alert/close/<int:alert_id>")
def close_alert(alert_id):
    d = db()
    d.execute("""
        UPDATE alerts
        SET status='CLOSED',
            closed_by='Operator',
            closed_at=?
        WHERE id=?
    """, (
        datetime.now().strftime("%Y-%m-%d %H:%M"),
        alert_id
    ))
    d.commit()
    return redirect(request.referrer or "/")    

if __name__ == "__main__":
    app.run()
