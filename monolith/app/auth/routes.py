import os
import sys
import subprocess
import requests
import docker
from flask import Blueprint, render_template, redirect, url_for, flash, request, jsonify
from flask_login import login_user, logout_user, login_required, current_user
from ..forms import RegisterForm, LoginForm
from ..extensions import db
from ..models import User

bp = Blueprint("auth", __name__, url_prefix="/auth")

# ── Auth Routes ────────────────────────────────────────────────────────────────
@bp.route("/register", methods=["GET", "POST"])
def register():
    if current_user.is_authenticated:
        return redirect(url_for("shop.index"))
    form = RegisterForm()
    if form.validate_on_submit():
        if User.query.filter_by(email=form.email.data).first():
            flash("Email already registered", "warning")
        else:
            u = User(email=form.email.data, name=form.name.data)
            u.set_password(form.password.data)
            db.session.add(u)
            db.session.commit()
            flash("Account created. Please login.", "success")
            return redirect(url_for("auth.login"))
    return render_template("auth/register.html", form=form)

@bp.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("shop.index"))
    form = LoginForm()
    if form.validate_on_submit():
        u = User.query.filter_by(email=form.email.data).first()
        if u and u.check_password(form.password.data):
            login_user(u)
            flash("Welcome back!", "success")
            return redirect(url_for("shop.index"))
        flash("Invalid credentials", "danger")
    return render_template("auth/login.html", form=form)

@bp.route("/logout")
@login_required
def logout():
    logout_user()
    flash("Logged out successfully.", "info")
    return redirect(url_for("shop.index"))


# ── Admin Dashboard ────────────────────────────────────────────────────────────
@bp.route("/admin/dashboard")
def admin_dashboard():
    return render_template("admin_dashboard.html")


# ── API: System Stats ──────────────────────────────────────────────────────────
@bp.route("/api/system-stats")
def system_stats():
    TARGET_QUEUES = {"stock_queue", "order_queue", "notif_queue", "order_db_write_queue"}
    total_messages = 0
    queue_details = [] 
    
    # --- YENİ: Gerçek Docker Worker Sayımlarını Al ---
    worker_counts = {"stock": 0, "order": 0, "notif": 0, "db": 0}
    try:
        client = docker.from_env()
        for c in client.containers.list():
            name = c.name.lower()
            if "pumba" in name: continue # Geciktiricileri atla
            if "stock_worker" in name: worker_counts["stock"] += 1
            elif "order_worker" in name: worker_counts["order"] += 1
            elif "notif_worker" in name: worker_counts["notif"] += 1
            elif "db_worker" in name: worker_counts["db"] += 1
    except Exception as e:
        print("Docker connection error:", e)
    # --------------------------------------------------

    try:
        rabbitmq_url = "http://guest:guest@localhost:15672/api/queues"
        resp = requests.get(rabbitmq_url, timeout=2).json()

        for q in resp:
            if q.get("name") in TARGET_QUEUES:
                ready = q.get("messages_ready", 0)
                total_messages += ready
                queue_details.append({
                    "name": q["name"],
                    "messages_ready": ready,
                    "consumers": q.get("consumers", 0)
                })
    except Exception as e:
        pass

    scaler_proc = _running_processes.get("autoscaler")
    scaler_status = "running" if scaler_proc and scaler_proc.poll() is None else "stopped"

    stress_proc = _running_processes.get("stress_test")
    stress_status = "running" if stress_proc and stress_proc.poll() is None else "idle"

    return jsonify({
        "status": "Online",
        "pending_messages": total_messages,
        "worker_counts": worker_counts,  # YENİ EKLENDİ
        "queue_details": queue_details,  
        "autoscaler": scaler_status,
        "stress_test": stress_status,
        "region_health": {
            "London_Stock": "OK",
            "Rome_Order": "OK",
            "Tokyo_Notif": "OK",
            "Istanbul_DB": "OK",
        },
    })


# ── Helpers ────────────────────────────────────────────────────────────────────
def _project_root() -> str:
    return os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..")
    )

_running_processes: dict[str, subprocess.Popen] = {}

def _launch(name: str, script_relative: str):
    prev = _running_processes.get(name)
    if prev is not None and prev.poll() is None:
        return None, f"{name} is already running (pid {prev.pid})"

    script_path = os.path.join(_project_root(), script_relative)

    if not os.path.isfile(script_path):
        return None, f"Script not found: {script_path}"

    proc = subprocess.Popen(
        [sys.executable, script_path],
        cwd=_project_root()
    )
    
    _running_processes[name] = proc
    return proc, None


# ── API: Commands ─────────────────────────────────────────────────────
@bp.route("/api/start-stress", methods=["POST"])
def start_stress():
    proc, err = _launch("stress_test", "stress_test.py")
    if err: return jsonify({"status": "error", "message": err}), 400
    return jsonify({"status": "success", "pid": proc.pid})

@bp.route("/api/stop-stress", methods=["POST"])
def stop_stress():
    proc = _running_processes.get("stress_test")
    if proc is None or proc.poll() is not None:
        return jsonify({"status": "error", "message": "Stress test is not running"}), 400
    proc.terminate()
    return jsonify({"status": "success", "message": "Stress test stopped"})

@bp.route("/api/start-autoscaler", methods=["POST"])
def start_autoscaler():
    proc, err = _launch("autoscaler", "autoscaler.py")
    if err: return jsonify({"status": "error", "message": err}), 400
    return jsonify({"status": "success", "pid": proc.pid})

@bp.route("/api/stop-autoscaler", methods=["POST"])
def stop_autoscaler():
    proc = _running_processes.get("autoscaler")
    if proc is None or proc.poll() is not None:
        return jsonify({"status": "error", "message": "Autoscaler is not running"}), 400
    proc.terminate()
    return jsonify({"status": "success", "message": "Autoscaler stopped"})