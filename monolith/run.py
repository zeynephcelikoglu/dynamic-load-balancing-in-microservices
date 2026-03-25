import os
import subprocess
from app import create_app
from app.seed import run as seed_run

app = create_app()

def auto_migrate_and_seed():
    """Run migrations and seeders automatically when app starts."""
    # Ensure migrations folder exists
    if not os.path.exists("migrations"):
        subprocess.call(["flask", "--app", "run.py", "db", "init"])

    # Run migrations
    subprocess.call(["flask", "--app", "run.py", "db", "migrate", "-m", "auto migration"])
    subprocess.call(["flask", "--app", "run.py", "db", "upgrade"])

    # Run seeders
    try:
        seed_run()
    except Exception as e:
        print("Seeder skipped or failed:", e)

if __name__ == "__main__":
    auto_migrate_and_seed()
    app.run(debug=True)
