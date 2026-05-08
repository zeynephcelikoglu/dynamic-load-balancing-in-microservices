import os
import subprocess
from app import create_app
from seed import seed_data 

app = create_app()

def auto_migrate_and_seed():
    """Run migrations and seeders automatically when app starts."""

    if not os.path.exists("migrations"):
        subprocess.call(["flask", "--app", "run.py", "db", "init"])

    subprocess.call(["flask", "--app", "run.py", "db", "migrate", "-m", "auto migration"])
    subprocess.call(["flask", "--app", "run.py", "db", "upgrade"])

    try:
        seed_data()  
    except Exception as e:
        print("Seeder skipped or failed:", e)

if __name__ == "__main__":
    auto_migrate_and_seed()
    app.run(debug=True, port=5000)