"""
Launch the Streamlit frontend
==============================

Usage:
    python scripts/run_frontend.py          # default port 8501
    python scripts/run_frontend.py 8080     # custom port
"""

import os
import sys
import subprocess

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
frontend_app = os.path.join(project_root, "frontend", "chat.py")


def main() -> None:
    port = sys.argv[1] if len(sys.argv) > 1 else "8501"
    cmd = [
        sys.executable, "-m", "streamlit", "run",
        frontend_app,
        "--server.port", port,
        "--server.headless", "true",
    ]
    print(f"Starting Streamlit on port {port} …")
    subprocess.run(cmd, cwd=project_root)


if __name__ == "__main__":
    main()
