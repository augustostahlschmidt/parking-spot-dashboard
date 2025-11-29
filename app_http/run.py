import os
import time
import webbrowser
import subprocess
import sys

SERVER_URL = "http://localhost:8000"


def start_uvicorn():
    print("[SERVER] Starting uvicorn...")

    process = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn",
            "app:app",
            "--host", "0.0.0.0",
            "--port", "8000",
        ]
    )

    # Wait a moment for the server to lift
    time.sleep(2)
    return process


def open_browser():
    print("[BROWSER] Opening dashboard...")
    webbrowser.open(SERVER_URL)

def start_sensor_simulator():
    """
    Abre uma nova janela de terminal (cmd) e executa:
      <python> <path_to>/sensor_simulator.py
    Usa caminhos absolutos para evitar problemas de cwd.
    """
    script_path = os.path.join(os.path.dirname(__file__), "sensor_simulator.py")
    if not os.path.exists(script_path):
        print("[SIMULATOR] sensor_simulator.py não encontrado em:", script_path)
        return

    python_exe = sys.executable

    # Comando: cmd /c start "" <python_exe> "<script_path>"
    # Note o "" logo após start: título vazio (obrigatório se o próximo arg estiver entre aspas)
    cmd = ["cmd", "/c", "start", "", python_exe, script_path]

    try:
        subprocess.Popen(cmd, shell=False)
        print(f"[SIMULATOR] Iniciado em nova janela: {script_path}")
    except Exception as e:
        print("[SIMULATOR] Erro ao iniciar sensor_simulator:", e)

if __name__ == "__main__":
    print("=== RUN.PY: Starting Dashboard ===")

    try:
        uvicorn_process = start_uvicorn()
        open_browser()

        # Start the simulator in a new terminal session
        start_sensor_simulator()

        print("=== Dashboard loaded! Server logs below ===\n")

        uvicorn_process.wait()

    except KeyboardInterrupt:
        print("\n[EXIT] Shutting down server…")
        try:
            uvicorn_process.terminate()
        except:
            pass
        sys.exit(0)
