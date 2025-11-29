import os
import time
import webbrowser
import subprocess
import urllib.request
import sys

SERVER_URL = "http://localhost:8000"

def start_uvicorn():
    print("[SERVER] Iniciando servidor uvicorn...")

    process = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    print("[SERVER] Aguardando inicialização...")
    time.sleep(2)
    return 

def open_browser():
    print("[BROWSER] Abrindo dashboard no navegador...")
    webbrowser.open(SERVER_URL)


if __name__ == "__main__":
    try:
        print("=== RUN.PY: Inicialização do Dashboard ===")
        process = start_uvicorn()
        open_browser()
        print("=== Dashboard carregado com sucesso! ===")

    except AssertionError as e:
        print(str(e))
        print("\n⚠ Encerrando servidor...")
        try:
            process.terminate()
        except:
            pass
        sys.exit(1)

    except KeyboardInterrupt:
        print("\n[EXIT] Encerrando servidor...")
        try:
            process.terminate()
        except:
            pass
        sys.exit(0)
