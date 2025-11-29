import os
import time
import webbrowser
import subprocess
import threading
import sys
import re

SERVER_URL = "http://localhost:8000"
PORT = 8000


# ---------------------------------------
# 1) Verifica e mata processo da porta
# ---------------------------------------
def kill_process_on_port(port):
    print(f"[CHECK] Verificando processos na porta {port}...")

    try:
        result = subprocess.check_output(
            f'netstat -ano | findstr :{port}',
            shell=True, text=True, stderr=subprocess.STDOUT
        )
    except subprocess.CalledProcessError:
        print(f"[CHECK] Porta {port} está livre.")
        return

    pids = set()

    for line in result.splitlines():
        parts = line.split()
        if len(parts) >= 5:
            pid = parts[-1]
            if pid.isdigit():
                pids.add(pid)

    if not pids:
        print(f"[CHECK] Porta {port} está livre.")
        return

    print(f"[KILL] Encontrados processos usando a porta {port}: {pids}")

    for pid in pids:
        try:
            print(f"[KILL] Matando PID {pid} ...")
            subprocess.run(f"taskkill /PID {pid} /F", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except Exception as e:
            print(f"[KILL] Erro ao matar PID {pid}: {e}")

    print(f"[KILL] Porta {port} está agora liberada.\n")


# ---------------------------------------
# 2) Stream seguro dos logs
# ---------------------------------------
def stream_output(pipe, prefix):
    encoding = sys.stdout.encoding or "utf-8"

    for line in iter(pipe.readline, b""):
        text = line.decode(encoding, errors="replace")
        sys.stdout.write(f"[{prefix}] {text}")
    pipe.close()


# ---------------------------------------
# 3) Start do Uvicorn
# ---------------------------------------
def start_uvicorn():
    print("[SERVER] Iniciando servidor uvicorn...")

    process = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn", "app:app",
            "--host", "0.0.0.0",
            "--port", str(PORT)
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    print("[SERVER] Aguardando inicialização...")
    time.sleep(2)

    threading.Thread(target=stream_output, args=(process.stdout, "UVICORN"), daemon=True).start()
    threading.Thread(target=stream_output, args=(process.stderr, "UVICORN-ERR"), daemon=True).start()

    return process


# ---------------------------------------
# 4) Abre navegador
# ---------------------------------------
def open_browser():
    print("[BROWSER] Abrindo dashboard no navegador...")
    webbrowser.open(SERVER_URL)


# ---------------------------------------
# MAIN
# ---------------------------------------
if __name__ == "__main__":
    try:
        print("=== RUN.PY: Inicialização do Dashboard ===\n")

        # Mata processos ocupando a porta
        kill_process_on_port(PORT)

        # Inicia servidor
        process = start_uvicorn()

        # Abre navegador
        open_browser()

        print("\n=== Dashboard carregado! Logs abaixo ===\n")

        # Mantém logs fluindo
        process.wait()

    except KeyboardInterrupt:
        print("\n[EXIT] Encerrando servidor...")
        try:
            process.terminate()
        except:
            pass
        sys.exit(0)