#!/usr/bin/env python3
"""
run_mqtt.py

Start sequence (Windows-focused):
 - try start mosquitto (binary -> new window) or docker
 - start uvicorn pointing to module `app_mqtt:app` with cwd set to THIS_DIR
 - open browser to dashboard
 - start sensor_simulator_mqtt.py in new terminal (mqtt mode)
 - stream uvicorn logs to this console to show import tracebacks/errors
"""
import os
import time
import webbrowser
import subprocess
import sys
import shutil
import threading

THIS_DIR = os.path.dirname(__file__) or os.getcwd()
SERVER_URL = "http://localhost:8000"
PORT = 8000
# <- UPDATED: simulator filename points to the MQTT simulator
SIMULATOR_SCRIPT = os.path.join(THIS_DIR, "sensor_simulator_mqtt.py")
MOSQUITTO_DOCKER_NAME = "mosquitto_sim"


def _stream_pipe(pipe, prefix):
    """Read binary lines from pipe and print decoded (safe)."""
    enc = sys.stdout.encoding or "utf-8"
    try:
        for line in iter(pipe.readline, b""):
            try:
                text = line.decode(enc, errors="replace")
            except Exception:
                text = line.decode("utf-8", errors="replace")
            sys.stdout.write(f"[{prefix}] {text}")
    finally:
        try:
            pipe.close()
        except Exception:
            pass


def start_uvicorn():
    """
    Start uvicorn using module 'app_mqtt:app' with cwd=THIS_DIR.
    Streams stdout/stderr so you can see import errors.
    """
    print("[SERVER] Starting uvicorn (module: app_mqtt:app)...")
    cmd = [
        sys.executable, "-m", "uvicorn",
        "app_mqtt:app",
        "--host", "0.0.0.0",
        "--port", str(PORT),
    ]

    # start process with pipes so we can stream logs
    proc = subprocess.Popen(
        cmd,
        cwd=THIS_DIR,                 # ensure Python can import app_mqtt from THIS_DIR
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # start threads to stream output
    threading.Thread(target=_stream_pipe, args=(proc.stdout, "UVICORN"), daemon=True).start()
    threading.Thread(target=_stream_pipe, args=(proc.stderr, "UVICORN-ERR"), daemon=True).start()

    # give some time to attempt startup (but streaming threads will show later logs)
    time.sleep(2)
    return proc


def open_browser():
    print("[BROWSER] Opening dashboard...")
    webbrowser.open(SERVER_URL)


def start_mosquitto_via_binary():
    """Open mosquitto in a new cmd window (requires mosquitto on PATH)."""
    print("[BROKER] mosquitto binary found. Starting broker in new window...")
    try:
        subprocess.Popen(["cmd", "/c", "start", "", "mosquitto", "-v"], shell=False)
        print("[BROKER] mosquitto started (new window).")
        return {"method": "binary"}
    except Exception as e:
        print("[BROKER] Failed to start mosquitto binary:", e)
        return None


def start_mosquitto_via_docker():
    """Run mosquitto in docker detached. Returns container id on success."""
    print("[BROKER] Docker found. Starting mosquitto container...")
    try:
        res = subprocess.check_output(
            ["docker", "run", "-d", "--rm", "--name", MOSQUITTO_DOCKER_NAME, "-p", "1883:1883", "eclipse-mosquitto:2.0"],
            stderr=subprocess.STDOUT,
            text=True,
        ).strip()
        cid = res.splitlines()[-1]
        print(f"[BROKER] Docker container started: {cid}")
        return {"method": "docker", "container_id": cid}
    except subprocess.CalledProcessError as e:
        print("[BROKER] Docker run failed:", e.output)
        return None
    except Exception as e:
        print("[BROKER] Docker start error:", e)
        return None


def ensure_mosquitto():
    """Try binary then docker. If none available instruct user."""
    if shutil.which("mosquitto"):
        return start_mosquitto_via_binary()
    if shutil.which("docker"):
        return start_mosquitto_via_docker()
    print("[BROKER] mosquitto not found and docker not available.")
    print("         Install mosquitto or docker, or ensure a broker is reachable at localhost:1883.")
    return None


def start_sensor_simulator_mqtt():
    """Open a new cmd window and run sensor_simulator_mqtt.py (MQTT simulator)."""
    if not os.path.exists(SIMULATOR_SCRIPT):
        print("[SIMULATOR] sensor_simulator_mqtt.py not found at:", SIMULATOR_SCRIPT)
        return False

    python_exe = sys.executable
    # build command: cmd /c start "" <python> <script> --transport mqtt --broker localhost --port 1883
    cmd = [
        "cmd", "/c", "start", "",
        python_exe, SIMULATOR_SCRIPT,
        "--transport", "mqtt", "--broker", "localhost", "--port", "1883"
    ]
    try:
        subprocess.Popen(cmd, shell=False)
        print("[SIMULATOR] sensor_simulator_mqtt.py started in new window (mqtt).")
        return True
    except Exception as e:
        print("[SIMULATOR] Failed to start sensor_simulator_mqtt.py:", e)
        return False


def stop_docker_container(container_id):
    try:
        print(f"[BROKER] Stopping docker container {container_id} ...")
        subprocess.run(["docker", "stop", container_id], check=False)
        print("[BROKER] Docker container stopped.")
    except Exception as e:
        print("[BROKER] Error stopping docker container:", e)


def main():
    print("=== RUN_MQTT: Starting environment ===\n")
    mosq_info = None
    uvicorn_proc = None

    try:
        mosq_info = ensure_mosquitto()
        print("[BROKER] Waiting a bit for broker to be ready...")
        time.sleep(2)

        uvicorn_proc = start_uvicorn()

        open_browser()

        start_sensor_simulator_mqtt()

        print("\n=== Dashboard loaded! Server logs below ===\n")

        # Wait until uvicorn exits (or is terminated)
        uvicorn_proc.wait()

    except KeyboardInterrupt:
        print("\n[EXIT] KeyboardInterrupt received, shutting down...")

    finally:
        if uvicorn_proc:
            try:
                print("[EXIT] Terminating uvicorn...")
                uvicorn_proc.terminate()
                try:
                    uvicorn_proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    uvicorn_proc.kill()
            except Exception:
                pass

        if mosq_info and mosq_info.get("method") == "docker":
            cid = mosq_info.get("container_id")
            if cid:
                stop_docker_container(cid)
        elif mosq_info and mosq_info.get("method") == "binary":
            print("[BROKER] Mosquitto was started in separate window. Close that window to stop it.")

        print("[EXIT] Done.")


if __name__ == "__main__":
    main()