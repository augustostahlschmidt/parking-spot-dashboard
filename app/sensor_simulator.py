#!/usr/bin/env python3
"""
sensor_simulator.py

Simula sensores de estacionamento e envia leituras para POST /readings do app.

Uso:
  python sensor_simulator.py            # roda com defaults (10 vagas, 1s)
  python sensor_simulator.py --slots 6 --interval 0.5 --mode sequential
"""
import time
import random
import argparse
import requests
from datetime import datetime

API_URL_DEFAULT = "http://localhost:8000/readings"

def iso_now():
    return datetime.utcnow().isoformat() + "Z"

class SlotSimulator:
    def __init__(self, slot_id, base_empty=120.0, base_occupied=20.0, jitter=5.0):
        """base_empty/base_occupied em cm; jitter é ruído adicionado"""
        self.slot_id = slot_id
        self.base_empty = base_empty
        self.base_occupied = base_occupied
        self.jitter = jitter
        # start random state
        self.occupied = random.random() < 0.3

    def sample_distance(self):
        """Retorna uma distância em cm simulada dependendo do estado atual."""
        if self.occupied:
            d = random.gauss(self.base_occupied, self.jitter)
            # às vezes carro mexe um pouco
            if random.random() < 0.02:
                d += random.uniform(5, 30)
        else:
            d = random.gauss(self.base_empty, self.jitter * 3)
            # às vezes um carro passa e fecha a vaga brevemente
            if random.random() < 0.03:
                d = random.gauss(self.base_occupied + 5, self.jitter)
        return max(1.0, float(d))

    def maybe_flip_state(self, flip_prob=0.01):
        """Opcionalmente muda o estado para criar eventos reais de ocupar/liberar."""
        if random.random() < flip_prob:
            self.occupied = not self.occupied

def send_reading(api_url, slot_id, distance_cm, timeout=3):
    payload = {
        "device_id": slot_id,
        "timestamp": iso_now(),
        "distance_cm": float(distance_cm)
    }
    try:
        r = requests.post(api_url, json=payload, timeout=timeout)
        return r.status_code, r.text
    except requests.RequestException as e:
        return None, str(e)

def run_simulator(api_url, slots=10, interval=1.0, mode="random"):
    # cria instâncias para cada slot
    sims = {f"slot-{i+1}": SlotSimulator(f"slot-{i+1}") for i in range(slots)}
    slot_keys = list(sims.keys())
    print(f"Simulator: sending to {api_url} - slots={slots} interval={interval}s mode={mode}\nPress Ctrl+C to stop\n")

    idx = 0
    try:
        while True:
            if mode == "random":
                slot = random.choice(slot_keys)
            else:  # sequential
                slot = slot_keys[idx % len(slot_keys)]
                idx += 1

            sim = sims[slot]
            # chance pequena de mudar estado (carro entra/sai)
            sim.maybe_flip_state(flip_prob=0.02)

            dist = sim.sample_distance()
            status_code, resp = send_reading(api_url, slot, dist)
            now = datetime.now().strftime("%H:%M:%S")
            if status_code is None:
                print(f"[{now}] -> {slot} distance={dist:.1f}cm  -> ERROR: {resp}")
            else:
                print(f"[{now}] -> {slot} distance={dist:.1f}cm  -> {status_code} {resp}")

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\nSimulator stopped by user.")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--api", default=API_URL_DEFAULT, help="URL do endpoint /readings")
    p.add_argument("--slots", type=int, default=10, help="Número de vagas/sensores")
    p.add_argument("--interval", type=float, default=1.0, help="Intervalo entre envios (segundos)")
    p.add_argument("--mode", choices=["random", "sequential"], default="random", help="Escolha envio random ou sequencial")
    args = p.parse_args()

    run_simulator(api_url=args.api, slots=args.slots, interval=args.interval, mode=args.mode)
