#!/usr/bin/env python3
"""Local JSON_RIP integration checks driven by nc and real router processes."""

import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import roteador


class RouterProcess:
    def __init__(self, name, port, log_path, log_file):
        self.name = name
        self.port = port
        self.log_path = log_path
        self.log_file = log_file
        self.process = subprocess.Popen(
            [sys.executable, str(ROOT / "roteador.py"), str(port)],
            cwd=str(ROOT),
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )

    def log(self):
        try:
            return self.log_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return ""


def available_port(used):
    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        if port not in used:
            used.add(port)
            return port


def wait_until(predicate, timeout=5, interval=0.05, message="timeout"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError(message)


def send_nc(router, command, tcp=False, data=None):
    before = len(router.log())
    payload = (command + "\n").encode() if data is None else data
    args = ["nc"]
    if not tcp:
        args.append("-u")
    args.extend(["-w", "1", "127.0.0.1", str(router.port)])
    subprocess.run(args, input=payload, stdout=subprocess.PIPE,
                   stderr=subprocess.PIPE, timeout=5, check=False)
    wait_until(
        lambda: command.splitlines()[0] in router.log()[before:],
        message=f"{router.name} did not echo command {command!r}",
    )
    return router.log()[before:]


def query_table(router):
    output = send_nc(router, "T")
    routes = {}
    for line in output.splitlines():
        parts = line.split()
        if len(parts) == 3 and parts[1].isdigit():
            routes[parts[0]] = (int(parts[1]), parts[2])
    return routes, output


def assert_route(routes, destination, cost, next_hop):
    actual = routes.get(destination)
    assert actual == (cost, next_hop), (
        f"{destination}: esperado {(cost, next_hop)}, observado {actual}; "
        f"tabela={routes}"
    )


def run_component_checks():
    router = roteador.Roteador(0)
    try:
        router.executar_comando_N("A")
        router.executar_comando_V("B", "127.0.0.1", 42001)
        router.executar_comando_V("C", "127.0.0.1", 42002)
        router.tabela["D"] = {"cost": 2, "next_hop": "B"}
        vector = router._construir_vetor_para_vizinho("B")
        assert vector["A"] == 0
        assert vector["B"] == 1
        assert vector["C"] == 1
        assert vector["D"] == roteador.INFINITO

        router.tabela["E"] = {"cost": 3, "next_hop": "B"}
        router.executar_comando_R("B")
        assert router.tabela["B"]["cost"] == roteador.INFINITO
        assert router.tabela["D"]["cost"] == roteador.INFINITO
        assert router.tabela["E"]["cost"] == roteador.INFINITO
        assert router.tabela["C"] == {"cost": 1, "next_hop": "C"}

        sent = []
        router.roteamento_ativo = True
        router.enviar_com_ack = lambda packet, addr: sent.append(
            (dict(packet), addr)
        ) or True
        router._ciclo_temporizador()
        assert len(sent) == 1
        assert sent[0][0]["type"] == "update"
        assert sent[0][0]["routes"]["D"] == roteador.INFINITO
        assert "B" not in router.tabela
        assert "D" not in router.tabela
        assert "E" not in router.tabela
    finally:
        router.fechar()


def count_line(text, expected):
    return sum(line == expected for line in text.splitlines())


def main():
    if shutil.which("nc") is None:
        print("ERRO: nc (netcat) não encontrado")
        return 2

    results = []

    def check(title, expected, function):
        try:
            observed = function()
            results.append((title, expected, str(observed), "APROVADO"))
        except Exception as exc:
            results.append((title, expected, f"{type(exc).__name__}: {exc}", "REPROVADO"))

    temp = tempfile.TemporaryDirectory(prefix="json-rip-tests-")
    routers = []
    log_files = []
    peers = []
    try:
        used = set()
        names = ("R1", "R2", "R3", "R4")
        for name in names:
            port = available_port(used)
            path = Path(temp.name) / f"{name}.log"
            log_file = path.open("w", encoding="utf-8")
            log_files.append(log_file)
            routers.append(RouterProcess(name, port, path, log_file))
        aux_port = available_port(used)
        peer_port = available_port(used)
        aux_path = Path(temp.name) / "P.log"
        aux_file = aux_path.open("w", encoding="utf-8")
        log_files.append(aux_file)
        aux = RouterProcess("P", aux_port, aux_path, aux_file)
        routers.append(aux)
        peer = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        peer.bind(("127.0.0.1", peer_port))
        peer.settimeout(3)
        peers.append(peer)

        for router in routers:
            wait_until(
                lambda r=router: r.process.poll() is not None
                or can_connect(r.port),
                timeout=5,
                message=f"{router.name} did not start",
            )
            if router.process.poll() is not None:
                raise RuntimeError(f"{router.name} exited during startup")

        r1, r2, r3, r4 = routers[:4]

        def initialization():
            for router in (r1, r2, r3, r4, aux):
                send_nc(router, f"N {router.name}")
            for router in (r1, r2, r3, r4, aux):
                routes, _ = query_table(router)
                assert_route(routes, router.name, 0, router.name)
            return "cinco rotas próprias observadas com custo zero"

        check("Inicialização e rota própria", "R1-R4 e P com custo 0", initialization)

        def configure_linear():
            send_nc(r1, f"V R2 127.0.0.1 {r2.port}")
            send_nc(r2, f"V R1 127.0.0.1 {r1.port}")
            send_nc(r2, f"V R3 127.0.0.1 {r3.port}")
            send_nc(r3, f"V R2 127.0.0.1 {r2.port}")
            for router in (r1, r2, r3):
                send_nc(router, "I 200")
            for router, neighbor in ((r1, "R2"), (r2, "R1"), (r3, "R2")):
                routes, _ = query_table(router)
                assert_route(routes, neighbor, 1, neighbor)
            return "enlaces R1-R2, R2-R3 com custo 1"

        check("V e custos diretos", "cada vizinho configurado custa 1", configure_linear)

        def linear_convergence():
            time.sleep(3)
            routes1, _ = query_table(r1)
            routes3, _ = query_table(r3)
            assert_route(routes1, "R3", 2, "R2")
            assert_route(routes3, "R1", 2, "R2")
            return "R1->R3 = 2 via R2; R3->R1 = 2 via R2"

        check("Convergência linear", "rotas de custo 2", linear_convergence)

        def forwarding():
            text = "texto com  espaços repetidos e fim  "
            send_nc(r1, f"E R3 {text}")
            wait_until(lambda: f"EE R3 {text}" in r2.log(), timeout=4,
                       message="R2 did not print EE")
            wait_until(lambda: f"DD R1 {text}" in r3.log(), timeout=4,
                       message="R3 did not print DD")
            assert count_line(r2.log(), f"EE R3 {text}") == 1
            assert count_line(r3.log(), f"DD R1 {text}") == 1
            assert f"EE R3 {text}" not in r1.log()
            assert f"DD R1 {text}" not in r1.log()
            return "origem E; um EE em R2; um DD em R3; mensagem preservada"

        check("Encaminhamento e espaços", "E/EE/DD uma vez e payload íntegro", forwarding)

        def configure_ring():
            send_nc(r1, f"V R4 127.0.0.1 {r4.port}")
            send_nc(r4, f"V R1 127.0.0.1 {r1.port}")
            send_nc(r3, f"V R4 127.0.0.1 {r4.port}")
            send_nc(r4, f"V R3 127.0.0.1 {r3.port}")
            send_nc(r4, "I 200")
            time.sleep(3)
            routes1, _ = query_table(r1)
            routes2, _ = query_table(r2)
            routes3, _ = query_table(r3)
            routes4, _ = query_table(r4)
            assert routes1["R3"][0] == 2 and routes1["R3"][1] in ("R2", "R4")
            assert routes2["R4"][0] == 2 and routes2["R4"][1] in ("R1", "R3")
            assert routes3["R1"][0] == 2 and routes3["R1"][1] in ("R2", "R4")
            assert routes4["R2"][0] == 2 and routes4["R2"][1] in ("R1", "R3")
            run_component_checks()
            return "anel converge com custo 2; Poison Reverse e R/removal verificados"

        check("Anel e Poisoned Reverse", "próximos saltos coerentes e poison 16",
              configure_ring)

        def link_failure():
            send_nc(r1, "I 10000")
            send_nc(r1, "R R2")
            routes, _ = query_table(r1)
            assert_route(routes, "R2", 16, "R2")
            if "R3" in routes:
                assert routes["R3"] in ((16, "R2"), (2, "R4")), (
                    f"route via removed neighbor was not invalidated: {routes['R3']}"
                )
            send_nc(r1, "I 200")
            deadline = time.monotonic() + 8
            latest = {}
            while time.monotonic() < deadline:
                latest, _ = query_table(r1)
                if latest.get("R3") == (2, "R4"):
                    return "R2 invalidado; rota alternativa R1-R4-R3 ativa com custo 2"
                time.sleep(0.1)
            raise AssertionError(
                f"alternate route R1-R4-R3 did not converge; latest table={latest}"
            )

        check("Falha de enlace e alternativa", "invalidação e rota alternativa",
              link_failure)

        def stop_and_wait():
            send_nc(aux, f"V Q 127.0.0.1 {peer_port}")
            before = len(aux.log())
            received = []
            peer_error = []

            def ack_after_retry():
                try:
                    first, sender = peer.recvfrom(4096)
                    second, sender2 = peer.recvfrom(4096)
                    received.extend((json.loads(first), json.loads(second)))
                    assert sender == sender2
                    ack = {"type": "ack", "src": "Q", "seq": received[1]["seq"]}
                    peer.sendto(json.dumps(ack).encode(), sender2)
                except Exception as exc:
                    peer_error.append(exc)

            worker = threading.Thread(target=ack_after_retry, daemon=True)
            worker.start()
            send_nc(aux, "E Q retry probe")
            worker.join(timeout=3)
            if peer_error:
                raise peer_error[0]
            assert len(received) == 2
            assert received[0]["seq"] == received[1]["seq"]
            assert "FALTOU UM ACK" not in aux.log()[before:]

            failure_start = len(aux.log())
            send_nc(aux, "E Q no ack probe")
            failed_packets = [json.loads(peer.recvfrom(4096)[0]) for _ in range(3)]
            assert len({packet["seq"] for packet in failed_packets}) == 1
            wait_until(lambda: "FALTOU UM ACK" in aux.log()[failure_start:],
                       message="missing FALTOU UM ACK")
            assert count_line(aux.log()[failure_start:], "FALTOU UM ACK") == 1

            duplicate_start = len(aux.log())
            data = {"type": "data", "src": "ORIG", "dst": "P",
                    "msg": "duplicate payload", "seq": 91}
            peer.sendto(json.dumps(data).encode(), ("127.0.0.1", aux.port))
            ack1 = json.loads(peer.recvfrom(4096)[0])
            peer.sendto(json.dumps(data).encode(), ("127.0.0.1", aux.port))
            ack2 = json.loads(peer.recvfrom(4096)[0])
            assert ack1["seq"] == ack2["seq"] == 91
            wait_until(lambda: "DD ORIG duplicate payload" in aux.log()[duplicate_start:])
            assert count_line(aux.log()[duplicate_start:], "DD ORIG duplicate payload") == 1
            return "ACK ok; retry seq estável; 3 tentativas; duplicata ACKada e descartada"

        check("Stop-and-Wait", "ACK/retry/duplicata/timeout", stop_and_wait)

        def tcp():
            send_nc(r4, "N R4\nV R1 127.0.0.1 " + str(r1.port) + "\nT\nT\n",
                    tcp=True, data=("N R4\nV R1 127.0.0.1 " + str(r1.port) +
                                    "\nT\nT\n").encode())
            before = len(r4.log())
            send_nc(r4, "T", tcp=True, data=b"T")
            wait_until(lambda: "T\n" in r4.log()[before:],
                       message="residual TCP command not processed at EOF")
            before = len(r4.log())
            conn = socket.create_connection(("127.0.0.1", r4.port), timeout=2)
            conn.sendall(b"T\n")
            wait_until(lambda: "T\n" in r4.log()[before:],
                       message="held TCP command not processed")
            udp_before = len(r4.log())
            send_nc(r4, "T")
            wait_until(lambda: "T\n" in r4.log()[udp_before:],
                       message="UDP command blocked by active TCP")
            conn.close()
            before = len(r4.log())
            send_nc(r4, "T", tcp=True)
            wait_until(lambda: "T\n" in r4.log()[before:],
                       message="router did not accept TCP after disconnect")
            return "linhas múltiplas, EOF residual, reconexão e UDP/TCP simultâneos"

        check("Interface TCP", "comandos multilinha, EOF e multiplexação", tcp)

        def output_format():
            for router in routers:
                lines = router.log().splitlines()
                unexpected = [
                    line for line in lines
                    if line and not (
                        re.fullmatch(r"[NVRITE] .*", line)
                        or line == "T"
                        or re.fullmatch(r"[A-Z]\w* \d+ [A-Z]\w*", line)
                        or line.startswith("EE ")
                        or line.startswith("DD ")
                        or line == "FALTOU UM ACK"
                    )
                ]
                assert not unexpected, f"{router.name} unexpected output: {unexpected}"
            return "linhas limitadas aos comandos, tabela, EE/DD e aviso definido"

        check("Formato e ausência de debug", "somente saída prevista", output_format)
    finally:
        for router in routers:
            if router.process.poll() is None:
                router.process.terminate()
                try:
                    router.process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    router.process.kill()
                    router.process.wait()
        for peer in peers:
            peer.close()
        for log_file in log_files:
            log_file.close()
        temp.cleanup()

    print("| Teste | Resultado esperado | Resultado observado | Status |")
    print("|---|---|---|---|")
    for title, expected, observed, status in results:
        print(f"| {title} | {expected} | {observed} | {status} |")
    return 0 if all(row[3] == "APROVADO" for row in results) else 1


def can_connect(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.settimeout(0.1)
        return probe.connect_ex(("127.0.0.1", port)) == 0


if __name__ == "__main__":
    raise SystemExit(main())
