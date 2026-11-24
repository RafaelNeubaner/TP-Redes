#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Trabalho Prático - Redes de Computadores
Protocolo de Roteamento JSON_RIP

Implementação completa contemplando:
- Parte 1: Protocolo de controle (N, V, R, I, T, E) e Vetor de Distâncias (JSON_RIP)
  com Split Horizon, Poisoned Reverse (infinito = 16), Timer periódico e encaminhamento (EE/DD).
- Parte 2: Protocolo Para-e-Espera (Stop-and-Wait) sobre UDP para mensagens entre roteadores
  (sequenciamento incremental, timeout de 0.1s, até 3 tentativas e aviso "FALTOU UM ACK").
- Parte 3: Isolamento do canal de controle via TCP na mesma porta do UDP e multiplexação
  concorrente de I/O utilizando select.select().

Bibliotecas utilizadas: exclusivamente bibliotecas padrão permitidas (sys, json, socket, select, threading).
"""

import sys
import json
import socket
import select
import threading

INFINITO = 16
TIMEOUT_ACK = 0.1
MAX_TENTATIVAS = 3
TAMANHO_BUFFER = 4096


class Roteador:
    def __init__(self, porta: int):
        self.porta = porta

        # =====================================================================
        # ESTRUTURAS DO DEV 1: Rede, Multiplexação, Controle e Stop-and-Wait
        # =====================================================================
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Em sistemas Windows (testes locais), evita que ICMP Port Unreachable
        # dispare ConnectionResetError imediato no recvfrom antes do timeout de 0.1s.
        if hasattr(socket, "SIO_UDP_CONNRESET"):
            try:
                self.udp_sock.ioctl(socket.SIO_UDP_CONNRESET, False)
            except OSError:
                pass

        self.udp_sock.bind(("", self.porta))

        self.tcp_listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp_listen_sock.bind(("", self.porta))
        self.tcp_listen_sock.listen(5)

        self.tcp_conn = None
        self.tcp_buffer = ""

        # Lock para garantir exclusão mútua no uso do socket UDP durante o Stop-and-Wait
        self.udp_lock = threading.Lock()
        self.seq_lock = threading.Lock()
        self.seq_envio = 0

        # Controle de duplicatas recebidas no Stop-and-Wait: mapeia origem -> último seq processado
        self.ultimo_seq_recebido = {}
        # Fila de pacotes recebidos enquanto se aguardava por um ACK no Stop-and-Wait
        self.fila_pendentes = []

        # =====================================================================
        # ESTRUTURAS DO DEV 2: Tabela de Roteamento, Vizinhos e Temporização
        # =====================================================================
        self. state_lock = threading.Lock()
        self.meu_nome = None
        # vizinhos: {nome_vizinho: (ip_resolvido, porta_int)}
        self.vizinhos = {}
        # vizinhos_removidos: conjunto de vizinhos removidos via comando R
        self.vizinhos_removidos = set()
        # tabela: {destino: {"cost": int, "next_hop": str}}
        self.tabela = {}
        # rotas_para_remover: destinos com custo 16 (poisoned) que devem ser
        # removidos da tabela logo após serem anunciados no próximo ciclo periódico
        self.rotas_para_remover = set()
        self.timer_roteamento = None
        self.roteamento_ativo = False
        self.geracao_temporizador = 0
        self.temporizador_em_execucao = False
        self.encerrado = False

    # =========================================================================
    # MÓDULO DEV 1: Stop-and-Wait (UDP), Recepção, Parser de Comandos e select()
    # =========================================================================

    def _proximo_seq(self) -> int:
        """Retorna o próximo número de sequência incremental (iniciando em 0)."""
        with self.seq_lock:
            atual = self.seq_envio
            self.seq_envio += 1
            return atual

    def _enviar_ack(self, seq: int, addr: tuple):
        """Envia confirmação de entrega (ACK) para o endereço remetente."""
        with self.state_lock:
            src_nome = self.meu_nome if self.meu_nome is not None else ""
        pacote_ack = {
            "type": "ack",
            "src": src_nome,
            "seq": seq,
        }
        dados = json.dumps(pacote_ack).encode("utf-8")
        try:
            self.udp_sock.sendto(dados, addr)
        except OSError:
            pass

    def enviar_com_ack(self, mensagem_dict: dict, addr: tuple) -> bool:
        """
        Envia uma mensagem JSON entre roteadores usando protocolo Para-e-Espera (Stop-and-Wait).
        Atribui o número de sequência incremental, aguarda ACK com timeout de 0.1s e realiza
        até 3 tentativas (1 envio original + 2 retransmissões).
        Caso o ACK não seja recebido após a 3ª tentativa, imprime 'FALTOU UM ACK'.
        """
        seq = self._proximo_seq()
        mensagem_dict["seq"] = seq
        dados_envio = json.dumps(mensagem_dict).encode("utf-8")

        ack_recebido = False

        with self.udp_lock:
            self.udp_sock.settimeout(TIMEOUT_ACK)
            try:
                for _tentativa in range(MAX_TENTATIVAS):
                    try:
                        self.udp_sock.sendto(dados_envio, addr)
                    except OSError:
                        pass

                    # Aguarda pelo ACK correspondente ao número de sequência enviado
                    while True:
                        try:
                            dados_rec, addr_rec = self.udp_sock.recvfrom(TAMANHO_BUFFER)
                        except socket.timeout:
                            # Timeout de 0.1s expirado nesta tentativa; passa para a próxima
                            break
                        except OSError:
                            # Caso ocorra erro de socket na tentativa, trata como falha da tentativa
                            break

                        texto = dados_rec.decode("utf-8", errors="replace").strip()
                        if not texto:
                            continue

                        if texto.startswith("{"):
                            try:
                                obj = json.loads(texto)
                            except json.JSONDecodeError:
                                continue

                            tipo = obj.get("type")
                            if tipo == "ack":
                                if obj.get("seq") == seq:
                                    ack_recebido = True
                                    break
                                # ACK antigo/duplicado de outra mensagem: ignora e continua esperando
                                continue
                            elif tipo in ("update", "data"):
                                # Uma mensagem de outro roteador chegou enquanto esperávamos o ACK.
                                # Respondemos o ACK imediatamente para evitar deadlock quando dois
                                # roteadores anunciam simultaneamente, e guardamos a mensagem na fila
                                # para processar logo após sair do Stop-and-Wait.
                                seq_remoto = obj.get("seq")
                                if seq_remoto is not None:
                                    self._enviar_ack(seq_remoto, addr_rec)
                                self.fila_pendentes.append((dados_rec, addr_rec))
                                continue
                        else:
                            # Comando de controle recebido via UDP durante a espera pelo ACK:
                            # guarda na fila para processamento imediato após o envio.
                            self.fila_pendentes.append((dados_rec, addr_rec))
                            continue

                    if ack_recebido:
                        break
            finally:
                self.udp_sock.settimeout(None)

        if not ack_recebido:
            print("FALTOU UM ACK", flush=True)

        # Processa eventuais pacotes que chegaram durante a janela de espera do ACK
        self._processar_fila_pendentes()
        return ack_recebido

    def _processar_fila_pendentes(self):
        """Despacha pacotes que foram armazenados durante a espera por um ACK."""
        while self.fila_pendentes:
            dados, addr = self.fila_pendentes.pop(0)
            self._tratar_datagrama_udp(dados, addr, ack_ja_enviado=True)

    def _tratar_datagrama_udp(self, dados: bytes, addr: tuple, ack_ja_enviado: bool = False):
        """
        Identifica se o datagrama UDP recebido é uma mensagem entre roteadores (inicia com '{')
        ou uma mensagem de configuração/controle, despachando para o módulo correspondente.
        """
        texto_bruto = dados.decode("utf-8", errors="replace")
        texto_strip = texto_bruto.lstrip()

        if texto_strip.startswith("{"):
            try:
                msg = json.loads(texto_strip.strip())
            except json.JSONDecodeError:
                return

            tipo = msg.get("type")
            if tipo == "ack":
                # ACKs fora de hora são ignorados no loop principal
                return

            src = msg.get("src")
            seq = msg.get("seq")

            # Envia confirmação (ACK) para mensagens entre roteadores (update ou data)
            if not ack_ja_enviado and seq is not None and tipo in ("update", "data"):
                with self.udp_lock:
                    self._enviar_ack(seq, addr)

            # Descarte de duplicatas de retransmissão no Stop-and-Wait (por endereço do remetente)
            if seq is not None:
                if self.ultimo_seq_recebido.get(addr) == seq:
                    return
                self.ultimo_seq_recebido[addr] = seq

            if tipo == "update":
                with self.state_lock:
                    eh_vizinho = src in self.vizinhos
                if not eh_vizinho:
                    return
                rotas = msg.get("routes")
                if isinstance(rotas, dict):
                    self.processar_vetor_distancias(src, rotas)
            elif tipo == "data":
                dst = msg.get("dst")
                conteudo = msg.get("msg")
                if isinstance(src, str) and isinstance(dst, str) and isinstance(conteudo, str):
                    self.processar_pacote_dados(src, dst, conteudo)
        else:
            # Mensagem do protocolo de controle recebida via UDP
            linhas = texto_bruto.splitlines()
            for linha in linhas:
                linha_limpa = linha.rstrip("\r\n")
                if linha_limpa:
                    self.processar_comando_controle(linha_limpa)

    def processar_comando_controle(self, comando: str):
        """
        Faz o parsing e validação dos comandos do protocolo de controle (N, V, R, I, T, E).
        Comandos válidos são ecoados literalmente na saída padrão antes de sua execução.
        Comandos não reconhecidos são impressos seguidos de '???'.
        """
        if not comando:
            return

        # Comando T: exatamente "T"
        if comando == "T":
            print(comando, flush=True)
            self.executar_comando_T()
            return

        # Comando E: "E <nome> <mensagem>" (mensagem pode conter espaços)
        if comando.startswith("E "):
            resto = comando[2:]
            idx_espaco = resto.find(" ")
            if idx_espaco > 0:
                destino = resto[:idx_espaco]
                mensagem = resto[idx_espaco + 1 :]
                if destino and mensagem != "":
                    print(comando, flush=True)
                    self.executar_comando_E(destino, mensagem)
                    return
            print(f"{comando}???", flush=True)
            return

        partes = comando.split(" ")

        # Comando N: "N <nome>"
        if len(partes) == 2 and partes[0] == "N" and partes[1]:
            print(comando, flush=True)
            self.executar_comando_N(partes[1])
            return

        # Comando V: "V <nome> <endereço> <porto>"
        if len(partes) == 4 and partes[0] == "V" and partes[1] and partes[2] and partes[3]:
            try:
                porto_viz = int(partes[3])
                if 1 <= porto_viz <= 65535:
                    print(comando, flush=True)
                    self.executar_comando_V(partes[1], partes[2], porto_viz)
                    return
            except ValueError:
                pass
            print(f"{comando}???", flush=True)
            return

        # Comando R: "R <nome>"
        if len(partes) == 2 and partes[0] == "R" and partes[1]:
            print(comando, flush=True)
            self.executar_comando_R(partes[1])
            return

        # Comando I: "I <tempo>"
        if len(partes) == 2 and partes[0] == "I" and partes[1]:
            try:
                tempo_ms = int(partes[1])
                if tempo_ms >= 0:
                    print(comando, flush=True)
                    self.executar_comando_I(tempo_ms)
                    return
            except ValueError:
                pass
            print(f"{comando}???", flush=True)
            return

        # Qualquer outro formato é inválido
        print(f"{comando}???", flush=True)

    def executar_loop_principal(self):
        """
        Orquestra o loop central de eventos monitorando simultaneamente o socket UDP
        e o socket TCP (de escuta ou de conexão ativa) através de select.select().
        """
        try:
            while not self.encerrado:
                # Há três sockets possíveis, mas monitoramos dois por vez:
                # o socket UDP e o socket TCP ativo (ou o socket TCP de accept caso não haja conexão)
                socket_tcp_atual = (
                    self.tcp_conn if self.tcp_conn is not None else self.tcp_listen_sock
                )
                leitura = [self.udp_sock, socket_tcp_atual]

                try:
                    prontos, _, _ = select.select(leitura, [], [])
                except ( InterruptedError, OSError ):
                    continue

                for s in prontos:
                    if s is self.udp_sock:
                        # Adquire o lock UDP e confirma se ainda há datagrama pendente de leitura
                        # (caso a thread do Timer não o tenha consumido como ACK durante enviar_com_ack)
                        dados = None
                        addr = None
                        with self.udp_lock:
                            ainda_pronto, _, _ = select.select([self.udp_sock], [], [], 0)
                            if ainda_pronto:
                                try:
                                    dados, addr = self.udp_sock.recvfrom(TAMANHO_BUFFER)
                                except OSError:
                                    dados = None
                        if dados is not None:
                            self._tratar_datagrama_udp(dados, addr)

                    elif s is self.tcp_listen_sock:
                        try:
                            conn, _ = self.tcp_listen_sock.accept()
                            self.tcp_conn = conn
                            self.tcp_buffer = ""
                        except OSError:
                            pass

                    elif s is self.tcp_conn:
                        try:
                            dados_tcp = self.tcp_conn.recv(TAMANHO_BUFFER)
                        except OSError:
                            dados_tcp = b""

                        if dados_tcp:
                            self.tcp_buffer += dados_tcp.decode("utf-8", errors="replace")
                            while "\n" in self.tcp_buffer:
                                linha, self.tcp_buffer = self.tcp_buffer.split("\n", 1)
                                linha = linha.rstrip("\r")
                                if linha:
                                    self.processar_comando_controle(linha)
                        else:
                            # Conexão TCP fechada pelo cliente: processa última mensagem residual (se houver)
                            # e volta a monitorar novas conexões no socket TCP de escuta.
                            if self.tcp_buffer:
                                restantes = self.tcp_buffer.splitlines()
                                self.tcp_buffer = ""
                                for linha in restantes:
                                    linha = linha.rstrip("\r\n")
                                    if linha:
                                        self.processar_comando_controle(linha)
                            try:
                                self.tcp_conn.close()
                            except OSError:
                                pass
                            self.tcp_conn = None
        except KeyboardInterrupt:
            pass
        finally:
            self.fechar()

    def fechar(self):
        """Encerra timers e fecha os sockets abertos pelo roteador."""
        self.encerrado = True
        with self.state_lock:
            if self.timer_roteamento is not None:
                self.timer_roteamento.cancel()
                self.timer_roteamento = None
        if self.tcp_conn is not None:
            try:
                self.tcp_conn.close()
            except OSError:
                pass
            self.tcp_conn = None
        try:
            self.udp_sock.close()
        except OSError:
            pass
        try:
            self.tcp_listen_sock.close()
        except OSError:
            pass

    # =========================================================================
    # MÓDULO DEV 2: Algoritmo de Roteamento, Tabela, Temporização e Dados
    # =========================================================================

    def executar_comando_N(self, nome: str):
        """
        Define o nome do roteador e inicializa a tabela de rotas com custo 0 para si mesmo.
        """
        with self.state_lock:
            nome_antigo = self.meu_nome
            if nome_antigo is not None and nome_antigo != nome:
                rota_antiga = self.tabela.get(nome_antigo)
                if (
                    rota_antiga is not None
                    and rota_antiga["cost"] == 0
                    and rota_antiga["next_hop"] == nome_antigo
                ):
                    del self.tabela[nome_antigo]
            self.meu_nome = nome
            self.tabela[nome] = {"cost": 0, "next_hop": nome}

    def executar_comando_V(self, nome: str, endereco: str, porto: int):
        """
        Registra um vizinho com custo 1 no endereço e porto informados.
        """
        try:
            ip_resolvido = socket.gethostbyname(endereco)
        except OSError:
            ip_resolvido = endereco

        with self.state_lock:
            self.vizinhos[nome] = (ip_resolvido, porto)
            self.vizinhos_removidos.discard(nome)
            self.rotas_para_remover.discard(nome)
            self.tabela[nome] = {"cost": 1, "next_hop": nome}

        # Limpa histórico de sequência do endereço do vizinho caso ele tenha sido readicionado
        self.ultimo_seq_recebido.pop((ip_resolvido, porto), None)

    def executar_comando_R(self, nome: str):
        """
        Remove o roteador informado da lista de vizinhos ativos e marca todas as rotas
        que o tinham como próximo passo com distância infinita (16) para envenenamento
        reverso no próximo anúncio periódico, removendo-as em seguida.
        """
        with self.state_lock:
            if nome in self.vizinhos:
                del self.vizinhos[nome]
            self.vizinhos_removidos.add(nome)

            for destino, info in list(self.tabela.items()):
                if destino == self.meu_nome:
                    continue
                if info["next_hop"] == nome:
                    info["cost"] = INFINITO
                    self.rotas_para_remover.add(destino)

    def executar_comando_I(self, tempo_ms: int):
        """
        Inicia a execução periódica do protocolo de roteamento após <tempo_ms> milissegundos.
        """
        atraso_inicial = tempo_ms / 1000.0
        with self.state_lock:
            if self.timer_roteamento is not None:
                self.timer_roteamento.cancel()
            self.geracao_temporizador += 1
            geracao = self.geracao_temporizador
            self.roteamento_ativo = True
            self.temporizador_em_execucao = True
            self.timer_roteamento = threading.Timer(
                atraso_inicial,
                self._ciclo_temporizador,
                args=(geracao,),
            )
            self.timer_roteamento.daemon = True
            self.timer_roteamento.start()

    def executar_comando_T(self):
        """
        Imprime a tabela de roteamento ordenada por nome do destino no formato '%s %d %s'.
        """
        with self.state_lock:
            destinos_ordenados = sorted(self.tabela.keys())
            linhas = [
                f"{dst} {self.tabela[dst]['cost']} {self.tabela[dst]['next_hop']}"
                for dst in destinos_ordenados
            ]
        for linha in linhas:
            print(linha, flush=True)

    def executar_comando_E(self, destino: str, mensagem: str):
        """
        Origem do envio de dados: encaminha a mensagem para o próximo salto rumo ao destino.
        """
        with self.state_lock:
            meu_nome = self.meu_nome
            if meu_nome is None:
                return
            if destino == meu_nome:
                print(f"DD {meu_nome} {mensagem}", flush=True)
                return
            info_rota = self.tabela.get(destino)
            if not info_rota or info_rota["cost"] >= INFINITO:
                return
            prox_salto = info_rota["next_hop"]
            addr_vizinho = self.vizinhos.get(prox_salto)

        if addr_vizinho is not None:
            pacote = {
                "type": "data",
                "src": meu_nome,
                "dst": destino,
                "msg": mensagem,
            }
            self.enviar_com_ack(pacote, addr_vizinho)

    def processar_pacote_dados(self, origem: str, destino: str, mensagem: str):
        """
        Trata pacotes do tipo 'data' recebidos de outros roteadores:
        - Se este roteador for o destino final, exibe 'DD <origem> <mensagem>'.
        - Caso contrário, exibe 'EE <destino> <mensagem>' e encaminha ao próximo salto.
        """
        with self.state_lock:
            meu_nome = self.meu_nome
            if destino == meu_nome:
                print(f"DD {origem} {mensagem}", flush=True)
                return

            print(f"EE {destino} {mensagem}", flush=True)
            info_rota = self.tabela.get(destino)
            if not info_rota or info_rota["cost"] >= INFINITO:
                return
            prox_salto = info_rota["next_hop"]
            addr_vizinho = self.vizinhos.get(prox_salto)

        if addr_vizinho is not None:
            pacote = {
                "type": "data",
                "src": origem,
                "dst": destino,
                "msg": mensagem,
            }
            self.enviar_com_ack(pacote, addr_vizinho)

    def processar_vetor_distancias(self, vizinho_origem: str, rotas_recebidas: dict):
        """
        Aplica o algoritmo de Bellman-Ford (Vetor de Distâncias) a partir do mapa de rotas
        anunciado por um vizinho conhecido, respeitando o limite infinito = 16.
        """
        with self.state_lock:
            if vizinho_origem not in self.vizinhos or self.meu_nome is None:
                return

            # O enlace diretamente configurado com um vizinho ativo é sempre custo 1.
            self.tabela[vizinho_origem] = {
                "cost": 1,
                "next_hop": vizinho_origem,
            }
            self.rotas_para_remover.discard(vizinho_origem)

            for destino, custo_anunciado in rotas_recebidas.items():
                if (
                    not isinstance(destino, str)
                    or isinstance(custo_anunciado, bool)
                    or not isinstance(custo_anunciado, int)
                    or custo_anunciado < 0
                    or custo_anunciado > INFINITO
                ):
                    continue
                if destino == self.meu_nome:
                    continue

                # Uma rota direta ativa nunca pode ser substituída por uma rota remota.
                if destino in self.vizinhos:
                    self.tabela[destino] = {
                        "cost": 1,
                        "next_hop": destino,
                    }
                    self.rotas_para_remover.discard(destino)
                    continue

                novo_custo = min(INFINITO, custo_anunciado + 1)

                if destino not in self.tabela:
                    if novo_custo < INFINITO:
                        self.tabela[destino] = {
                            "cost": novo_custo,
                            "next_hop": vizinho_origem,
                        }
                else:
                    rota_atual = self.tabela[destino]
                    custo_atual = rota_atual["cost"]
                    prox_atual = rota_atual["next_hop"]

                    if novo_custo < custo_atual:
                        self.tabela[destino] = {
                            "cost": novo_custo,
                            "next_hop": vizinho_origem,
                        }
                        self.rotas_para_remover.discard(destino)
                    elif prox_atual == vizinho_origem:
                        if novo_custo >= INFINITO:
                            if custo_atual < INFINITO:
                                rota_atual["cost"] = INFINITO
                                self.rotas_para_remover.add(destino)
                        else:
                            rota_atual["cost"] = novo_custo
                            self.rotas_para_remover.discard(destino)

    def _construir_vetor_para_vizinho(self, nome_vizinho: str) -> dict:
        """
        Constrói o dicionário de rotas a ser anunciado para `nome_vizinho`, aplicando
        Split Horizon com Poisoned Reverse: rotas cujo next_hop é o próprio `nome_vizinho`
        (exceto a rota direta para ele mesmo) são anunciadas com custo INFINITO (16).
        """
        rotas_anuncio = {}
        for destino, info in self.tabela.items():
            custo = info["cost"]
            prox = info["next_hop"]
            if destino == self.meu_nome:
                rotas_anuncio[destino] = 0
            elif prox == nome_vizinho and destino != nome_vizinho:
                rotas_anuncio[destino] = INFINITO
            else:
                rotas_anuncio[destino] = min(INFINITO, custo)
        return rotas_anuncio

    def _ciclo_temporizador(self, geracao: int = None):
        """
        Callback executado periodicamente a cada 1 segundo (após o disparo inicial de I <tempo>).
        Envia o vetor de distâncias a todos os vizinhos ativos e remove rotas envenenadas (custo 16)
        que já foram anunciadas.
        """
        if self.encerrado:
            return

        envios = []
        with self.state_lock:
            if not self.roteamento_ativo or self.meu_nome is None:
                return
            if geracao is not None and geracao != self.geracao_temporizador:
                return

            meu_nome = self.meu_nome
            for nome_viz, addr_viz in self.vizinhos.items():
                vetor = self._construir_vetor_para_vizinho(nome_viz)
                envios.append((addr_viz, vetor))

            # O próximo ciclo é agendado a partir deste disparo, independentemente
            # da duração dos envios Stop-and-Wait.
            if geracao is not None:
                if self.timer_roteamento is not None:
                    self.timer_roteamento.cancel()
                self.timer_roteamento = threading.Timer(
                    1.0,
                    self._ciclo_temporizador,
                    args=(geracao,),
                )
                self.timer_roteamento.daemon = True
                self.timer_roteamento.start()

        for addr_viz, vetor in envios:
            if self.encerrado:
                return
            pacote = {
                "type": "update",
                "src": meu_nome,
                "routes": vetor,
            }
            self.enviar_com_ack(pacote, addr_viz)

        # Remove somente depois de os vetores terem sido preparados e enviados.
        with self.state_lock:
            if geracao is not None and geracao != self.geracao_temporizador:
                return
            if self.rotas_para_remover:
                for dst in list(self.rotas_para_remover):
                    if dst in self.tabela and self.tabela[dst]["cost"] >= INFINITO:
                        del self.tabela[dst]
                self.rotas_para_remover.clear()


def main():
    if len(sys.argv) != 2:
        sys.exit(1)

    try:
        porta = int(sys.argv[1])
    except ValueError:
        sys.exit(1)

    roteador = Roteador(porta)
    roteador.executar_loop_principal()


if __name__ == "__main__":
    main()
