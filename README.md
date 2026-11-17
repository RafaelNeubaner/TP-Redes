# Trabalho Prático — Redes de Computadores: Protocolo `JSON_RIP`

Este documento detalha toda a implementação realizada pelo **Dev 1 (Núcleo de Rede, Concorrência, Multiplexação e Stop-and-Wait)** no arquivo [`roteador.py`](./roteador.py) e especifica exatamente o escopo e os pontos de integração que pertencem ao **Dev 2 (Algoritmo de Roteamento, Tabela, Temporização, Testes e Relatório)**.

---

## 1. O que foi feito pelo Dev 1 (Concluído)

Toda a infraestrutura de rede, gerência de sockets, multiplexação com `select()`, parser do protocolo de controle e camada de transporte confiável Para-e-Espera (*Stop-and-Wait*) sobre UDP já está implementada na primeira metade da classe `Roteador` em [`roteador.py`](./roteador.py).

### 1.1. Inicialização de Sockets e Multiplexação de I/O (`select()`)
- **Porta Unificada (UDP e TCP)**:
  - O programa lê a porta via linha de comando (`python3 roteador.py <porto>`).
  - Cria o socket UDP (`self.udp_sock`) e o socket TCP passivo de escuta (`self.tcp_listen_sock`) vinculados à mesma porta (`("", porta)`) utilizando a opção `SO_REUSEADDR`.
- **Loop Central com `select.select()` (`executar_loop_principal`)**:
  - Monitora simultaneamente 2 sockets por vez na lista de leitura:
    1. `self.udp_sock` (mensagens JSON entre roteadores e/ou comandos de controle via UDP).
    2. `self.tcp_listen_sock` (quando não há cliente de controle conectado) **ou** `self.tcp_conn` (quando há uma conexão TCP de controle ativa).
- **Tratamento de Fluxo (Stream) e Fechamento TCP (EOF)**:
  - Como o TCP é orientado a fluxo de bytes, os dados recebidos em `self.tcp_conn.recv()` são acumulados em `self.tcp_buffer` e processados linha a linha a cada `\n`.
  - **Requisito crítico de avaliação atendido**: caso a conexão TCP seja fechada pelo cliente (`recv` retornando `b""`), qualquer comando residual presente em `self.tcp_buffer` (mesmo sem `\n` no final) é processado normalmente antes de fechar `self.tcp_conn` e voltar a escutar novas conexões em `self.tcp_listen_sock`.

### 1.2. Parser e Validação de Comandos de Controle (`processar_comando_controle`)
- Diferencia automaticamente pacotes de roteamento (iniciados por `{`) de comandos de configuração textual.
- Reconhece e valida rigorosamente a sintaxe dos 6 comandos especificados:
  - `N <nome>` $\rightarrow$ chama `self.executar_comando_N(nome)`
  - `V <nome> <endereço> <porto>` $\rightarrow$ valida se `<porto>` é inteiro (`1..65535`) e chama `self.executar_comando_V(nome, endereco, porto)`
  - `R <nome>` $\rightarrow$ chama `self.executar_comando_R(nome)`
  - `I <tempo>` $\rightarrow$ valida se `<tempo>` é inteiro não-negativo (em ms) e chama `self.executar_comando_I(tempo_ms)`
  - `T` $\rightarrow$ chama `self.executar_comando_T()`
  - `E <nome> <mensagem>` $\rightarrow$ extrai `<nome>` até o primeiro espaço e preserva integralmente `<mensagem>` (incluindo múltiplos espaços internos), chamando `self.executar_comando_E(destino, mensagem)`
- **Saída Padrão (`stdout`)**:
  - Todo comando válido é impresso exatamente como recebido (`print(comando, flush=True)`) antes de disparar sua ação.
  - Qualquer comando malformado ou desconhecido é impresso imediatamente seguido de três pontos de interrogação sem espaços: `<comando>???`.

### 1.3. Módulo Para-e-Espera (*Stop-and-Wait*) sobre UDP
- **Envio Confiável (`enviar_com_ack(mensagem_dict, addr)`)**:
  - Atribui automaticamente o número de sequência incremental (`"seq": 0, 1, 2, ...`) através de `_proximo_seq()` protegido por `self.seq_lock`.
  - Configura `self.udp_sock.settimeout(0.1)` (100 ms) e realiza até **3 tentativas** (1 envio inicial + até 2 retransmissões com o mesmo `seq`).
  - Se o pacote `{"type": "ack", "src": "...", "seq": <seq>}` correspondente chegar dentro das 3 tentativas, encerra a espera com sucesso (`True`).
  - Se as 3 tentativas expirarem sem confirmação, imprime exatamente `FALTOU UM ACK` na saída padrão e encerra o envio daquela mensagem (`False`).
- **Sincronização entre Thread Principal (`select`) e Thread do `Timer`**:
  - O acesso de leitura/espera no `self.udp_sock` é sincronizado com `self.udp_lock`. Assim, quando a thread do `threading.Timer` está aguardando um ACK dentro de `enviar_com_ack`, o `select()` da thread principal não consome o pacote de ACK por engano.
  - Caso uma mensagem `update` ou `data` de outro roteador chegue enquanto o roteador aguarda um ACK em `enviar_com_ack`, o roteador envia o ACK dessa mensagem imediatamente (evitando *deadlock* quando dois roteadores disparam seus timers no mesmo instante) e guarda o pacote em `self.fila_pendentes` para processá-lo logo após o término de `enviar_com_ack`.
- **Recepção de Mensagens JSON e Descarte de Duplicatas (`_tratar_datagrama_udp`)**:
  - Envia imediatamente o pacote `{"type": "ack", "src": <meu_nome>, "seq": <seq>}` para qualquer mensagem `update` ou `data` recebida.
  - Registra o último `seq` recebido por endereço (`self.ultimo_seq_recebido[addr]`). Se um pacote retransmitido (mesmo `seq`) chegar novamente, o ACK é reenviado, mas o pacote é descartado sem reprocessar a tabela ou duplicar impressões `EE`/`DD`.

---

## 2. O que cabe ao Dev 2 fazer (Escopo do Dev 2)

> **Nota sobre o estado atual do código**: Para que o arquivo [`roteador.py`](./roteador.py) já pudesse ser testado de ponta a ponta pelo Dev 1 sem erros de integração, uma implementação funcional de referência do **Módulo Dev 2** já foi deixada na segunda metade do arquivo (a partir do comentário `# === MÓDULO DEV 2 ===`). O **Dev 2** deve revisar, ajustar caso deseje personalizar alguma lógica interna, e realizar as tarefas da **Fase 5** (bateria de testes de automação com `netcat` e redação do Relatório Técnico em PDF).

Abaixo está o detalhamento completo das responsabilidades do **Dev 2** segundo o Plano de Execução:

### 2.1. Estruturas de Dados e Concorrência
Manter e manipular as estruturas de roteamento sempre protegidas por `with self.state_lock:` (`threading.Lock()`), pois elas são acessadas concorrentemente pela thread principal (comandos `N`, `V`, `R`, `T`, `E` e recepção de pacotes) e pela thread do `threading.Timer` (anúncio periódico a cada 1 segundo):
- `self.meu_nome`: string com o nome do roteador definido pelo comando `N`.
- `self.vizinhos`: dicionário `{nome_vizinho: (ip, porta)}` com os vizinhos ativos adicionados via `V`.
- `self.tabela`: dicionário `{destino: {"cost": int, "next_hop": str}}` representando a tabela de roteamento atual.
- `self.rotas_para_remover`: conjunto (`set`) de destinos marcados com custo infinito (`16`) que devem ser anunciados uma última vez (*poisoned reverse*) e removidos da tabela logo após o anúncio.

### 2.2. Métodos dos Comandos de Configuração e Dados
1. **`executar_comando_N(self, nome: str)`**:
   - Define `self.meu_nome = nome` e inicializa `self.tabela[nome] = {"cost": 0, "next_hop": nome}`.
2. **`executar_comando_V(self, nome: str, endereco: str, porto: int)`**:
   - Registra o vizinho em `self.vizinhos[nome] = (ip, porto)` e insere a rota direta de custo `1` em `self.tabela[nome] = {"cost": 1, "next_hop": nome}`.
3. **`executar_comando_R(self, nome: str)`**:
   - Remove `nome` de `self.vizinhos`.
   - Percorre `self.tabela` e, para toda rota cujo `next_hop == nome`, altera `"cost"` para `16` (`INFINITO`) e adiciona o destino em `self.rotas_para_remover` para que seja anunciado com distância 16 no próximo ciclo do timer e removido em seguida.
4. **`executar_comando_I(self, tempo_ms: int)`**:
   - Inicia um `threading.Timer(tempo_ms / 1000.0, self._ciclo_temporizador)` que dará início às trocas periódicas de vetores de distância.
5. **`executar_comando_T(self)`**:
   - Ordena as chaves de `self.tabela` em ordem alfabética (`sorted()`) e imprime uma linha por destino no formato estrito `"%s %d %s"` (`<destino> <custo> <next_hop>`).
6. **`executar_comando_E(self, destino: str, mensagem: str)`**:
   - Consulta `self.tabela[destino]` (verificando se o custo é `< 16`), descobre o `next_hop` e o `(ip, porta)` correspondente em `self.vizinhos`.
   - Monta o dicionário:
     ```json
     {"type": "data", "src": "<meu_nome>", "dst": "<destino>", "msg": "<mensagem>"}
     ```
   - Chama `self.enviar_com_ack(pacote, addr_vizinho)` (método fornecido pelo Dev 1).

### 2.3. Algoritmo de Vetor de Distâncias, Timer e Encaminhamento
1. **`processar_vetor_distancias(self, vizinho_origem: str, rotas_recebidas: dict)`**:
   - Ignora se `vizinho_origem` não estiver em `self.vizinhos`.
   - Para cada `(destino, custo_anunciado)` em `rotas_recebidas`:
     - Calcula `novo_custo = min(16, custo_anunciado + 1)`.
     - Se `destino` não existe na tabela e `novo_custo < 16`, adiciona com `next_hop = vizinho_origem`.
     - Se `novo_custo < custo_atual`, atualiza a rota para `cost = novo_custo` e `next_hop = vizinho_origem`.
     - Se `next_hop_atual == vizinho_origem`, atualiza o custo mesmo que tenha aumentado; caso chegue a `16` (rota envenenada pelo vizinho), marca `cost = 16` e agenda em `self.rotas_para_remover` (a menos que o próprio `destino` seja um vizinho direto ativo em `self.vizinhos`, caso em que assume o enlace direto de custo `1`).
2. **`_construir_vetor_para_vizinho(self, nome_vizinho: str) -> dict`**:
   - Aplica **Split Horizon com Poisoned Reverse**: se a rota para um `destino` tem `next_hop == nome_vizinho` (e `destino != nome_vizinho`), anuncia custo `16` para esse vizinho no dicionário `"routes"`.
3. **`_ciclo_temporizador(self)`**:
   - Para cada vizinho ativo em `self.vizinhos`, constrói o vetor com `_construir_vetor_para_vizinho` e envia via `self.enviar_com_ack({"type": "update", "src": self.meu_nome, "routes": vetor}, addr_viz)`.
   - Remove de `self.tabela` as rotas com custo `16` que estavam marcadas em `self.rotas_para_remover`.
   - Reagenda `threading.Timer(1.0, self._ciclo_temporizador)` para repetir a cada **1 segundo**.
4. **`processar_pacote_dados(self, origem: str, destino: str, mensagem: str)`**:
   - Se `destino == self.meu_nome`: escreve na saída padrão `DD <origem> <mensagem>`.
   - Se `destino != self.meu_nome`: escreve na saída padrão `EE <destino> <mensagem>` e encaminha o pacote (`src` mantendo a `origem` original!) para o próximo salto na tabela via `self.enviar_com_ack`.

### 2.4. Testes de Automação e Relatório Técnico (Fase 5 — Dev 2)
Conforme a divisão da **Fase 5 do Plano de Execução**, falta ao **Dev 2** elaborar para a entrega final:
1. **Scripts de Teste (`netcat` / `nc`) em Ambiente Linux**:
   - Testar topologia linear (`R1 <-> R2 <-> R3`) e topologia em anel (`R1 <-> R2 <-> R3 <-> R4 <-> R1`) tanto via UDP (`nc -u`) quanto via TCP (`nc`).
   - Validar convergência (`T`), envio de mensagens (`E` / `EE` / `DD`), falha de enlace (`R`) e falha de confirmação (`FALTOU UM ACK`).
2. **Relatório Técnico em PDF**:
   - Documentar as decisões de projeto adotadas pela dupla:
     - **Formato das 3 mensagens JSON** (`update`, `data` e `ack`) e seus campos (`type`, `src`, `dst`, `msg`, `routes`, `seq`).
     - **Sincronização e Concorrência**: uso de `threading.Lock()` (`state_lock` para a tabela de rotas e `udp_lock` para coordenar `select()` e `recvfrom()` do Stop-and-Wait).
     - **Algoritmo de Roteamento**: implementação de Bellman-Ford, *Split Horizon* com *Poisoned Reverse* (`infinito = 16`) e remoção pós-anúncio no comando `R`.
     - **Multiplexação TCP/UDP**: gerência de sockets com `select.select()` e *bufferização* de linhas TCP.
3. **Empacotamento Final (`.zip`)**:
   - Gerar o arquivo `.zip` contendo estritamente:
     - `roteador.py` (na raiz do `.zip`, sem prints extras de depuração)
     - `relatorio.pdf`
