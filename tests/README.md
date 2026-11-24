# Testes de integração JSON_RIP

Os testes iniciam processos reais de `roteador.py` em portas locais distintas.
O runner envia comandos UDP e TCP por `nc`; usa sockets Python somente para
controlar o peer de teste nos cenários de ACK, retransmissão e duplicatas.

## Requisitos

- Python 3
- `nc` (netcat com suporte a `-u` e `-w`, como o OpenBSD netcat do Linux)
- Executar a partir da raiz do repositório

```sh
python3 tests/run_integration.py
```

O runner escolhe portas locais disponíveis, aguarda convergência e encerra os
roteadores ao terminar. Imprime resultado observado e status de cada cenário;
qualquer cenário reprovado resulta em código de saída diferente de zero. Os
logs temporários ficam em uma pasta temporária e são removidos ao final.

O ambiente de desenvolvimento usado para a primeira execução era macOS com
netcat BSD, não Linux. Para validar exatamente no laboratório, execute o mesmo
comando em Linux; os comandos `nc` usados pelo runner são compatíveis com
OpenBSD netcat.

## Cenários cobertos

1. Inicialização dos roteadores e rota própria de custo zero.
2. Cadastro de vizinhos e custos diretos iguais a 1.
3. Convergência da topologia linear `R1 <-> R2 <-> R3`.
4. Convergência da topologia em anel `R1 <-> R2 <-> R3 <-> R4 <-> R1`.
5. Split Horizon com Poisoned Reverse, usando o método real de construção do
   vetor; também verifica que `R` anuncia rotas marcadas antes de removê-las.
6. Encaminhamento com espaços repetidos e espaços no fim da mensagem:
   `E` na origem, `EE` no intermediário e `DD` no destino.
7. Remoção de enlace, invalidação e funcionamento de rota alternativa no anel.
8. Stop-and-Wait: ACK, retransmissão com o mesmo `seq`, descarte de duplicata
   e `FALTOU UM ACK` após três envios sem confirmação; mensagens `data` de
   endpoint não cadastrado e `update` com endereço divergente não recebem ACK nem
   são processadas/encaminhadas.
9. Controle TCP: comandos em múltiplas linhas, comando residual sem newline,
   reconexão, configuração por TCP e simultaneidade UDP/TCP.
10. Formato de saída, sem mensagens de depuração extras nos cenários normais.

## Reprodução manual com `nc`

Em terminais separados, inicie três processos:

```sh
python3 roteador.py 41001
python3 roteador.py 41002
python3 roteador.py 41003
```

Envie comandos UDP (substitua as portas conforme os processos iniciados):

```sh
printf 'N R1\nV R2 127.0.0.1 41002\nI 200\n' | nc -u -w 1 127.0.0.1 41001
printf 'N R2\nV R1 127.0.0.1 41001\nV R3 127.0.0.1 41003\nI 200\n' | nc -u -w 1 127.0.0.1 41002
printf 'N R3\nV R2 127.0.0.1 41002\nI 200\n' | nc -u -w 1 127.0.0.1 41003
sleep 3
printf 'T\n' | nc -u -w 1 127.0.0.1 41001
printf 'E R3 mensagem com espaços\n' | nc -u -w 1 127.0.0.1 41001
```

O esperado em R1 é `R3 2 R2`; R2 imprime `EE R3 mensagem com espaços` e R3
imprime `DD R1 mensagem com espaços`.

Controle TCP usa a mesma porta, sem `-u`. Várias linhas podem ser enviadas na
mesma conexão:

```sh
printf 'T\nT\n' | nc -w 1 127.0.0.1 41001
```

## Limites da evidência

O projeto não inclui um PDF ou outro arquivo do enunciado original. A
especificação usada nesta bateria é o README e os requisitos fornecidos na
solicitação. O runner demonstra comportamento executado localmente; uma
execução em macOS não certifica, por si só, compatibilidade no Linux do
laboratório.
