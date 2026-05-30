# Mesh Offline: desenho de transportes

## Estado

Em experimento. Esta spec registra a direcao combinada: o `master-program` nao deve depender de IP fixo, roteador, Wi-Fi existente ou LAN tradicional para ser considerado um node do mesh.

O HTTP atual entre `192.168.x.x` continua util para debug, bootstrap local e compatibilidade, mas nao e a arquitetura final do mesh.

## Objetivo

Permitir que dois ou mais nodes `master-program` se descubram, testem link e troquem mensagens mesmo quando nao existe roteador confiavel.

O caso mental principal e:

- M5 rodando Levelup e/ou `master-program`.
- M1 rodando `master-program`.
- Futuramente celular ou outro Mac entrando como node.
- Sem cloud, conta, login, internet obrigatoria ou dependencia de LAN pronta.

## Decisao principal

Separar o conceito de `peer` do conceito de `URL HTTP`.

Um peer deve ser registrado como um node com transportes possiveis:

- `nearby`: transporte local/offline, idealmente Apple MultipeerConnectivity/AWDL no macOS/iOS.
- `hotspot`: um node cria ou usa uma rede direta; a camada mesh continua igual.
- `http`: transporte legado/de desenvolvimento, bom para curl, testes e fallback quando ja existe IP.

Assim, `http://192.168.100.7:17321` vira apenas um endpoint de um transporte, nao a identidade do node.

## Modelo de peer desejado

```json
{
  "id": "m1",
  "displayName": "MacBook M1",
  "status": "registered",
  "transports": [
    {
      "kind": "nearby",
      "status": "available"
    },
    {
      "kind": "http",
      "url": "http://192.168.100.7:17321",
      "status": "debug"
    }
  ],
  "capabilities": ["core.ping", "mesh.peer"],
  "lastSeen": null,
  "latencyMs": null
}
```

## Contrato de ping

`ping` deve testar o link do mesh, nao apenas um endpoint HTTP.

Fluxo esperado:

1. UI ou CLI chama `POST /v1/mesh/peers/{id}/ping`.
2. O `master-program` escolhe o melhor transporte disponivel para aquele peer.
3. Se houver `nearby`, ele usa `nearby`.
4. Se houver apenas `http`, ele usa HTTP como compatibilidade.
5. Se nao houver transporte ativo, retorna erro explicito: `peer_transport_unavailable`.

O resultado deve atualizar:

- `status`
- `lastSeen`
- `latencyMs`
- transporte usado no ultimo ping

## Transporte HTTP

Uso permitido:

- Debug com `curl`.
- Teste rapido entre M5 e M1 enquanto o transporte offline nao existe.
- Compatibilidade com apps servidos pelo `master-program`.

Uso nao permitido como base final:

- Assumir IP fixo.
- Assumir roteador.
- Assumir que os nodes estao na mesma LAN.
- Fazer a UI depender de `localhost` ou `192.168.x.x` como identidade do node.

## Transporte nearby

No macOS/iOS, a primeira opcao realista e um helper Apple-native usando MultipeerConnectivity, que usa AWDL/Bluetooth/Wi-Fi direto por baixo quando possivel.

Desenho sugerido:

- Um helper Swift pequeno roda ao lado do `master-program`.
- O Rust conversa com o helper por stdio, Unix socket ou porta local.
- O helper publica o node e descobre nodes proximos.
- O Rust continua dono do protocolo, estado, eventos e API local.

Esse desenho mantem o core em Rust e coloca apenas a parte Apple-specific no lugar certo.

## Transporte hotspot

Hotspot nao deve virar outro protocolo.

Ele e apenas uma forma de criar link fisico quando nao ha roteador:

- Um node cria ou orienta uma rede direta.
- Os outros entram nessa rede.
- O mesh continua usando a mesma abstracao de transportes.
- Se o hotspot fornecer IP, o transporte HTTP pode funcionar por baixo, mas isso continua sendo detalhe do transporte.

## Control plane e data plane

Separar desde ja:

- Control plane: descoberta, ping, status, capabilities, eventos leves.
- Data plane: stream, arquivo grande, ROM transfer, video, assets.

O ping entra no control plane.

Streaming e transferencia grande nao devem passar pelo mesmo caminho ate medirmos latencia e throughput.

## Primeira entrega tecnica

1. Atualizar o modelo `MeshPeer` para suportar `transport`/`transports`.
2. Permitir registrar peer sem `url` quando `transport != http`.
3. Manter compatibilidade com payload antigo:

```json
{
  "id": "m1",
  "url": "http://192.168.100.7:17321"
}
```

4. Adicionar payload novo:

```json
{
  "id": "m1",
  "displayName": "MacBook M1",
  "transport": "nearby"
}
```

5. Fazer `POST /v1/mesh/peers/{id}/ping` escolher transporte.
6. Para `nearby` ainda nao implementado, retornar `peer_transport_unavailable` em vez de fingir que falhou HTTP.

## Comandos de debug atuais

Enquanto o HTTP existir como transporte de debug:

```bash
curl http://192.168.100.7:17321/v1/health
curl http://192.168.100.7:17321/v1/mesh/node
curl -X POST http://127.0.0.1:17321/v1/mesh/peers/m1/ping
```

Registrar M1 no M5 com transporte HTTP:

```bash
curl -X POST http://127.0.0.1:17321/v1/mesh/peers/register \
  -H 'Content-Type: application/json' \
  -d '{"id":"m1","url":"http://192.168.100.7:17321"}'
```

Registrar um peer nearby experimental:

```bash
curl -X POST http://127.0.0.1:17321/v1/mesh/peers/register \
  -H 'Content-Type: application/json' \
  -d '{"id":"m1","displayName":"MacBook M1","transport":"nearby"}'
```

## Principios

- A solucao mais simples que preserva o futuro geralmente e a melhor.
- O mesh deve ser local-first e offline-first.
- HTTP e uma ferramenta, nao a identidade do sistema.
- Um node ligado deve continuar sendo um node, mesmo sem app aberto na frente.
- A UI deve mostrar estado real: registrado, sem transporte, online, offline, latencia.
- Nao inventar cloud para v1.

## Perguntas futuras

- O helper nearby deve ser embutido no Tauri ou rodar como binario separado?
- O node deve anunciar capabilities por broadcast local quando o transporte permitir?
- O Levelup deve conseguir selecionar o node ativo manualmente?
- Devemos persistir peers em arquivo local no `master-program`?
- O protocolo de mensagens entre nodes sera JSON simples, SSE, ou envelopes assinados?

