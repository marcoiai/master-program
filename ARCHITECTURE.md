# Arquitetura do master-program

O `master-program` eh um node local-first. Hoje o transporte real ainda eh HTTP, mas a logica principal ja fica separada para permitir outros meios depois.

## Camadas

- `NodeCore`: guarda a identidade do node, registra peers, lista peers e atualiza status/latencia depois de um ping.
- `PeerRegistry`: armazenamento em memoria dos peers conhecidos.
- `Transport`: contrato minimo para enviar mensagens entre nodes. Nesta versao ele tem `send_ping`.
- `HttpTransport`: implementacao atual do `Transport`, usando o ping HTTP existente.
- Rotas Axum: adaptadores publicos. Elas continuam recebendo e respondendo o mesmo JSON de antes.

## Fluxo atual

1. O node inicia com `MASTER_PROGRAM_NODE_ID` ou gera um `node-{uuid}`.
2. `POST /v1/mesh/peers/register` recebe `{ "id": "...", "url": "http://..." }`.
3. A rota valida a URL HTTP, transforma o payload em `PeerInfo` e chama `NodeCore.register_peer`.
4. `POST /v1/mesh/peers/{id}/ping` busca o peer no `NodeCore`.
5. O `NodeCore` cria um `MessageEnvelope` de ping e delega o envio para o `Transport`.
6. O `HttpTransport` envia o ping para `/v1/ping` do peer e devolve sucesso ou erro.
7. O `NodeCore` marca o peer como `online` com latencia ou `offline: ...`.

## Por que isso ajuda o mesh

HTTP agora eh so um carregador de envelopes. O core nao precisa saber se o envelope trafegou por router/LAN, hotspot, AWDL, Bluetooth, LoRa, USB ou store-and-forward.

Os proximos transports devem implementar o mesmo contrato sem alterar `NodeCore`. Por exemplo:

- `HotspotTransport`: canal IP direto criado por um gadget ou Mac.
- `NearbyTransport`: descoberta local/AWDL/mDNS quando existir rede local.
- `BleTransport`: mensagens pequenas fatiadas em caracteristicas BLE.
- `StoreForwardTransport`: envelopes gravados e sincronizados depois.

## Limites desta etapa

- HTTP continua sendo o unico transport real.
- Nao ha descoberta automatica.
- Peers ainda ficam so em memoria.
- Nao ha roteamento multi-hop.
- O contrato publico das rotas HTTP foi preservado.
