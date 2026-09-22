# Nápady na zlepšenie: vizualizácia a operácie

Brainstorm pre demo prednášky Ghost Tap. Zoradené, s odporúčaným výberom na koniec.

## Vizualizácia (web log / demo stránka)

1. **Ladder / sequence view** — dve dráhy (telefón A = READER vľavo, telefón B = CARD vpravo)
   a šípky APDU medzi nimi v čase. Presne ukazuje MITM relay, publikum hneď pochopí.
2. **Zoskupenie do transakcie** — od SELECT PPSE po GENERATE AC ako jeden collapsible blok
   so zhrnutím (AID, výsledok, počet APDU, trvanie).
3. **Latencia na očiach** — delta ms medzi APDU a round-trip relaya, zvýraznené keď prekročí
   prah. Priamo podporuje pointu o obrane (relay pridáva merateľné oneskorenie).
4. **TLV / EMV dekóder** — parsovať tagy z PPSE/GPO/READ RECORD: 4F (AID), 50 (label),
   57 (track2 equivalent), 5A (PAN), 9F36 (ATC)... **PAN maskovať defaultne** (citlivé dáta).
5. **Presenter mode** — full-screen, veľké fonty, minimálne chrome, prepínač. Na projektor.
6. **Live štatistiky** — frames/s, bytes, aktívne sessions, p50/p95 relay latencia, sparkline.
7. **Hex + ASCII stĺpce** a copy-to-clipboard na rámec.
8. **Session topológia** — kto je pripojený (rola reader/card/mula), IP, stav spojenia.
9. **Replay / scrub** — pauza a krokovanie zachytenej transakcie; export session ako JSON.
10. **Subtílny ping** (zvuk/blik) na každý relayovaný tap pre efekt na pódiu.

## Operácie (server / infra)

1. **Session pairing secret** — dnes je session len 1 bajt; ktokoľvek na sieti sa vie pripojiť.
   Voliteľný PSK/token na join chráni demo na otvorenej WiFi.
2. **Limit peerov na session** — vynútiť práve 2 (reader + card), odmietnuť tretieho.
3. **Rozšírené metriky** — histogram relay latencie, `frames_total{src}`, `sessions`,
   `bytes_total`. K tomu compose profil s **Prometheus + Grafana** a hotový dashboard.
4. **Nahrávanie rámcov do JSONL** — voliteľný záznam celej session na neskorší rozbor / dôkaz.
5. **Pytest suita + CI** — session join, obojsmerný relay, izolácia viacerých sessions,
   správanie pri odpojení, vyváženosť gauge. GitHub Actions workflow.
6. **Docker hardening** — non-root user v kontajneri, pinned base image digest, read-only FS,
   multi-arch build. Healthcheck už je.
7. **Config a štart** — vypísať efektívnu konfiguráciu pri štarte; `--json-logs` prepínač.
8. **Idle TTL na session** — auto-zatvorenie zaseknutých spojení skôr než default 300 s.
9. **TLS end-to-end** — helper skript na self-signed cert + návod na dôveru v telefóne.
10. **Reset/kill endpoint** — rýchly reštart stavu medzi pokusmi na pódiu.

## Odporúčaný ďalší krok (top 3 pre prednášku) — ✅ IMPLEMENTOVANÉ

1. **Ladder view + latencia** (vizualizácia #1 + #3) — najväčší dopad na publikum a viaže sa
   priamo na obranu.
2. **TLV dekóder s maskovaním PAN** (#4) — ukáže, aké dáta reálne tečú, bezpečne.
3. **Session PSK + limit 2 peerov** (ops #1 + #2) — aby demo na otvorenej sieti nikto nenarušil.

K tomu ako rýchle výhry: pytest suita + CI (ops #5) a Prometheus/Grafana profil (ops #3).

**Doimplementované navyše (2026-09-22):** pytest suita + GitHub Actions CI (ops #5),
plugin `mod_modify` na živú modifikáciu relayovaného APDU, **latency hero + umelé
oneskorenie `--delay-ms`** (viz #1/#3) a **capture/replay** (`--record` / `--replay`,
viz #2/#9) — deterministický fallback dema bez telefónov.
