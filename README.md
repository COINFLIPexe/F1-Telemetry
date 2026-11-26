# F1 Live Dashboard

Dashboard web per seguire una sessione di Formula 1 in tempo reale, oppure rigiocare una sessione passata.
Un solo server Node.js e una pagina HTML, senza database e senza chiavi API.

> Progetto personale e di studio. Non è affiliato alla Formula 1 né a nessuna sua società. Vedi "Dati e limiti".

## Cosa mostra

- **Torre tempi** su due colonne: posizione col colore del team, gomma e giri di utilizzo, marcia con velocità, PIT / OVT, distacco dal pilota davanti e dal leader, ultimo giro e miglior giro, mini-settori colorati.
- **Qualifica**: fase (SQ1-SQ3 / Q1-Q3), tempo rimanente, righe rosse per chi è a rischio eliminazione e grigie per chi è già fuori. **Gara**: giro corrente con barra di avanzamento, ritirati in grigio.
- **Classifiche piloti (top 10) e costruttori** con punti guadagnati in gara e frecce ▲▼ per i cambi di posizione.
- **Mappa del circuito** con le auto, **speed trap** e migliori tempi di settore.
- **Meteo**: track time, aria, pista, umidità, vento, pioggia in mm/h con livello (leggera, moderata, forte) e radar.
- **Bandiere**: gialla, safety car, virtual safety car, rossa, a scacchi.
- **Replay** di sessioni passate: scelta di gran premio e sessione, play/pausa, 1x/2x, barra di scorrimento, salto a un giro o a un orario del circuito.

Senza server la pagina parte in **modalità demo** con dati simulati.

## Avvio in locale

```bash
npm install
npm start
# apri http://localhost:3000
```

Serve Node.js 20 o superiore.

## Pubblicazione su Render (piano gratuito)

1. Carica `package.json`, `server.js`, `index.html` in un repository GitHub (nella radice, senza cartelle).
2. Su render.com: New + > Web Service > collega il repository.
3. Build Command `npm install`, Start Command `npm start`, piano Free.

Il piano gratuito va in pausa dopo un po' di inattività: la prima apertura può richiedere circa un minuto.

## Come funziona

| Modalità | Fonte | Note |
|---|---|---|
| Live | feed SignalR di `livetiming.formula1.com` (lo stesso usato da FastF1) | non ufficiale, attivo solo durante le sessioni |
| Replay | [OpenF1](https://openf1.org) (sessioni concluse, gratuite) | richieste distanziate per rispettare il limite di 3 al secondo e 30 al minuto |
| Classifica mondiale | [Jolpica-F1](https://github.com/jolpica/jolpica-f1) | punti di base per le proiezioni |
| Tracciato | MultiViewer circuits API | cade sui punti raccolti dalle auto se non disponibile |
| Coordinate, radar | Open-Meteo geocoding, Windy embed | il radar è un'incorporazione di Windy |
| Pioggia in mm/h | Open-Meteo | stima del modello meteo sul circuito, non misura la pioggia sull'asfalto |

Il server tiene lo stato in memoria. `/state` restituisce lo stato corrente (live o replay), `/track` il tracciato, `/replay/*` controlla il replay.

## Dati e limiti

- Il feed live non è documentato e può cambiare senza preavviso. Sono dati non ufficiali: uso personale e di studio, controlla i termini di ogni fonte prima di qualunque uso commerciale.
- Nel replay alcuni dati sono **stimati**: fase della qualifica e tempo rimanente (dalle pause tra i giri), ritirati (assenza di nuovi giri mentre gli altri proseguono), pit (durata della corsia box).
- Problemi noti dei dati OpenF1 che toccano questo progetto: durata dei pit spesso mancante nel 2026, mini-settori nulli su molti giri, stint che possono iniziare un giro prima, messaggi di direzione gara simultanei che si sovrascrivono. Le bandiere e i giri gomma del replay possono quindi avere piccoli errori.
- La modalità sorpasso (OVT) usa il campo DRS della telemetria: da verificare sul 2026.
- Le proiezioni punti usano la classifica attuale: nel replay di una gara passata sono dimostrative.
- Il replay arriva a 2x per non superare il limite di richieste di OpenF1.

## Crediti

FastF1, OpenF1, Jolpica-F1, MultiViewer, Open-Meteo e Windy per i dati e gli strumenti. F1 e i marchi correlati appartengono a Formula One Licensing B.V.

## Licenza

MIT, vedi `LICENSE`.
