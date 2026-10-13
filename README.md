# Mistral Smart HA Add-on (WebStorm Project)

Dodatek do Home Assistanta integrujący rekuperator Pro-Vent Mistral EC300 z wykorzystaniem Node.js, RxJS i protokołu MQTT Discovery.

## Przydzielanie nowych encji
Encje stworzone w tym dodatku mają unikalne sufiksy/przedrostki `mistral_addon_`, co zapobiega powielaniu i konfliktom z encjami utworzonymi bezpośrednio w Node-RED (`provent_http_`).

## Otwieranie w WebStormie
1. Rozpakuj archiwum ZIP.
2. W WebStormie wybierz **File -> Open...** i wskaż rozpakowany katalog `mistral-smart-ha-addon`.
3. Otwórz wbudowany terminal (`Alt + F12`) i uruchom `npm install`, aby zainstalować zależności (`rxjs`, `axios`, `mqtt`).

## Tworzenie Repozytorium GitHub z WebStorma
1. Wybierz z menu górnego **VCS -> Enable Version Control Integration... -> Git**.
2. Wybierz **VCS -> Share Project on GitHub**.
