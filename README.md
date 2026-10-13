# Mistral Smart HA Add-on

Dodatek do Home Assistanta integrujący rekuperatory z serii **Pro-Vent Mistral Smart** z wykorzystaniem Node.js, RxJS i protokołu MQTT Discovery.

## Wersja v0.1.1
- Podbicie wersji do `0.1.1`.
- Domyślny interwał odpytywania w `config.json` zmieniony na `5` sekund (z fallbackiem do `60` sekund w przypadku braku wartości w zmiennych środowiskowych).
- Zoptymalizowane nazwy encji oraz skrócenie identyfikatorów (`reku_temp_t1`, `reku_bieg_went` itd.).
- Dedykowany przycisk synchronizacji czasu z HA oraz czujnik daty i czasu sterownika.
