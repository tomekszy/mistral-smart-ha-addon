# 🌀 Pro-Vent Mistral Smart – Home Assistant Add-on

Oficjalny dodatek do systemu **Home Assistant** przeznaczony do bezprzewodowej integracji, lokalnego sterowania i monitorowania rekuperatorów z serii **Pro-Vent Mistral Smart** (m.in. EC300, EC450, EC600 i innych urządzeń z tą samą automatyką).

Dodatek komunikuje się z fizycznym sterownikiem rekuperatora za pośrednictwem lokalnego protokołu HTTP (REST API), a następnie automatycznie rejestruje i udostępnia wszystkie parametry w Home Assistancie przy użyciu protokołu **MQTT Discovery**.

---

## 🚀 Kluczowe Funkcje

* **Automatyczne wykrywanie i rejestracja (MQTT Discovery):** Wszystkie encje pojawiają się w systemie jako jedno gotowe urządzenie bez konieczności ręcznego definiowania plików YAML.
* **Precyzyjne sterowanie wydajnością ($0\% - 100\%$):** Możliwość ustawiania biegu wentylacji z poziomu suwaka w skokach co $25\%$ ($0\% = \text{Wyłączony}$, $25\% = \text{Bieg 1}$, $50\% = \text{Bieg 2}$, $75\% = \text{Bieg 3}$, $100\% = \text{Bieg 4}$).
* **Sterowanie trybami pracy:**
  * Wybór trybu wentylacji (program dobowy / tryb ręczny)
  * Sterowanie kierunkiem nawiewu/wywiewu (nawiew + wywiew, tylko nawiew, tylko wywiew / otwarte okno)
  * Zarządzanie pracą bypassu (automatyczny, stale załączony, stale wyłączony)
  * Wybór sezonu (automatyczny, grzewczy / zima, chłodniczy / lato)
  * Szybkie wietrzenie z odczytem pozostałego czasu w minutach
* **Pełen monitoring temperatur:** Odczyty temperatur na kluczowych odcinkach:
  * **$T_1$** – Temperatura czerpni (świeże powietrze z zewnątrz)
  * **$T_2$** – Temperatura nawiewu (powietrze wdmuchiwane do domu po odzysku)
  * **$T_3$** – Temperatura wywiewu / wyrzutni (powietrze usuwane z budynku)
* **Status i konserwacja:** Odczyt liczby dni pozostałych do wymiany filtrów, aktualny czas sterownika oraz synchronizacja czasu z systemem Home Assistant jednym przyciskiem.
* **Wbudowana wizualizacja (Ingress):** Zintegrowana zakładka w menu bocznym z interaktywnym schematem przepływu powietrza.

---

## 🛠️ Instalacja i Konfiguracja

1. W Home Assistancie przejdź do: **Ustawienia $\rightarrow$ Dodatki $\rightarrow$ Sklep z dodatkami**.
2. Kliknij **3 kropki (prawy górny róg) $\rightarrow$ Repozytoria** i wklej adres tego repozytorium GitHub.
3. Znajdź dodatek **Mistral Smart Rekuperacja** i kliknij **Zainstaluj**.
4. Przejdź do zakładki **Konfiguracja** dodatku i uzupełnij adres IP rekuperatora w sieci lokalnej (`REKUPERATOR_IP`).
5. Uruchom dodatek.

---

## 📊 Wizualizacja i Karty Pulpitu (Lovelace)

Dodatek udostępnia dwa sposoby na podgląd schematu rekuperacji oraz temperatur:

### Opcja 1: Dedykowany Panel Ingress (Automatyczny)
Po zainstalowaniu i uruchomieniu dodatku, w lewym menu bocznym Home Assistanta pojawi się nowa zakładka **"Mistral Smart"**. 
Zawiera ona wbudowany, interaktywny schemat przepływu powietrza SVG z aktualizowanymi na żywo temperaturami i parametrami pracy.

---

### Opcja 2: Ręczna Karta na Pulpicie (Szablon `dashboard_card.yaml`)

Jeśli wolisz mieć schemat rekuperatora bezpośrednio na swoim głównym pulpicie (Lovelace), skorzystaj z przygotowanego szablonu karty `picture-elements`.

#### Instrukcja krok po kroku:
1. Upewnij się, że dodatek jest uruchomiony (plik `rekuperator.svg` jest serwowany automatycznie).
2. Otwórz plik [**`dashboard_card.yaml`**](https://raw.githubusercontent.com/tomekszy/mistral-smart-ha-addon/refs/heads/master/mistral_smart/dashboard_card.yaml) znajdujący się w repozytorium i skopiuj całą jego zawartość.
3. W Home Assistancie przejdź do swojego pulpitu nawigacyjnego, kliknij **3 kropki (prawy górny róg) $\rightarrow$ Edytuj pulpit**.
4. Kliknij **Dodaj kartę**, przewiń na sam dół i wybierz **Ręcznie (Manual)**.
5. Wklej skopiowany kod YAML i zapisz kartę.

[**Plik dashboard_card.yaml**](https://raw.githubusercontent.com/tomekszy/mistral-smart-ha-addon/refs/heads/master/mistral_smart/dashboard_card.yaml)
