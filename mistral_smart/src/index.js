const {timer, Subject, merge, of, from} = require('rxjs');
const {switchMap, catchError, map, filter, share} = require('rxjs/operators');
const axios = require('axios');
const mqtt = require('mqtt');
const fs = require('fs');
const express = require('express');
const path = require('path');

// 1. Odczyt statycznej wersji z manifestu config.json
let addonVersion = '1.0.0';
try {
    addonVersion = require('../config.json').version;
} catch (e) {
    console.warn('Błąd odczytu wersji z config.json:', e.message);
}

// 2. Domyślna konfiguracja
let config = {
    REKUPERATOR_IP: process.env.REKUPERATOR_IP || '',
    INGRESS_PORT: parseInt(process.env.INGRESS_PORT) || 8192,
    POLL_INTERVAL_SEC: parseInt(process.env.POLL_INTERVAL_SEC) || 60,
    HTTP_TIMEOUT_SEC: parseInt(process.env.HTTP_TIMEOUT_SEC) || 5,
    POST_SAVE_DELAY_MS: parseInt(process.env.POST_SAVE_DELAY_MS) || 300,
    MQTT_BROKER: process.env.MQTT_BROKER || 'mqtt://core-mosquitto:1883',
    MQTT_USER: process.env.MQTT_USER || process.env.SERVICES_MQTT_USERNAME || '',
    MQTT_PASSWORD: process.env.MQTT_PASSWORD || process.env.SERVICES_MQTT_PASSWORD || ''
};

if (fs.existsSync('/data/options.json')) {
    try {
        const rawOptions = fs.readFileSync('/data/options.json');
        const userOptions = JSON.parse(rawOptions);
        config = {...config, ...userOptions};
        console.log(`Wczytano konfigurację HA (wersja ${addonVersion}):`, config);
    } catch (e) {
        console.error('Błąd odczytu /data/options.json:', e.message);
    }
}

const REKUPERATOR_IP = config.REKUPERATOR_IP;
const PORT = config.INGRESS_PORT || 8192;
const POLL_INTERVAL_MS = (config.POLL_INTERVAL_SEC || 60) * 1000;
const HTTP_TIMEOUT_MS = (config.HTTP_TIMEOUT_SEC || 5) * 1000;
const POST_SAVE_DELAY_MS = config.POST_SAVE_DELAY_MS ?? 300;
const MQTT_BROKER = config.MQTT_BROKER;

if (!REKUPERATOR_IP) {
    console.error('BŁĄD: Adres REKUPERATOR_IP jest pusty! Uzupełnij IP w zakładce Konfiguracja dodatku.');
}

const mqttOptions = {};
if (config.MQTT_USER) mqttOptions.username = config.MQTT_USER;
if (config.MQTT_PASSWORD) mqttOptions.password = config.MQTT_PASSWORD;

const client = mqtt.connect(MQTT_BROKER, mqttOptions);

const deviceInfo = {
    identifiers: ['provent_mistral_smart_addon'],
    name: 'Rekuperator Pro-Vent Mistral',
    manufacturer: 'Pro-Vent',
    model: 'Pro-Vent Mistral',
    sw_version: `${addonVersion}`
};

const manualRefresh$ = new Subject();

// Bezwarunkowe kopiowanie pliku rekuperator.svg do /config/www/mistral_smart/
const haConfigWww = '/config/www/mistral_smart';
try {
    if (!fs.existsSync(haConfigWww)) {
        fs.mkdirSync(haConfigWww, {recursive: true});
    }
    fs.copyFileSync(
        path.join(__dirname, '../www/rekuperator.svg'),
        path.join(haConfigWww, 'rekuperator.svg')
    );
    console.log('SUKCES: Skopiowano rekuperator.svg do /config/www/mistral_smart/');
} catch (err) {
    console.error('BŁĄD KOPIOWANIA SVG:', err.message);
}

// Obiekt stanu serwowany przez API Express dla Ingress
let latestData = {
    t1: null,
    t2: null,
    t3: null,
    speed: 0,
    mode: '',
    fans: '',
    airing: false,
    airingTime: 0,
    bypass: '',
    season: '',
    filterDays: null,
    datetime: ''
};

// Inicjalizacja serwera Express (Ingress)
const app = express();

app.use(express.static(path.join(__dirname, '../www')));
app.get('/api/state', (req, res) => {
    res.json(latestData);
});

app.listen(PORT, () => {
    console.log(`Serwer Express/Ingress uruchomiony na porcie ${PORT}`);
});

const pollData$ = merge(
    timer(0, POLL_INTERVAL_MS),
    manualRefresh$
).pipe(
    switchMap(() => {
        if (!REKUPERATOR_IP) {
            console.warn('Pominięto zapytanie HTTP: brak skonfigurowanego IP rekuperatora.');
            return of(null);
        }

        return from(axios.post(`http://${REKUPERATOR_IP}/api/getdata.php`, 'variable[]=all', {
            headers: {'Content-Type': 'application/x-www-form-urlencoded'},
            timeout: HTTP_TIMEOUT_MS
        })).pipe(
            map(res => res.data?.all),
            catchError(err => {
                console.error('Błąd pobierania danych z rekuperatora:', err.message);
                return of(null);
            })
        );
    }),
    filter(data => data !== null),
    map(dataRaw => {
        try {
            return typeof dataRaw === 'string' ? JSON.parse(dataRaw) : dataRaw;
        } catch (e) {
            console.warn('Nie udało się sparsować odpowiedzi JSON z rekuperatora:', dataRaw);
            return null;
        }
    }),
    filter(data => data !== null),
    share()
);

client.on('connect', () => {
    console.log('Połączono z brokerem MQTT');
    registerHAEntities();
    client.subscribe('mistral_addon/+/set');
});

client.on('error', err => {
    console.error('Błąd połączenia z MQTT:', err.message);
});

client.on('message', (topic, message) => {
    if (!REKUPERATOR_IP) {
        console.error('Błąd zapisu: brak skonfigurowanego IP rekuperatora.');
        return;
    }

    const payload = message.toString().trim();
    let command = '';

    if (topic.includes('spd_b')) {
        const percentVal = parseInt(payload) || 0;
        const gear = Math.round(percentVal / 25);
        command = `data=spd:b${gear}`;
    } else if (topic.includes('airing')) {
        command = `data=spd:w${payload === 'ON' ? 1 : 0}`;
    } else if (topic.includes('fans')) {
        const mapFans = {
            'nawiew + wywiew': 'po',
            'tylko wywiew (otwarte okno)': 'pw',
            'tylko nawiew': 'pi'
        };
        if (mapFans[payload]) {
            command = `data=spd:${mapFans[payload]}`;
        } else {
            console.warn('Nieznana opcja wentylatorów z MQTT:', payload);
        }
    } else if (topic.includes('work_mode')) {
        const mapMode = {
            'wg programu dobowego': 'ta',
            'ręczny': 'tm'
        };
        if (mapMode[payload]) {
            command = `data=spd:${mapMode[payload]}`;
        } else {
            console.warn('Nieznany tryb pracy wentylacji z MQTT:', payload);
        }
    } else if (topic.includes('bypass')) {
        const mapBps = {
            'automatyczny (wg temp.)': 'ta',
            'stale załączony': 'tz',
            'stale wyłączony': 'tw'
        };
        if (mapBps[payload]) {
            command = `data=bps:${mapBps[payload]}`;
        } else {
            console.warn('Nieznany tryb bypassu z MQTT:', payload);
        }
    } else if (topic.includes('season')) {
        const mapSez = {
            'automatyczny': 'sa',
            'sezon chłodniczy (lato)': 'sl',
            'sezon grzewczy (zima)': 'sz'
        };
        if (mapSez[payload]) {
            command = `data=sez:${mapSez[payload]}`;
        } else {
            console.warn('Nieznana opcja sezonu z MQTT:', payload);
        }
    } else if (topic.includes('sync_time')) {
        if (payload === 'PRESS') {
            const now = new Date();
            const dayOfWeek = now.getDay();
            const hours = String(now.getHours()).padStart(2, '0');
            const minutes = String(now.getMinutes()).padStart(2, '0');
            command = `data=dat:d${dayOfWeek},dat:g${hours},dat:m${minutes}`;
            console.log('Synchronizacja czasu z rekuperatorem:', command);
        }
    }

    if (command) {
        axios.post(`http://${REKUPERATOR_IP}/api/savedata.php`, command, {
            headers: {'Content-Type': 'application/x-www-form-urlencoded'},
            timeout: HTTP_TIMEOUT_MS
        }).then(res => {
            if (res.data?.Status === 'OK') {
                console.log('Zapis udany:', command);
                setTimeout(() => manualRefresh$.next(), POST_SAVE_DELAY_MS);
            } else {
                console.warn('Nieoczekiwana odpowiedź sterownika przy zapisie:', res.data);
            }
        }).catch(err => console.error('Błąd zapisu do rekuperatora:', err.message));
    }
});

pollData$.subscribe(data => {
    // 1. Parsowanie temperatur T1, T2, T3
    if (data.tmp) {
        const lines = data.tmp.split(';');
        const parseVal = val => {
            if (!val) return null;
            let v = val.trim();
            if (v === '---') return null;
            let num = parseFloat(v);
            if (isNaN(num) || num === 50.0 || num === 63.0) return null;
            return num;
        };

        const line1 = lines[0] ? lines[0].split(',') : [];
        const line2 = lines[1] ? lines[1].split(',') : [];

        const t1 = line1.map(parseVal).find(v => v !== null) ?? null;
        const t2 = line2[0] ? parseVal(line2[0]) : null;
        const t3 = line2[1] ? parseVal(line2[1]) : null;

        if (t1 !== null) {
            client.publish('mistral_addon/sensor/t1/state', t1.toString());
            latestData.t1 = t1;
        } else {
            console.warn('Nie udało się wyznaczyć poprawnej temperatury T1 z ciągu:', lines[0]);
        }

        if (t2 !== null) {
            client.publish('mistral_addon/sensor/t2/state', t2.toString());
            latestData.t2 = t2;
        } else {
            console.warn('Nie udało się wyznaczyć poprawnej temperatury T2 z ciągu:', lines[1]);
        }

        if (t3 !== null) {
            client.publish('mistral_addon/sensor/t3/state', t3.toString());
            latestData.t3 = t3;
        } else {
            console.warn('Nie udało się wyznaczyć poprawnej temperatury T3 z ciągu:', lines[1]);
        }
    } else {
        console.warn('Brak pola "tmp" w odpowiedzi z rekuperatora');
    }

    // 2. Parsowanie filtrów
    if (data.flt !== undefined) {
        const days = parseInt(data.flt);
        if (!isNaN(days)) {
            client.publish('mistral_addon/sensor/filter_days/state', days.toString());
            latestData.filterDays = days;
        } else {
            console.warn('Nie udało się sparować wartości dni filtrów z pola flt:', data.flt);
        }
    } else {
        console.warn('Brak pola "flt" w odpowiedzi z rekuperatora');
    }

    // 3. Parsowanie prędkości, wietrzenia i czasu
    if (data.spd) {
        const spd = String(data.spd); // np. "2ao", "2ai", "2aow15"

        if (spd.length >= 3) {
            const rawGear = parseInt(spd[0]) || 0;
            const percentState = rawGear * 25;
            const workMode = spd[1] === 'a' ? 'wg programu dobowego' : 'ręczny';

            const directionMap = {
                o: 'nawiew + wywiew',
                i: 'tylko nawiew',
                w: 'tylko wywiew (otwarte okno)'
            };
            const fansDir = directionMap[spd[2]] || 'nawiew + wywiew';
            const airingOn = spd.slice(3).length >= 3;
            const airingOff = spd.slice(3).length < 3;
            const ventTime = airingOff ? (parseInt(spd.slice(3)) || 0) : (parseInt(spd.slice(5)) || 0);

            client.publish('mistral_addon/number/speed/state', percentState.toString());
            client.publish('mistral_addon/select/work_mode/state', workMode);
            client.publish('mistral_addon/select/fans/state', fansDir);
            client.publish('mistral_addon/switch/airing/state', airingOn ? 'ON' : 'OFF');
            client.publish('mistral_addon/sensor/airing_time/state', ventTime.toString());

            latestData.speed = percentState;
            latestData.mode = workMode;
            latestData.fans = fansDir;
            latestData.airing = airingOn;
            latestData.airingTime = ventTime;
        } else {
            console.warn('Za krótki ciąg spd z rekuperatora (oczekiwano min. 3 znaki):', spd);
        }
    } else {
        console.warn('Brak pola "spd" w odpowiedzi z rekuperatora');
    }

    // 4. Parsowanie bypassu
    if (data.bps) {
        const bpsMap = {'0a': 'automatyczny (wg temp.)', '0z': 'stale załączony', '0w': 'stale wyłączony'};
        const bypassState = bpsMap[data.bps];
        if (bypassState) {
            client.publish('mistral_addon/select/bypass/state', bypassState);
            latestData.bypass = bypassState;
        } else {
            console.warn('Nieoczekiwana wartość pola bps z rekuperatora:', data.bps);
        }
    } else {
        console.warn('Brak pola "bps" w odpowiedzi z rekuperatora');
    }

    // 5. Parsowanie sezonu
    if (data.sez) {
        const sezMap = {
            'za': 'automatyczny',
            'zl': 'automatyczny',
            'll': 'sezon chłodniczy (lato)',
            'zz': 'sezon grzewczy (zima)'
        };
        const seasonState = sezMap[data.sez];
        if (seasonState) {
            client.publish('mistral_addon/select/season/state', seasonState);
            latestData.season = seasonState;
        } else {
            console.warn('Nieoczekiwana wartość pola sez z rekuperatora:', data.sez);
        }
    } else {
        console.warn('Brak pola "sez" w odpowiedzi z rekuperatora');
    }

    // 6. Parsowanie daty i czasu
    if (data.dat) {
        const rawDat = String(data.dat).trim();
        let hour = null;
        let min = null;

        // Wariant 1: Sam ciąg cyfr (np. "21219" lub "20905")
        if (/^\d{5}$/.test(rawDat)) {
            hour = rawDat.substring(1, 3);
            min = rawDat.substring(3, 5);
        }
        // Wariant 2: Ciąg z literami g/m (np. "d2,g12,m19")
        else {
            const hourMatch = rawDat.match(/g(\d+)/i);
            const minMatch = rawDat.match(/m(\d+)/i);
            if (hourMatch && minMatch) {
                hour = hourMatch[1].padStart(2, '0');
                min = minMatch[1].padStart(2, '0');
            }
        }

        if (hour !== null && min !== null) {
            const now = new Date();
            const year = now.getFullYear();
            const month = String(now.getMonth() + 1).padStart(2, '0');
            const day = String(now.getDate()).padStart(2, '0');

            const dateStr = `${year}-${month}-${day} ${hour}:${min}:00`;

            client.publish('mistral_addon/sensor/datetime/state', dateStr);
            latestData.datetime = dateStr;
        } else {
            console.warn('Nie udało się sparować godziny/minuty z ciągu data.dat:', rawDat);
        }
    } else {
        console.warn('Brak pola "dat" w odpowiedzi z rekuperatora');
    }
});

function registerHAEntities() {
    const publishConfig = (topic, payload) => {
        client.publish(topic, JSON.stringify(payload), {retain: true}, (err) => {
            if (err) {
                console.warn(`Nie udało się zarejestrować encji MQTT (${topic}):`, err.message);
            }
        });
    };

    // Czujniki temperatur
    publishConfig('homeassistant/sensor/reku_temp_t1/config', {
        name: 'Temperatura czerpni (T1)',
        state_topic: 'mistral_addon/sensor/t1/state',
        unit_of_measurement: '°C',
        device_class: 'temperature',
        state_class: 'measurement',
        unique_id: 'reku_temp_t1',
        device: deviceInfo
    });

    publishConfig('homeassistant/sensor/reku_temp_t2/config', {
        name: 'Temperatura nawiewu (T2)',
        state_topic: 'mistral_addon/sensor/t2/state',
        unit_of_measurement: '°C',
        device_class: 'temperature',
        state_class: 'measurement',
        unique_id: 'reku_temp_t2',
        device: deviceInfo
    });

    publishConfig('homeassistant/sensor/reku_temp_t3/config', {
        name: 'Temperatura wywiewu (T3)',
        state_topic: 'mistral_addon/sensor/t3/state',
        unit_of_measurement: '°C',
        device_class: 'temperature',
        state_class: 'measurement',
        unique_id: 'reku_temp_t3',
        device: deviceInfo
    });

    publishConfig('homeassistant/sensor/reku_filtry_dni/config', {
        name: 'Dni do wymiany filtrów',
        state_topic: 'mistral_addon/sensor/filter_days/state',
        unit_of_measurement: 'd',
        icon: 'mdi:air-filter',
        unique_id: 'reku_filtry_dni',
        device: deviceInfo
    });

    publishConfig('homeassistant/sensor/reku_czas_wietrzenia/config', {
        name: 'Czas wietrzenia',
        state_topic: 'mistral_addon/sensor/airing_time/state',
        unit_of_measurement: 'min',
        icon: 'mdi:clock-outline',
        unique_id: 'reku_czas_wietrzenia',
        device: deviceInfo
    });

    publishConfig('homeassistant/sensor/reku_data_czas/config', {
        name: 'Data i czas sterownika',
        state_topic: 'mistral_addon/sensor/datetime/state',
        icon: 'mdi:calendar-clock',
        unique_id: 'reku_data_czas',
        device: deviceInfo
    });

    publishConfig('homeassistant/button/reku_synch_czas/config', {
        name: 'Synchronizuj czas z HA',
        command_topic: 'mistral_addon/sync_time/set',
        payload_press: 'PRESS',
        icon: 'mdi:clock-sync-outline',
        unique_id: 'reku_synch_czas',
        device: deviceInfo
    });

    publishConfig('homeassistant/number/reku_bieg_went/config', {
        name: 'Bieg wentylacji',
        state_topic: 'mistral_addon/number/speed/state',
        command_topic: 'mistral_addon/spd_b/set',
        min: 0,
        max: 100,
        step: 25,
        unit_of_measurement: '%',
        icon: 'mdi:fan',
        unique_id: 'reku_bieg_went',
        device: deviceInfo
    });

    publishConfig('homeassistant/switch/reku_wietrzenie/config', {
        name: 'Wietrzenie',
        state_topic: 'mistral_addon/switch/airing/state',
        command_topic: 'mistral_addon/airing/set',
        payload_on: 'ON',
        payload_off: 'OFF',
        icon: 'mdi:fan',
        unique_id: 'reku_wietrzenie',
        device: deviceInfo
    });

    publishConfig('homeassistant/select/reku_wentylatory/config', {
        name: 'Wentylatory',
        state_topic: 'mistral_addon/select/fans/state',
        command_topic: 'mistral_addon/fans/set',
        options: ['nawiew + wywiew', 'tylko wywiew (otwarte okno)', 'tylko nawiew'],
        icon: 'mdi:fan',
        unique_id: 'reku_wentylatory',
        device: deviceInfo
    });

    publishConfig('homeassistant/select/reku_tryb_pracy/config', {
        name: 'Tryb pracy wentylacji',
        state_topic: 'mistral_addon/select/work_mode/state',
        command_topic: 'mistral_addon/work_mode/set',
        options: ['wg programu dobowego', 'ręczny'],
        icon: 'mdi:fan',
        unique_id: 'reku_tryb_pracy',
        device: deviceInfo
    });

    publishConfig('homeassistant/select/reku_bypass/config', {
        name: 'Tryb pracy bypassu',
        state_topic: 'mistral_addon/select/bypass/state',
        command_topic: 'mistral_addon/bypass/set',
        options: ['automatyczny (wg temp.)', 'stale załączony', 'stale wyłączony'],
        icon: 'mdi:air-filter',
        unique_id: 'reku_bypass',
        device: deviceInfo
    });

    publishConfig('homeassistant/select/season/config', {
        name: 'Sezon',
        state_topic: 'mistral_addon/select/season/state',
        command_topic: 'mistral_addon/season/set',
        options: ['automatyczny', 'sezon chłodniczy (lato)', 'sezon grzewczy (zima)'],
        icon: 'mdi:calendar',
        unique_id: 'reku_sezon',
        device: deviceInfo
    });
}