const { timer, Subject, merge, of, from } = require('rxjs');
const { switchMap, catchError, map, filter, share } = require('rxjs/operators');
const axios = require('axios');
const mqtt = require('mqtt');
const fs = require('fs');

// Wczytywanie konfiguracji z HA (/data/options.json)
let config = {
  REKUPERATOR_IP: '192.168.0.126',
  MQTT_BROKER: 'mqtt://core-mosquitto:1883'
};

if (fs.existsSync('/data/options.json')) {
  try {
    const rawOptions = fs.readFileSync('/data/options.json');
    const userOptions = JSON.parse(rawOptions);
    config = { ...config, ...userOptions };
    console.log('Wczytano konfigurację z HA:', config);
  } catch (e) {
    console.error('Błąd odczytu /data/options.json:', e.message);
  }
}

const REKUPERATOR_IP = config.REKUPERATOR_IP;
const MQTT_BROKER = config.MQTT_BROKER;

const client = mqtt.connect(MQTT_BROKER);

// Definicja urządzenia - odrębna nazwa urządzenia dla dodatku
const deviceInfo = {
  identifiers: ['provent_mistral_addon_ec300'],
  name: 'Rekuperator Mistral Smart (Addon)',
  manufacturer: 'Pro-Vent',
  model: 'Mistral EC300',
  sw_version: '1.0.0 (Node.js RxJS Addon)'
};

const manualRefresh$ = new Subject();

// Strumień okresowego pobierania danych (co 60s) oraz na żądanie po zapisie
const pollData$ = merge(
  timer(0, 60000),
  manualRefresh$
).pipe(
  switchMap(() => 
    from(axios.post(`http://${REKUPERATOR_IP}/api/getdata.php`, 'variable[]=all', {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      timeout: 5000
    })).pipe(
      map(res => res.data?.all),
      catchError(err => {
        console.error('Błąd pobierania danych z rekuperatora:', err.message);
        return of(null);
      })
    )
  ),
  filter(data => data !== null),
  map(dataRaw => typeof dataRaw === 'string' ? JSON.parse(dataRaw) : dataRaw),
  share()
);

client.on('connect', () => {
  console.log('Połączono z brokerem MQTT');
  registerHAEntities();

  // Odrębny prefiks tematów MQTT dla dodatku (mistral_addon/...)
  client.subscribe('mistral_addon/+/set');
});

client.on('message', (topic, message) => {
  const payload = message.toString().trim();
  let command = '';

  if (topic.includes('spd_b')) command = `data=spd:b${payload}`;
  else if (topic.includes('airing')) command = `data=spd:w${payload === 'ON' ? 1 : 0}`;
  else if (topic.includes('fans')) {
    const mapFans = {
      'nawiew + wywiew': 'po',
      'tylko wywiew (otwarte okno)': 'pw',
      'tylko nawiew': 'pi'
    };
    if (mapFans[payload]) command = `data=spd:${mapFans[payload]}`;
  } else if (topic.includes('work_mode')) {
    const mapMode = {
      'wg programu dobowego': 'ta',
      'ręczny': 'tm'
    };
    if (mapMode[payload]) command = `data=spd:${mapMode[payload]}`;
  } else if (topic.includes('bypass')) {
    const mapBps = {
      'automatyczny (wg temp.)': 'ta',
      'stale załączony': 'tz',
      'stale wyłączony': 'tw'
    };
    if (mapBps[payload]) command = `data=bps:${mapBps[payload]}`;
  } else if (topic.includes('season')) {
    const mapSez = {
      'automatyczny': 'sa',
      'sezon chłodniczy (lato)': 'sl',
      'sezon grzewczy (zima)': 'sz'
    };
    if (mapSez[payload]) command = `data=sez:${mapSez[payload]}`;
  }

  if (command) {
    axios.post(`http://${REKUPERATOR_IP}/api/savedata.php`, command, {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      timeout: 5000
    }).then(res => {
      if (res.data?.Status === 'OK') {
        console.log('Zapis udany:', command);
        setTimeout(() => manualRefresh$.next(), 300);
      }
    }).catch(err => console.error('Błąd zapisu:', err.message));
  }
});

// Reakcja na odebrane dane z urządzenia
pollData$.subscribe(data => {
  // Parsowanie temperatur T1, T2, T3
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

    if (t1 !== null) client.publish('mistral_addon/sensor/t1/state', t1.toString());
    if (t2 !== null) client.publish('mistral_addon/sensor/t2/state', t2.toString());
    if (t3 !== null) client.publish('mistral_addon/sensor/t3/state', t3.toString());
  }

  // Parsowanie filtrów
  if (data.flt !== undefined) {
    client.publish('mistral_addon/sensor/filter_days/state', data.flt.toString());
  }

  // Parsowanie prędkości i trybów (spd)
  if (data.spd) {
    const spd = data.spd;
    const fanSpeed = parseInt(spd[0]) || 0;
    const workMode = spd[1] === 'a' ? 'wg programu dobowego' : 'ręczny';
    const dirMap = { o: 'nawiew + wywiew', i: 'tylko nawiew', w: 'tylko wywiew (otwarte okno)' };
    const fansDir = dirMap[spd[2]] || 'nawiew + wywiew';
    const airingOn = spd.slice(3).length >= 3;

    client.publish('mistral_addon/number/speed/state', fanSpeed.toString());
    client.publish('mistral_addon/select/work_mode/state', workMode);
    client.publish('mistral_addon/select/fans/state', fansDir);
    client.publish('mistral_addon/switch/airing/state', airingOn ? 'ON' : 'OFF');
  }

  // Parsowanie bypassu (bps)
  if (data.bps) {
    const bpsMap = { '0a': 'automatyczny (wg temp.)', '0z': 'stale załączony', '0w': 'stale wyłączony' };
    client.publish('mistral_addon/select/bypass/state', bpsMap[data.bps] || 'automatyczny (wg temp.)');
  }

  // Parsowanie sezonu (sez)
  if (data.sez) {
    const sezMap = { 'za': 'automatyczny', 'zl': 'automatyczny', 'll': 'sezon chłodniczy (lato)', 'zz': 'sezon grzewczy (zima)' };
    client.publish('mistral_addon/select/season/state', sezMap[data.sez] || 'automatyczny');
  }
});

function registerHAEntities() {
  // Czujniki
  client.publish('homeassistant/sensor/mistral_addon_t1/config', JSON.stringify({
    name: 'Addon Temperatura T1',
    state_topic: 'mistral_addon/sensor/t1/state',
    unit_of_measurement: '°C',
    device_class: 'temperature',
    state_class: 'measurement',
    unique_id: 'mistral_addon_t1_sensor',
    device: deviceInfo
  }), { retain: true });

  client.publish('homeassistant/sensor/mistral_addon_t2/config', JSON.stringify({
    name: 'Addon Temperatura T2',
    state_topic: 'mistral_addon/sensor/t2/state',
    unit_of_measurement: '°C',
    device_class: 'temperature',
    state_class: 'measurement',
    unique_id: 'mistral_addon_t2_sensor',
    device: deviceInfo
  }), { retain: true });

  client.publish('homeassistant/sensor/mistral_addon_t3/config', JSON.stringify({
    name: 'Addon Temperatura T3',
    state_topic: 'mistral_addon/sensor/t3/state',
    unit_of_measurement: '°C',
    device_class: 'temperature',
    state_class: 'measurement',
    unique_id: 'mistral_addon_t3_sensor',
    device: deviceInfo
  }), { retain: true });

  client.publish('homeassistant/sensor/mistral_addon_filter_days/config', JSON.stringify({
    name: 'Addon Dni do wymiany filtrów',
    state_topic: 'mistral_addon/sensor/filter_days/state',
    unit_of_measurement: 'd',
    icon: 'mdi:air-filter',
    unique_id: 'mistral_addon_filter_days_sensor',
    device: deviceInfo
  }), { retain: true });

  // Sterowanie
  client.publish('homeassistant/number/mistral_addon_speed/config', JSON.stringify({
    name: 'Addon Bieg wentylacji',
    state_topic: 'mistral_addon/number/speed/state',
    command_topic: 'mistral_addon/spd_b/set',
    min: 0,
    max: 4,
    step: 1,
    icon: 'mdi:speedometer',
    unique_id: 'mistral_addon_speed_number',
    device: deviceInfo
  }), { retain: true });

  client.publish('homeassistant/switch/mistral_addon_airing/config', JSON.stringify({
    name: 'Addon Wietrzenie',
    state_topic: 'mistral_addon/switch/airing/state',
    command_topic: 'mistral_addon/airing/set',
    payload_on: 'ON',
    payload_off: 'OFF',
    icon: 'mdi:fan',
    unique_id: 'mistral_addon_airing_switch',
    device: deviceInfo
  }), { retain: true });

  client.publish('homeassistant/select/mistral_addon_fans/config', JSON.stringify({
    name: 'Addon Wentylatory',
    state_topic: 'mistral_addon/select/fans/state',
    command_topic: 'mistral_addon/fans/set',
    options: ['nawiew + wywiew', 'tylko wywiew (otwarte okno)', 'tylko nawiew'],
    icon: 'mdi:fan',
    unique_id: 'mistral_addon_fans_select',
    device: deviceInfo
  }), { retain: true });

  client.publish('homeassistant/select/mistral_addon_work_mode/config', JSON.stringify({
    name: 'Addon Tryb pracy wentylacji',
    state_topic: 'mistral_addon/select/work_mode/state',
    command_topic: 'mistral_addon/work_mode/set',
    options: ['wg programu dobowego', 'ręczny'],
    icon: 'mdi:fan',
    unique_id: 'mistral_addon_work_mode_select',
    device: deviceInfo
  }), { retain: true });

  client.publish('homeassistant/select/mistral_addon_bypass/config', JSON.stringify({
    name: 'Addon Tryb pracy bypassu',
    state_topic: 'mistral_addon/select/bypass/state',
    command_topic: 'mistral_addon/bypass/set',
    options: ['automatyczny (wg temp.)', 'stale załączony', 'stale wyłączony'],
    icon: 'mdi:air-filter',
    unique_id: 'mistral_addon_bypass_select',
    device: deviceInfo
  }), { retain: true });

  client.publish('homeassistant/select/mistral_addon_season/config', JSON.stringify({
    name: 'Addon Sezon',
    state_topic: 'mistral_addon/select/season/state',
    command_topic: 'mistral_addon/season/set',
    options: ['automatyczny', 'sezon chłodniczy (lato)', 'sezon grzewczy (zima)'],
    icon: 'mdi:calendar',
    unique_id: 'mistral_addon_season_select',
    device: deviceInfo
  }), { retain: true });
}
