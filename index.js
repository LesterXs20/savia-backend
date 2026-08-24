require('dotenv').config();
const mqtt = require('mqtt');
const fs = require('fs');
const { Pool } = require('pg');
const express = require('express');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

// ---------- CONEXIÓN MQTT (igual que antes) ----------
const mqttClient = mqtt.connect({
  host: process.env.MQTT_HOST,
  port: process.env.MQTT_PORT,
  protocol: 'mqtts',
  username: process.env.MQTT_USER,
  password: process.env.MQTT_PASSWORD,
  ca: fs.readFileSync('./certs/ca.crt'),
});

mqttClient.on('connect', () => {
  console.log('✅ Conectado al broker MQTT');
  mqttClient.subscribe('savia/+/+/telemetry/+', (err) => {
    if (!err) console.log('👂 Escuchando todos los tópicos de telemetría...');
  });
});

mqttClient.on('message', async (topic, message) => {
  try {
    const partes = topic.split('/');
    const site_id = partes[1];
    const device_id = partes[2];
    const sensor = partes[4];
    const data = JSON.parse(message.toString());

    await pool.query(
      `INSERT INTO telemetry (time, site_id, device_id, sensor, data)
       VALUES (NOW(), $1, $2, $3, $4)`,
      [site_id, device_id, sensor, data]
    );

    console.log(`💾 Guardado: ${device_id} / ${sensor}`);
  } catch (err) {
    console.error('Error guardando en la base de datos:', err.message);
  }
});

mqttClient.on('error', (err) => {
  console.error('❌ Error de conexión MQTT:', err.message);
});

// ---------- SERVIDOR WEB (nuevo) ----------
const app = express();

app.get('/nodes/:id/telemetry/latest', async (req, res) => {
  const device_id = req.params.id;

  try {
    // Por cada sensor, buscamos SU lectura más reciente
    const sensores = ['light', 'presence', 'electric', 'water'];
    const resultado = {};

    for (const sensor of sensores) {
      const { rows } = await pool.query(
        `SELECT data, time FROM telemetry
         WHERE device_id = $1 AND sensor = $2
         ORDER BY time DESC
         LIMIT 1`,
        [device_id, sensor]
      );

      resultado[sensor] = rows.length > 0
        ? { ...rows[0].data, last_update: rows[0].time }
        : null;
    }

    res.json(resultado);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(process.env.PORT, () => {
  console.log(`🚀 Servidor corriendo en el puerto ${process.env.PORT}`);
});
