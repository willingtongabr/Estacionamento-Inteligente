const mqtt = require('mqtt');
const express = require('express');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// Conexão com o Broker MQTT local
const client = mqtt.connect('mqtt://localhost');

// --- CONFIGURAÇÕES DO SIMULADOR ---
const TICK_RATE = 4000; 
const SIMULATED_MINUTES_PER_TICK = 5; 
let simulatedTime = new Date();

const SECTORS = ['A', 'B', 'C'];
const SPOTS_PER_SECTOR = 30;
const spots = [];

// --- INICIALIZAÇÃO ---
SECTORS.forEach(sectorId => {
    for (let i = 1; i <= SPOTS_PER_SECTOR; i++) {
        const spotId = `${sectorId}-${i.toString().padStart(2, '0')}`;
        spots.push({
            id: spotId,
            sectorId: sectorId,
            state: 'FREE',
            nextChangeMinutes: 0,
            failure: null // Falhas: null, 'stuck_occupied', 'stuck_free', 'flapping'
        });
    }
});

// --- LÓGICA DE PROBABILIDADE SUAVE ---
function getArrivalProbability(hour) {
    if ((hour >= 7 && hour <= 9) || (hour >= 17 && hour <= 19)) {
        return 0.15; // Horário de pico
    }
    if (hour >= 23 || hour <= 5) {
        return 0.01; // Madrugada
    }
    return 0.05; // Fluxo normal
}

// --- ENVIO DE MENSAGENS MQTT ---
function publishSpotEvent(spot) {
    const payload = {
        eventId: uuidv4(),
        // Usamos Date exato para sincronizar perfeitamente com o backend
        ts: new Date().toISOString(), 
        sectorId: spot.sectorId,
        spotId: spot.id,
        state: spot.state,
        source: "sensor"
    };
    
    const topic = `campus/parking/sectors/${spot.sectorId}/spots/${spot.id}/events`;
    client.publish(topic, JSON.stringify(payload), { qos: 1 });
}

function publishGatewayStatus() {
    SECTORS.forEach(sectorId => {
        const payload = {
            sectorId: sectorId,
            status: "ONLINE",
            ts: new Date().toISOString(),
            uptime: process.uptime().toFixed(0)
        };
        const topic = `campus/parking/sectors/${sectorId}/gateway/status`;
        client.publish(topic, JSON.stringify(payload), { qos: 1 });
    });
}

// --- LOOP PRINCIPAL DA SIMULAÇÃO ---
function runSimulation() {
    simulatedTime.setMinutes(simulatedTime.getMinutes() + SIMULATED_MINUTES_PER_TICK);
    
    const currentHour = simulatedTime.getHours();
    const probArrival = getArrivalProbability(currentHour);

    spots.forEach(spot => {
        // 1. TRATAMENTO DE FALHAS INJETADAS VIA HTTP
        if (spot.failure === 'stuck_occupied') {
            if (spot.state !== 'OCCUPIED') {
                spot.state = 'OCCUPIED';
                publishSpotEvent(spot);
            }
            return;
        }
        
        if (spot.failure === 'stuck_free') {
            if (spot.state !== 'FREE') {
                spot.state = 'FREE';
                publishSpotEvent(spot);
            }
            return;
        }

        if (spot.failure === 'flapping') {
            spot.state = (spot.state === 'FREE' ? 'OCCUPIED' : 'FREE');
            publishSpotEvent(spot);
            return;
        }

        // 2. COMPORTAMENTO REALISTA (Fluxo Normal)
        if (spot.state === 'FREE') {
            if (Math.random() < probArrival) {
                spot.state = 'OCCUPIED';
                spot.nextChangeMinutes = Math.floor(Math.random() * 300) + 60;
                publishSpotEvent(spot);
            }
        } else {
            spot.nextChangeMinutes -= SIMULATED_MINUTES_PER_TICK;
            if (spot.nextChangeMinutes <= 0) {
                spot.state = 'FREE';
                publishSpotEvent(spot);
            }
        }
    });

    console.clear();
    console.log("=========================================");
    console.log("   SIMULADOR DE ESTACIONAMENTO SMART");
    console.log("=========================================");
    console.log(`Hora Simulada: ${simulatedTime.toLocaleString()}`);
    console.log(`Prob. Chegada: ${(probArrival * 100).toFixed(1)}%`);
    
    const failingSpots = spots.filter(s => s.failure !== null);
    if (failingSpots.length > 0) {
        console.log("Vagas com Falha Injetada:");
        failingSpots.forEach(s => console.log(` - ${s.id}: ${s.failure}`));
    } else {
        console.log("Comportamento normal em todas as vagas");
    }
    console.log("=========================================");
}

// --- API HTTP PARA CONTROLE DE FALHAS ---

app.post('/api/v1/simulate/fail', (req, res) => {
    const { spotId, type } = req.body;
    const validFailures = ['stuck_occupied', 'stuck_free', 'flapping'];

    if (!validFailures.includes(type)) {
        return res.status(400).json({ error: "Tipo de falha inválido." });
    }

    const spot = spots.find(s => s.id === spotId);
    if (!spot) {
        return res.status(404).json({ error: "Vaga não encontrada." });
    }

    spot.failure = type;

    // Força a atualização imediata no MQTT ao aplicar o erro
    if (type === 'stuck_occupied' && spot.state !== 'OCCUPIED') {
        spot.state = 'OCCUPIED';
        publishSpotEvent(spot);
    } else if (type === 'stuck_free' && spot.state !== 'FREE') {
        spot.state = 'FREE';
        publishSpotEvent(spot);
    } else if (type === 'flapping') {
        spot.state = spot.state === 'FREE' ? 'OCCUPIED' : 'FREE';
        publishSpotEvent(spot);
    }

    console.log(`Falha '${type}' aplicada na vaga ${spotId}`);
    return res.json({ message: `Falha '${type}' injetada na vaga ${spotId}`, spot });
});

app.post('/api/v1/simulate/restore', (req, res) => {
    const { spotId } = req.body;

    const spot = spots.find(s => s.id === spotId);
    if (!spot) {
        return res.status(404).json({ error: "Vaga não encontrada." });
    }

    spot.failure = null;
    console.log(`Vaga ${spotId} restaurada ao funcionamento normal.`);
    return res.json({ message: `Vaga ${spotId} restaurada ao normal`, spot });
});

app.get('/api/v1/simulate/spots', (req, res) => {
    res.json(spots);
});

// --- CONEXÃO E START ---
client.on('connect', () => {
    console.log("Simulador conectado ao Broker MQTT.");
    
    // Inicia os loops
    setInterval(runSimulation, TICK_RATE);
    setInterval(publishGatewayStatus, 15000);

    app.listen(3001, () => {
        console.log("🚀 API de Controle do Simulador online na porta 3001");
    });
});

client.on('error', (err) => {
    console.error("Erro no MQTT do Simulador:", err);
});