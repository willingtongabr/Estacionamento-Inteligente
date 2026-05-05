const mqtt = require('mqtt');
const express = require('express');
const cors = require('cors');
const db = require('./database');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// Conecta ao Broker MQTT local
const client = mqtt.connect('mqtt://localhost');

// --- CONFIGURAÇÃO MQTT ---
client.on('connect', () => {
    client.subscribe('campus/parking/sectors/+/spots/+/events');
    client.subscribe('campus/parking/sectors/+/gateway/status');
    console.log("✅ Backend conectado ao Broker MQTT");
});

client.on('message', (topic, message) => {
    try {
        const data = JSON.parse(message.toString());

        // 1. PROCESSAMENTO DE EVENTOS DE VAGA
        if (topic.includes('/events')) {
            db.run(`INSERT OR IGNORE INTO spot_events (eventId, ts, sectorId, spotId, state, rawPayloadJson) 
                    VALUES (?, ?, ?, ?, ?, ?)`, 
                    [data.eventId, data.ts, data.sectorId, data.spotId, data.state, message.toString()], 
                    function(err) {
                        if (err) return console.error("❌ Erro ao gravar histórico:", err.message);
                        
                        if (this.changes > 0) {
                            console.log(`📥 Novo evento registrado [${data.spotId}]: ${data.state} (ID: ${data.eventId})`);
                            processUpdate(data);
                        } else {
                            console.log(`ℹ️ Evento duplicado ignorado: ${data.eventId}`);
                        }
                    });
        }
        
        // 2. MONITORIZAÇÃO DO GATEWAY
        if (topic.includes('/gateway/status')) {
            console.log(`📡 Gateway [Setor ${data.sectorId}]: ${data.status} em ${data.ts}`);
        }

    } catch (e) {
        console.error("❌ Erro ao processar mensagem MQTT:", e);
    }
});

// --- CRIAÇÃO DE SNAPSHOTS POR MINUTO ---
setInterval(() => {
    const ts = new Date().toISOString();
    const sectors = ['A', 'B', 'C'];

    sectors.forEach(sectorId => {
        db.get(`SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN currentState = 'OCCUPIED' THEN 1 ELSE 0 END) as occupied
            FROM spots WHERE sectorId = ?`, [sectorId], (err, row) => {
                
            if (row && row.total > 0) {
                const occupiedCount = row.occupied || 0;
                const freeCount = row.total - occupiedCount;
                const occupancyRate = (occupiedCount / row.total).toFixed(2);

                db.run(`INSERT INTO sector_snapshots (ts, sectorId, occupiedCount, freeCount, occupancyRate)
                        VALUES (?, ?, ?, ?, ?)`, 
                        [ts, sectorId, occupiedCount, freeCount, parseFloat(occupancyRate)]);
            }
        });
    });
}, 60000);

// --- LÓGICA DE PROCESSAMENTO ---
async function processUpdate(data) {
    db.get(`SELECT currentState, lastChangeTs FROM spots WHERE spotId = ?`, [data.spotId], (err, prev) => {
        const now = new Date(data.ts);
        
        if (prev && prev.lastChangeTs) {
            const lastChange = new Date(prev.lastChangeTs);
            const diffSeconds = (now - lastChange) / 1000;

            // Regra 6: Detecção de Flapping (troca rápida < 5s)
            if (diffSeconds < 5 && prev.currentState !== data.state) {
                registerIncident(data, 'FLAPPING', `Troca de estado em ${diffSeconds.toFixed(1)}s`);
            }
        }

        db.run(`UPDATE spots SET currentState = ?, lastChangeTs = ?, lastEventId = ? WHERE spotId = ?`,
                [data.state, data.ts, data.eventId, data.spotId]);

        if (data.state === 'OCCUPIED') {
            checkSectorCapacity(data.sectorId);
        }
    });
}

// --- DETECÇÃO DE SENSORES TRAVADOS ---
// --- DETECÇÃO DE SENSORES TRAVADOS ---
setInterval(() => {
    // Calcula o timestamp exato de 10 segundos atrás em formato ISO
    const dezSegundosAtras = new Date(Date.now() - 10 * 1000).toISOString();
    
    // Procura por vagas travadas em OCCUPIED
    db.all(`SELECT * FROM spots WHERE currentState = 'OCCUPIED' AND lastChangeTs < ?`, [dezSegundosAtras], (err, rows) => {
        if (err) return console.error("❌ Erro na detecção de STUCK_OCCUPIED:", err.message);
        rows?.forEach(row => {
            registerIncident(row, 'STUCK_OCCUPIED', `Vaga ocupada sem alteração por mais de 10 segundos`);
        });
    });

    // Procura por vagas travadas em FREE
    db.all(`SELECT * FROM spots WHERE currentState = 'FREE' AND lastChangeTs < ?`, [dezSegundosAtras], (err, rows) => {
        if (err) return console.error("❌ Erro na detecção de STUCK_FREE:", err.message);
        rows?.forEach(row => {
            registerIncident(row, 'STUCK_FREE', `Vaga livre sem alteração por mais de 10 segundos`);
        });
    });
}, 5000); // Roda a verificação a cada 5 segundos

// --- REGRA R-OP1: RECOMENDAÇÃO ---
function checkSectorCapacity(sectorId) {
    db.get(`SELECT 
        COUNT(*) as total, 
        SUM(CASE WHEN currentState = 'OCCUPIED' THEN 1 ELSE 0 END) as occupied 
        FROM spots WHERE sectorId = ?`, [sectorId], (err, row) => {
        
        if (row && row.total > 0) {
            const rate = row.occupied / row.total;
            if (rate >= 0.90) {
                findRecommendation(sectorId, row.occupied, row.total);
            }
        }
    });
}

function findRecommendation(fromSector, occupied, total) {
    db.get(`SELECT sectorId, (30 - COUNT(CASE WHEN currentState = 'OCCUPIED' THEN 1 END)) as freeSpots
            FROM spots WHERE sectorId != ? GROUP BY sectorId ORDER BY freeSpots DESC LIMIT 1`, 
            [fromSector], (err, best) => {
        
        if (best) {
            const occupancyPct = (occupied / total * 100).toFixed(0);
            const reason = `Setor ${fromSector} atingiu ${occupancyPct}% de ocupação; Setor ${best.sectorId} disponível com ${best.freeSpots} vagas`;
            const ts = new Date().toISOString();

            db.run(`INSERT INTO recommendations_log (ts, fromSector, recommendedSector, reason) VALUES (?, ?, ?, ?)`,
                [ts, fromSector, best.sectorId, reason]);
            
            client.publish('campus/parking/recommendations', JSON.stringify({ 
                fromSector, 
                recommendedSector: best.sectorId, 
                reason, 
                ts 
            }), { retain: true });
            
            console.log(`💡 RECOMENDAÇÃO GERADA: ${reason}`);
        }
    });
}

function registerIncident(data, type, evidence) {
    db.get(`SELECT id FROM incidents WHERE spotId = ? AND type = ? AND status = 'open'`, [data.spotId, type], (err, row) => {
        if (!row) {
            db.run(`INSERT INTO incidents (tsOpen, type, sectorId, spotId, status, severity, evidenceJson) VALUES (?, ?, ?, ?, 'open', 'HIGH', ?)`,
                [new Date().toISOString(), type, data.sectorId, data.spotId, JSON.stringify({ detail: evidence })]);
            console.warn(`⚠️ ALERTA: Incidente ${type} detectado na vaga ${data.spotId}`);
        }
    });
}

// --- ENDPOINTS DE CONSULTA (Requisito 4) ---

app.get('/api/v1/map', (req, res) => {
    db.all("SELECT * FROM spots", [], (err, rows) => {
        if (err) {
            return res.status(500).json({ error: "Erro ao consultar o mapa de vagas", details: err.message });
        }
        res.json(rows);
    });
});

app.get('/api/v1/sectors', (req, res) => {
    db.all(`SELECT sectorId, COUNT(*) as total, 
            SUM(CASE WHEN currentState = 'OCCUPIED' THEN 1 ELSE 0 END) as occupiedCount,
            SUM(CASE WHEN currentState = 'FREE' THEN 1 ELSE 0 END) as freeCount
            FROM spots GROUP BY sectorId`, (err, rows) => {
        if (err) {
            return res.status(500).json({ error: "Erro ao calcular disponibilidade dos setores", details: err.message });
        }
        
        const response = rows.map(r => ({
            sectorId: r.sectorId,
            total: r.total,
            occupiedCount: r.occupiedCount || 0,
            freeCount: r.freeCount || 0,
            occupancyRate: (r.occupiedCount / r.total).toFixed(2),
            lastUpdateTs: new Date().toISOString()
        }));

        res.json(response);
    });
});

app.get('/api/v1/sectors/:sectorId/spots', (req, res) => {
    const { sectorId } = req.params;
    const upperSector = sectorId.toUpperCase();

    db.all(`SELECT * FROM spots WHERE sectorId = ?`, [upperSector], (err, rows) => {
        if (err) {
            return res.status(500).json({ error: "Erro ao consultar vagas do setor", details: err.message });
        }
        res.json(rows);
    });
});

app.get('/api/v1/sectors/:sectorId/free-spots', (req, res) => {
    const { sectorId } = req.params;
    const limit = parseInt(req.query.limit) || 10;
    const upperSector = sectorId.toUpperCase();

    db.all(`SELECT spotId FROM spots WHERE sectorId = ? AND currentState = 'FREE' LIMIT ?`, 
        [upperSector, limit], (err, rows) => {
            if (err) {
                return res.status(500).json({ error: "Erro ao buscar vagas livres", details: err.message });
            }
            res.json(rows.map(r => r.spotId));
        });
});

// --- ENDPOINTS DE RELATÓRIOS E INCIDENTES ---

app.get('/api/v1/reports/turnover', (req, res) => {
    const { sectorId, from, to } = req.query;

    if (!sectorId || !from || !to) {
        return res.status(400).json({ 
            error: "Parâmetros obrigatórios ausentes. Forneça: sectorId, from e to (no formato ISO)." 
        });
    }

    const upperSector = sectorId.toUpperCase();

    db.get(`SELECT COUNT(*) as turnover FROM spot_events 
            WHERE sectorId = ? AND state = 'OCCUPIED' AND ts BETWEEN ? AND ?`, 
            [upperSector, from, to], (err, row) => {
        if (err) {
            return res.status(500).json({ error: "Erro ao consultar rotatividade", details: err.message });
        }
        
        res.json({
            sectorId: upperSector,
            from,
            to,
            turnover: row ? row.turnover : 0
        });
    });
});

app.get('/api/v1/incidents', (req, res) => {
    const status = req.query.status || 'open';

    db.all(`SELECT * FROM incidents WHERE status = ? ORDER BY tsOpen DESC`, [status], (err, rows) => {
        if (err) {
            return res.status(500).json({ error: "Erro ao buscar incidentes", details: err.message });
        }

        const response = rows.map(r => ({
            ...r,
            evidenceJson: r.evidenceJson ? JSON.parse(r.evidenceJson) : null
        }));

        res.json(response);
    });
});

// Consulta de Recomendação de Setor (Regra R-OP1)
app.get('/api/v1/recommendation', (req, res) => {
    const { fromSector } = req.query;

    if (!fromSector) {
        return res.status(400).json({ error: "O parâmetro fromSector é obrigatório." });
    }

    const upperSector = fromSector.toUpperCase();

    db.get(`SELECT fromSector, recommendedSector, reason, ts 
            FROM recommendations_log 
            WHERE fromSector = ? 
            ORDER BY ts DESC LIMIT 1`, [upperSector], (err, row) => {
        if (err) {
            return res.status(500).json({ error: "Erro ao consultar recomendações", details: err.message });
        }
        
        if (!row) {
            return res.json({ message: `Sem recomendações no momento para o Setor ${upperSector}` });
        }

        res.json(row);
    });
});

app.listen(3000, () => {
    console.log("-----------------------------------------");
    console.log("🚀 UNIMAR Smart Parking System Online!");
    console.log("PORTA: 3000");
    console.log("-----------------------------------------");
});