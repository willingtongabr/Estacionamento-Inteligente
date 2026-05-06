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

// linha 24 até 53
// Processa mensagens MQTT: salva eventos de vagas no banco evitando duplicados e atualiza o estado em tempo real.
// Também monitora o status dos gateways e trata erros de parsing das mensagens.
client.on('message', (topic, message) => {
    try {
        const data = JSON.parse(message.toString());

        // 1. PROCESSAMENTO DE EVENTOS DE VAGA
        if (topic.includes('/events')) {
            db.run(`INSERT OR IGNORE INTO spot_events (eventId, ts, sectorId, spotId, state, rawPayloadJson) 
                    VALUES (?, ?, ?, ?, ?, ?)`, 
                    [data.eventId, data.ts, data.sectorId, data.spotId, data.state, message.toString()], 
                    function(err) {
                        if (err) return console.error("Erro ao gravar histórico:", err.message);
                        
                        if (this.changes > 0) {
                            console.log(`Novo evento registrado [${data.spotId}]: ${data.state} (ID: ${data.eventId})`);
                            processUpdate(data);
                        } else {
                            console.log(`ℹEvento duplicado ignorado: ${data.eventId}`);
                        }
                    });
        }
        
        // 2. MONITORIZAÇÃO DO GATEWAY
        if (topic.includes('/gateway/status')) {
            console.log(`Gateway [Setor ${data.sectorId}]: ${data.status} em ${data.ts}`);
        }

    } catch (e) {
        console.error("Erro ao processar mensagem MQTT:", e);
    }
});

//linha 61 até 82
// A cada 60s calcula ocupação por setor (total, ocupadas, livres e taxa) consultando a tabela de vagas.
// Em seguida salva um “snapshot” no banco para histórico e análise de uso ao longo do tempo.

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

// linha 84 até 109
// Atualiza o estado atual da vaga e detecta “flapping” (mudança muito rápida <5s), registrando incidente se ocorrer.
// Depois salva o novo estado no banco e, se ficou ocupada, verifica a capacidade do setor.
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
//linha 114 a 133
// A cada 5s verifica vagas sem mudança há >10s e marca como sensor travado (ocupada ou livre).
// Para cada caso encontrado, registra um incidente (STUCK_OCCUPIED ou STUCK_FREE).
// --- DETECÇÃO DE SENSORES TRAVADOS ---
setInterval(() => {
    // Calcula o timestamp exato de 10 segundos atrás em formato ISO
    const dezSegundosAtras = new Date(Date.now() - 10 * 1000).toISOString();
    
    // Procura por vagas travadas em OCCUPIED
    db.all(`SELECT * FROM spots WHERE currentState = 'OCCUPIED' AND lastChangeTs < ?`, [dezSegundosAtras], (err, rows) => {
        if (err) return console.error("Erro na detecção de STUCK_OCCUPIED:", err.message);
        rows?.forEach(row => {
            registerIncident(row, 'STUCK_OCCUPIED', `Vaga ocupada sem alteração por mais de 10 segundos`);
        });
    });

    // Procura por vagas travadas em FREE
    db.all(`SELECT * FROM spots WHERE currentState = 'FREE' AND lastChangeTs < ?`, [dezSegundosAtras], (err, rows) => {
        if (err) return console.error("Erro na detecção de STUCK_FREE:", err.message);
        rows?.forEach(row => {
            registerIncident(row, 'STUCK_FREE', `Vaga livre sem alteração por mais de 10 segundos`);
        });
    });
}, 5000); // Roda a verificação a cada 5 segundos

//linha 139 até 152
// Verifica a taxa de ocupação do setor e, se ≥ 90%, aciona a busca por recomendações de vagas/setores alternativos.
// Usa contagem total vs ocupadas para calcular a taxa em tempo real.
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

// linha 157 até 180
// Encontra o setor com mais vagas livres (exceto o atual) e registra uma recomendação no banco com o motivo.
// Publica a recomendação via MQTT e mantém a última mensagem com retain para novos clientes.
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
            
            console.log(`RECOMENDAÇÃO GERADA: ${reason}`);
        }
    });
}
//linha 182 até 192
// Registra um incidente apenas se ainda não existir um aberto para a mesma vaga e tipo (evita duplicados).
// Insere no banco com severidade HIGH e salva a evidência em JSON, além de emitir um alerta no console.
function registerIncident(data, type, evidence) {
    db.get(`SELECT id FROM incidents WHERE spotId = ? AND type = ? AND status = 'open'`, [data.spotId, type], (err, row) => {
        if (!row) {
            db.run(`INSERT INTO incidents (tsOpen, type, sectorId, spotId, status, severity, evidenceJson) VALUES (?, ?, ?, ?, 'open', 'HIGH', ?)`,
                [new Date().toISOString(), type, data.sectorId, data.spotId, JSON.stringify({ detail: evidence })]);
            console.warn(`ALERTA: Incidente ${type} detectado na vaga ${data.spotId}`);
        }
    });
}

// Endpoint GET que retorna todas as vagas (spots) do banco para montar o mapa no frontend.
// Em caso de erro, responde 500 com detalhes; caso contrário, retorna os dados em JSON.
// --- ENDPOINTS DE CONSULTA (Requisito 4) ---
app.get('/api/v1/map', (req, res) => {
    db.all("SELECT * FROM spots", [], (err, rows) => {
        if (err) {
            return res.status(500).json({ error: "Erro ao consultar o mapa de vagas", details: err.message });
        }
        res.json(rows);
    });
});

// Endpoint GET que calcula e retorna, por setor, total de vagas, ocupadas, livres e taxa de ocupação.
// Formata os dados em JSON com timestamp da última atualização; em erro, responde 500.
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

// Endpoint GET que retorna todas as vagas de um setor específico (normalizando o ID para maiúsculo).
// Em caso de erro, responde 500; caso contrário, devolve os dados das vagas em JSON.
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


// Endpoint GET que retorna até N vagas livres de um setor (com limite via query, padrão 10).
// Normaliza o setor para maiúsculo e responde com uma lista de spotIds; em erro, retorna 500.
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

// Endpoint GET que calcula a rotatividade (turnover) contando quantas vezes vagas ficaram ocupadas no período informado.
// Valida parâmetros obrigatórios (sectorId, from, to) e retorna o total; em erro, responde 500.
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

// Endpoint GET que retorna incidentes filtrados por status (padrão: "open"), ordenados do mais recente.
// Converte o evidenceJson de string para objeto antes de enviar; em erro, responde 500.
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

// Endpoint GET que retorna a recomendação mais recente para um setor (baseado no histórico).
// Valida o parâmetro fromSector e, se não houver dados, informa que não há recomendações; em erro, responde 500.
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
    console.log("UNIMAR Smart Parking System Online!");
    console.log("PORTA: 3000");
    console.log("-----------------------------------------");
});