const sqlite3 = require('sqlite3').verbose();
const db = new sqlite3.Database('./parking.db', (err) => {
    if (err) {
        console.error("❌ Erro ao conectar ao SQLite:", err.message);
    } else {
        console.log("📚 Conectado ao banco de dados SQLite (parking.db)");
    }
});

// Inicialização das Tabelas
db.serialize(() => {
    // 1. Tabela de estado atual das vagas
    db.run(`CREATE TABLE IF NOT EXISTS spots (
        spotId TEXT PRIMARY KEY,
        sectorId TEXT NOT NULL,
        currentState TEXT NOT NULL DEFAULT 'FREE',
        lastChangeTs TEXT,
        lastEventId TEXT
    )`);

    // 2. Tabela de histórico de eventos (Ingestão/Idempotência)
    db.run(`CREATE TABLE IF NOT EXISTS spot_events (
        eventId TEXT PRIMARY KEY,
        ts TEXT NOT NULL,
        sectorId TEXT NOT NULL,
        spotId TEXT NOT NULL,
        state TEXT NOT NULL,
        rawPayloadJson TEXT NOT NULL
    )`);

    // 3. Tabela de snapshots por minuto para análise de ocupação
    db.run(`CREATE TABLE IF NOT EXISTS sector_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        sectorId TEXT NOT NULL,
        occupiedCount INTEGER NOT NULL,
        freeCount INTEGER NOT NULL,
        occupancyRate REAL NOT NULL
    )`);

    // 4. Tabela de incidentes operacionais
    db.run(`CREATE TABLE IF NOT EXISTS incidents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tsOpen TEXT NOT NULL,
        tsClose TEXT,
        type TEXT NOT NULL,
        severity TEXT NOT NULL,
        sectorId TEXT NOT NULL,
        spotId TEXT NOT NULL,
        evidenceJson TEXT,
        status TEXT NOT NULL DEFAULT 'open'
    )`);

    // 5. Tabela de recomendações de setores (R-OP1)
    db.run(`CREATE TABLE IF NOT EXISTS recommendations_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        fromSector TEXT NOT NULL,
        recommendedSector TEXT NOT NULL,
        reason TEXT NOT NULL
    )`);

    // 💡 CRIAÇÃO DE ÍNDICE: Evita lentidão e duplicidade ao buscar incidentes abertos
    db.run(`CREATE INDEX IF NOT EXISTS idx_incidents_active 
            ON incidents (spotId, type) 
            WHERE status = 'open'`);

    // Popula o estado inicial das vagas caso a tabela esteja vazia
    db.get("SELECT COUNT(*) as count FROM spots", (err, row) => {
        if (row && row.count === 0) {
            console.log("⚙️ Inicializando vagas padrão no banco de dados...");
            const sectors = ['A', 'B', 'C'];
            const spotsPerSector = 30;
            const ts = new Date().toISOString();

            const stmt = db.prepare(`INSERT INTO spots (spotId, sectorId, currentState, lastChangeTs) VALUES (?, ?, 'FREE', ?)`);
            sectors.forEach(sector => {
                for (let i = 1; i <= spotsPerSector; i++) {
                    const spotId = `${sector}-${i.toString().padStart(2, '0')}`;
                    stmt.run(spotId, sector, ts);
                }
            });
            stmt.finalize();
            console.log("✅ Vagas padrão inseridas com sucesso.");
        }
    });
});

module.exports = db;