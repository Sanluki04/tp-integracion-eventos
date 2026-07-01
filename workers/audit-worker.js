const connect = require('../connection');
const fs = require('fs');
const path = require('path');

const EXCHANGE = 'helpdesk.events';
const QUEUE = 'helpdesk.audit';
const LOG_FILE = path.join(__dirname, '../logs/audit.log');
const PROCESSED_FILE = path.join(__dirname, '../processed-events.json');

// ============ IDEMPOTENCIA ============
function isEventProcessed(eventId) {
    try {
        if (fs.existsSync(PROCESSED_FILE)) {
            const processed = JSON.parse(fs.readFileSync(PROCESSED_FILE, 'utf8'));
            return processed.includes(eventId);
        }
    } catch (error) {
        console.error(' Error leyendo processed-events:', error);
    }
    return false;
}

function markEventAsProcessed(eventId) {
    try {
        let processed = [];
        if (fs.existsSync(PROCESSED_FILE)) {
            processed = JSON.parse(fs.readFileSync(PROCESSED_FILE, 'utf8'));
        }
        if (!processed.includes(eventId)) {
            processed.push(eventId);
            fs.writeFileSync(PROCESSED_FILE, JSON.stringify(processed, null, 2));
        }
    } catch (error) {
        console.error(' Error guardando processed-events:', error);
    }
}

// ============ WORKER ============
async function startAuditWorker() {
    try {
        const { channel } = await connect();
        console.log(' Audit Worker conectado a RabbitMQ');

        await channel.assertExchange(EXCHANGE, 'topic', { durable: true });
        await channel.assertQueue(QUEUE, { durable: true });
        await channel.bindQueue(QUEUE, EXCHANGE, 'ticket.#');

        console.log(` Audit Worker escuchando en cola: ${QUEUE}`);
        console.log(`   Vinculado a: ${EXCHANGE} con routing key: ticket.#`);
        console.log(`   Guardando auditoría en: ${LOG_FILE}\n`);

        channel.consume(QUEUE, (msg) => {
            if (!msg) return;

            try {
                const event = JSON.parse(msg.content.toString());
                const eventId = event.eventId;

                // ===== IDEMPOTENCIA =====
                if (isEventProcessed(eventId)) {
                    console.log(` [AUDIT] Evento ${eventId} ya procesado. Ignorando.`);
                    channel.ack(msg);
                    return;
                }

                console.log(`\n [AUDIT] Evento recibido: ${event.type}`);
                console.log(`   Ticket ID: ${event.payload?.ticketId || 'N/A'}`);
                console.log(`   Prioridad: ${event.payload?.priority || 'N/A'}`);

                // Guardar en archivo
                const logEntry = {
                    timestamp: new Date().toISOString(),
                    event
                };

                const logsDir = path.join(__dirname, '../logs');
                if (!fs.existsSync(logsDir)) {
                    fs.mkdirSync(logsDir, { recursive: true });
                }

                fs.appendFileSync(LOG_FILE, JSON.stringify(logEntry) + '\n');
                console.log(` [AUDIT] Evento guardado en auditoría`);

                // ===== IDEMPOTENCIA =====
                markEventAsProcessed(eventId);

                console.log(` [AUDIT] Evento confirmado (ack)\n`);
                channel.ack(msg);

            } catch (error) {
                console.error(` [AUDIT] Error:`, error);
                channel.nack(msg, false, true);
            }
        }, { noAck: false });

    } catch (error) {
        console.error(' Error en Audit Worker:', error);
        setTimeout(startAuditWorker, 5000);
    }
}

startAuditWorker();