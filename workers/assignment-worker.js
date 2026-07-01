const connect = require('../connection');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs');
const path = require('path');

const EXCHANGE = 'helpdesk.events';
const QUEUE = 'helpdesk.assignment';
const METRICS_FILE = path.join(__dirname, '../metrics.json');
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

// ============ MÉTRICAS ============
function updateMetrics() {
    try {
        let metrics = {};
        if (fs.existsSync(METRICS_FILE)) {
            metrics = JSON.parse(fs.readFileSync(METRICS_FILE, 'utf8'));
        }
        const today = new Date().toISOString().split('T')[0];
        metrics[today] = (metrics[today] || 0) + 1;
        fs.writeFileSync(METRICS_FILE, JSON.stringify(metrics, null, 2));
        console.log(` [METRICS] Tickets hoy: ${metrics[today]}`);
    } catch (error) {
        console.error(' Error guardando métricas:', error);
    }
}

// ============ WORKER PRINCIPAL ============
async function startAssignmentWorker() {
    try {
        const { channel } = await connect();
        console.log(' Assignment Worker conectado a RabbitMQ');

        await channel.assertExchange(EXCHANGE, 'topic', { durable: true });
        await channel.assertQueue(QUEUE, { durable: true });
        await channel.bindQueue(QUEUE, EXCHANGE, 'ticket.created.*');
        channel.prefetch(1);

        console.log(` Assignment Worker escuchando en cola: ${QUEUE}`);
        console.log(`   Vinculado a: ${EXCHANGE} con routing key: ticket.created.*`);

        channel.consume(QUEUE, async (msg) => {
            if (!msg) return;

            try {
                const event = JSON.parse(msg.content.toString());
                const { ticketId, priority, title } = event.payload;
                const eventId = event.eventId;

                // ===== IDEMPOTENCIA =====
                if (isEventProcessed(eventId)) {
                    console.log(` [ASSIGN] Evento ${eventId} ya procesado. Ignorando.`);
                    channel.ack(msg);
                    return;
                }

                console.log(`\n [ASSIGN] Evento recibido: ${event.type}`);
                console.log(`   Ticket ID: ${ticketId}`);
                console.log(`   Prioridad: ${priority}`);
                console.log(`   Título: ${title}`);

                // ===== CRITICAL =====
                if (priority === 'critical') {
                    console.log(` [ASSIGN] CRITICAL detectado! Enviando a DLQ...`);
                    const errorEvent = {
                        ...event,
                        error: 'CRITICAL_PRIORITY',
                        failedAt: new Date().toISOString()
                    };
                    channel.publish(
                        EXCHANGE,
                        'ticket.error.critical',
                        Buffer.from(JSON.stringify(errorEvent)),
                        { persistent: true }
                    );
                    console.log(` [ASSIGN] Evento enviado a ticket.error.critical`);
                    markEventAsProcessed(eventId);
                    channel.ack(msg);
                    return;
                }

                // ===== ASIGNACIÓN =====
                const responsables = ['Ana López', 'Carlos Gómez', 'Laura Martínez', 'Miguel Pérez'];
                const asignado = responsables[Math.floor(Math.random() * responsables.length)];
                await new Promise(resolve => setTimeout(resolve, 1500));

                const assignedEvent = {
                    eventId: uuidv4(),
                    type: 'ticket.assigned',
                    occurredAt: new Date().toISOString(),
                    version: 1,
                    payload: {
                        ticketId,
                        assignedTo: asignado,
                        priority,
                        title,
                        assignedAt: new Date().toISOString()
                    }
                };

                const routingKey = `ticket.assigned.${priority}`;
                channel.publish(
                    EXCHANGE,
                    routingKey,
                    Buffer.from(JSON.stringify(assignedEvent)),
                    { persistent: true }
                );

                console.log(` [ASSIGN] Asignado a: ${asignado}`);
                console.log(`   Routing key: ${routingKey}`);

                // ===== MÉTRICAS =====
                updateMetrics();

                // ===== IDEMPOTENCIA =====
                markEventAsProcessed(eventId);

                channel.ack(msg);
                console.log(` [ASSIGN] Evento confirmado (ack)\n`);

            } catch (error) {
                console.error(` [ASSIGN] Error:`, error);
                channel.nack(msg, false, true);
            }
        }, { noAck: false });

    } catch (error) {
        console.error(' Error en Assignment Worker:', error);
        setTimeout(startAssignmentWorker, 5000);
    }
}

startAssignmentWorker();