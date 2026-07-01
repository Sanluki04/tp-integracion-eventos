const express = require('express');
const { v4: uuidv4 } = require('uuid');
const connect = require('../connection');

const app = express();
app.use(express.json());

const EXCHANGE = 'helpdesk.events';

app.post('/tickets', async (req, res) => {
    try {
        const { title, description, priority = 'normal' } = req.body;
        
        if (!title || !description) {
            return res.status(400).json({ 
                error: 'Faltan campos obligatorios: title y description' 
            });
        }

        const { channel } = await connect();
        
        await channel.assertExchange(EXCHANGE, 'topic', { 
            durable: true 
        });

        const ticketId = `TCK-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
        
        const event = {
            eventId: uuidv4(),
            type: 'ticket.created',
            occurredAt: new Date().toISOString(),
            version: 1,
            payload: {
                ticketId,
                title,
                description,
                priority,
                createdAt: new Date().toISOString()
            }
        };

        const routingKey = `ticket.created.${priority}`;
        channel.publish(
            EXCHANGE,
            routingKey,
            Buffer.from(JSON.stringify(event)),
            { persistent: true }
        );

        console.log(`\n [API] Evento publicado:`);
        console.log(`   Tipo: ${event.type}`);
        console.log(`   Routing key: ${routingKey}`);
        console.log(`   Ticket ID: ${ticketId}`);
        console.log(`   Prioridad: ${priority}`);
        console.log(`   Título: ${title}\n`);

        res.status(201).json({
            message: 'Ticket creado exitosamente',
            ticketId,
            event
        });

    } catch (error) {
        console.error(' [API] Error:', error);
        res.status(500).json({ 
            error: 'Error interno del servidor' 
        });
    }
});

const PORT = 3000;
app.listen(PORT, () => {
    console.log(` API Service corriendo en http://localhost:${PORT}`);
    console.log(`   Exchange: ${EXCHANGE} (topic)`);
    console.log(`   Endpoint: POST /tickets`);
    console.log(`   Body: { "title": "...", "description": "...", "priority": "high|normal|low|critical" }\n`);
});