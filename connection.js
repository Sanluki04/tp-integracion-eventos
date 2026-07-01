const amqp = require('amqplib');

const RABBITMQ_URL = 'amqp://localhost';

async function connect() {
    try {
        const connection = await amqp.connect(RABBITMQ_URL);
        const channel = await connection.createChannel();
        console.log(' Conectado a RabbitMQ exitosamente');
        return { connection, channel };
    } catch (error) {
        console.error(' Error conectando a RabbitMQ:', error.message);
        throw error;
    }
}

module.exports = connect;