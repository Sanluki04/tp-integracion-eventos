# TP Comunicación por Eventos con Broker

**Materia:** Integración de Aplicaciones  
**Alumnos:** Collazo Mariano, Perez Santiago  
**Profesor:** Arriagada Alejandro  
**Fecha:** Julio 2026

---

##  Descripción

Sistema de mesa de ayuda orientado a eventos utilizando **RabbitMQ** como broker de mensajes. El sistema está compuesto por tres servicios independientes que se comunican de forma asincrónica mediante eventos:

1. **API Service**: Recibe solicitudes `POST /tickets` y publica el evento `ticket.created`
2. **Assignment Worker**: Consume `ticket.created`, asigna responsable y publica `ticket.assigned`
3. **Audit Worker**: Consume `ticket.#` y guarda todos los eventos en auditoría

---

##  Tecnologías utilizadas

- Node.js
- Express
- RabbitMQ
- amqplib
- Docker
- uuid

---

##  Estructura del proyecto

tp-integracion-eventos/
├── api-service/
│   └── index.js
├── workers/
│   ├── assignment-worker.js
│   └── audit-worker.js
├── logs/
│   └── audit.log
├── connection.js
├── docker-compose.yml
├── package.json
├── package-lock.json
├── .gitignore
├── metrics.json
├── processed-events.json
└── README.md

---

## 🚀 Instalación y ejecución

### 1. Clonar o descargar el proyecto

git clone <url-del-repositorio>
cd tp-integracion-eventos

### 2. Instalar dependencias

npm install

### 3. Levantar RabbitMQ con Docker

npm run rabbitmq:up
# o
docker-compose up -d

### 4. Ejecutar los servicios (abrir 3 terminales)

# Terminal 1 - API Service
npm run api

# Terminal 2 - Assignment Worker
npm run worker:assign

# Terminal 3 - Audit Worker
npm run worker:audit

### 5. Probar el sistema

# Crear un ticket normal
curl -X POST http://localhost:3000/tickets \
  -H "Content-Type: application/json" \
  -d '{"title":"No puedo ingresar","description":"Error 403","priority":"high"}'

# Crear un ticket crítico (va a DLQ)
curl -X POST http://localhost:3000/tickets \
  -H "Content-Type: application/json" \
  -d '{"title":"Sistema caído","description":"Error crítico","priority":"critical"}'

### 6. Ver RabbitMQ Management

http://localhost:15672
Usuario: guest / Contraseña: guest

### 7. Ver archivos generados

# Auditoría
cat logs/audit.log

# Métricas diarias
cat metrics.json

# Eventos procesados (idempotencia)
cat processed-events.json

### Mejoras Implementadas

## Routing por prioridad: 
ticket.created.high
ticket.created.normal
ticket.created.low
ticket.created.critical

## Contador de tickets por día (metrics.json):
Guarda en metrics.json la cantidad de tickets creados por día

## Fallo con prioridad "critical":
Los tickets con prioridad critical se envían a ticket.error.critical (DLQ) y no se asignan

## Idempotencia (processed-events.json):
Evita duplicados usando processed-events.json, verificando eventId antes de procesar

### Comandos útiles

## Terminal 1
npm run api:
Levanta el API Service

## Terminal 2
npm run worker:assign:
Levanta el Assignment Worker

## Terminal 3
npm run worker:audit:
Levanta el Audit Worker

npm run rabbitmq:up:
Levanta RabbitMQ con Docker

npm run rabbitmq:down:
Detiene RabbitMQ

npm run rabbitmq:logs:
Ver logs de RabbitMQ

### Requisitos cumplidos
- RabbitMQ con Docker Compose
- Endpoint POST /tickets
- Eventos con eventId, type, occurredAt, version payload
- Worker que consume ticket.created y publica ticket.assigned
- Worker de auditoría que consume ticket.#
- Routing por prioridad
- Contador de tickets por día (metrics.json)
- Fallo con prioridad critical
- Idempotencia (processed-events.json)
- Logs claros en consola

### Evidencia de prueba

Las capturas de funcionamiento (Postman, RabbitMQ, terminales) se encuentran en el documento de entrega.