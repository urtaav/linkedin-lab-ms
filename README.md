# Digital Banking System — Microservices Architecture

> Sistema bancario digital completo con detección de fraude en tiempo real, pagos con Razorpat y notificaciones asíncronas via Kafka.

---

## Tabla de Contenidos

1. [Arquitectura del Sistema](#1-arquitectura-del-sistema)
2. [Stack Tecnológico](#2-stack-tecnológico)
3. [Puertos y Servicios](#3-puertos-y-servicios)
4. [Requisitos Previos](#4-requisitos-previos)
5. [Levantamiento del Ambiente (Docker)](#5-levantamiento-del-ambiente-docker)
6. [Levantamiento de Microservicios (Local)](#6-levantamiento-de-microservicios-local)
7. [Endpoints REST — Postman Ready](#7-endpoints-rest--postman-ready)
8. [Escenarios de Prueba — Flujo Completo](#8-escenarios-de-prueba--flujo-completo)
9. [Detección de Fraude — Guía Detallada](#9-detección-de-fraude--guía-detallada)
10. [Mapa de Topics Kafka](#10-mapa-de-topics-kafka)
11. [Flujo SAGA — Transferencia](#11-flujo-saga--transferencia)
12. [Implementación de Correos con Plantilla Bancaria](#12-implementación-de-correos-con-plantilla-bancaria)
13. [Consideraciones para Dockerizar los Microservicios](#13-consideraciones-para-dockerizar-los-microservicios)
14. [Troubleshooting](#14-troubleshooting)

---

## 1. Arquitectura del Sistema

```
                         ┌─────────────────────────────┐
                         │        CLIENTE / API         │
                         └──────────────┬──────────────┘
                                        │
                         ┌──────────────▼──────────────┐
                         │      API GATEWAY (8080)      │
                         │   Rate Limiting por IP       │
                         └──┬───────────┬───────────┬──┘
                            │           │           │
                ┌───────────▼──┐  ┌─────▼────────┐  ┌▼──────────────┐
                │  ACCOUNT     │  │  TRANSACTION  │  │   PAYMENT     │
                │  SERVICE     │  │  SERVICE      │  │   SERVICE     │
                │  (8081)      │  │  (8082)       │  │   (8083)      │
                │  MySQL       │  │  MySQL+Redis  │  │   MySQL       │
                └──────┬───────┘  └───────┬───────┘  └───────┬───────┘
                       │                  │                   │
          ┌────────────┤         Kafka    │          Kafka    │
          │  Feign     │         Topics   │          Topics   │
          │  HTTP      │                  │                   │
   ┌──────▼──────┐  ┌─▼──────────────────▼──┐        ┌──────▼──────┐
   │   FRAUD     │  │      KAFKA BROKER      │        │ NOTIFICATION│
   │   DETECTION │  │      (Confluent)       │        │   SERVICE   │
   │   SERVICE   │◄─┤                        ├───────►│   (8085)    │
   │   (8084)    │  │  Topics:               │        │  Email/Logs │
   │   Redis     │  │  transaction.initiated │        └─────────────┘
   └─────────────┘  │  fraud.check.clean     │
                    │  verification.required  │
                    │  transaction.completed  │
                    │  fraud.detected         │
                    │  transaction.refunded   │
                    │  payment.completed      │
                    │  payment.failed         │
                    └─────────────────────────┘
```

### Comunicación entre Servicios

| Tipo | Desde | Hacia | Protocolo |
|------|-------|-------|-----------|
| Síncrona | API Gateway | Account/Transaction/Payment | HTTP (Proxy) |
| Síncrona | Transaction Service | Account Service | Feign (HTTP) |
| Síncrona | Fraud Detection | Account Service | Feign (HTTP) |
| Asíncrona | Transaction Service | Fraud Detection | Kafka |
| Asíncrona | Fraud Detection | Transaction Service | Kafka |
| Asíncrona | Transaction Service | Account Service | Kafka |
| Asíncrona | Transaction Service | Notification Service | Kafka |
| Asíncrona | Payment Service | Notification Service | Kafka |

---

## 2. Stack Tecnológico

| Componente | Versión | Propósito |
|-----------|---------|-----------|
| Java | 17 | Runtime |
| Spring Boot | 3.2.0 | Framework base |
| Spring Cloud | 2023.0.0 | Gateway, Feign |
| Spring Kafka | 3.2.0 | Event streaming |
| Spring Data Redis | 3.2.0 | Rate limiting, OTP, Fraud patterns |
| Spring Data JPA | 3.2.0 | ORM para MySQL |
| Spring Mail | 3.2.0 | Envío de correos |
| MySQL | 8.0 | Base de datos |
| Redis | latest | Cache y patrones de fraude |
| Apache Kafka | 7.4.0 (Confluent) | Mensajería asíncrona |
| Zookeeper | 7.4.0 (Confluent) | Coordinación Kafka |
| Razorpay SDK | 1.4.3 | Procesamiento de pagos |
| Lombok | latest | Reducción de boilerplate |

---

## 3. Puertos y Servicios

### Infraestructura (Docker)

| Puerto | Servicio | Descripción |
|--------|----------|-------------|
| 6379 | Redis | Rate limiting, OTP storage, Fraud patterns |
| 3306 | MySQL | Base de datos principal |
| 2181 | Zookeeper | Coordinación Kafka (interno) |
| 9092 | Kafka | Broker externo (para servicios fuera de Docker) |
| 29092 | Kafka | Broker interno (entre contenedores Docker) |

### Microservicios (Local)

| Puerto | Servicio | Base de Datos | Kafka Topics (Publica) | Kafka Topics (Escucha) |
|--------|----------|--------------|------------------------|----------------------|
| 8080 | API Gateway | - | - | - |
| 8081 | Account Service | account_db | transaction.completed, fraud.detected | - |
| 8082 | Transaction Service | transaction_db | transaction.initiated, transaction.completed, fraud.detected, transaction.refunded, transaction.otp.generated | verification.required, fraud.check.clean |
| 8083 | Payment Service | payment_db | payment.completed, payment.failed | - |
| 8084 | Fraud Detection | - | verification.required, fraud.check.clean | transaction.initiated |
| 8085 | Notification Service | - | - | transaction.completed, fraud.detected, transaction.otp.generated, transaction.refunded, payment.completed, payment.failed |

---

## 4. Requisitos Previos

```bash
# Verificar instalaciones
java -version          # Necesita Java 17+
mvn -version           # Necesita Maven 3.8+
docker --version       # Necesita Docker Desktop
docker compose version # Necesita Docker Compose V2
```

---

## 5. Levantamiento del Ambiente (Docker)

### Paso 1: Levantar infraestructura

```bash
# Desde la raíz del proyecto
docker compose up -d
```

Esto levanta:
- **Redis** en `localhost:6379`
- **MySQL** en `localhost:3306` (con base de datos `account_db` pre-creada)
- **Zookeeper** en puerto interno `2181`
- **Kafka** en `localhost:9092` (externo) y `kafka:29092` (interno entre contenedores)

### Paso 2: Verificar que los servicios estén corriendo

```bash
docker compose ps
# Debes ver: redis, mysql, zookeeper, kafka — todos "Up"
```

### Paso 3: Verificar Kafka está listo

```bash
# Esperar ~30 segundos después del docker compose up
# Luego verificar que Kafka acepta conexiones:
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
# Debe retornar una lista vacía o los topics existentes
```

### Paso 4: Verificar MySQL está listo

```bash
docker exec mysql mysql -uroot -proot -e "SHOW DATABASES;"
# Debe mostrar: account_db, information_schema, mysql, performance_schema, sys
```

### Comandos Útiles Docker

```bash
# Ver logs de un servicio
docker compose logs -f kafka
docker compose logs -f mysql
docker compose logs -f redis

# Detener todo
docker compose down

# Detener y eliminar volúmenes (limpiar datos)
docker compose down -v

# Reiniciar un servicio específico
docker compose restart kafka
```

---

## 6. Levantamiento de Microservicios (Local)

### Orden de levantamiento (IMPORTANTE)

El orden es crítico porque los servicios dependen entre sí:

```
INFRAESTRUCTURA (Docker)
    │
    ▼
1. Account Service (8081)          ← Primero: es la base de datos de cuentas
    │
    ├── 2. Transaction Service (8082)  ← Necesita Account Service para Feign calls
    │
    ├── 3. Payment Service (8083)      ← Independiente pero usa Kafka
    │
    ├── 4. Fraud Detection (8084)      ← Necesita Account Service para Feign calls
    │
    ├── 5. Notification Service (8085) ← Solo escucha Kafka
    │
    └── 6. API Gateway (8080)          ← Último: enruta a todos los demás
```

### Script de levantamiento (Windows PowerShell)

```powershell
# Terminal 1: Account Service (PRIMERO)
cd account-service
mvn spring-boot:run

# Terminal 2: Transaction Service
cd transaction-service
mvn spring-boot:run

# Terminal 3: Payment Service
cd payment-service
mvn spring-boot:run

# Terminal 4: Fraud Detection Service
cd fraud-detection-service
mvn spring-boot:run

# Terminal 5: Notification Service
cd notification-service
mvn spring-boot:run

# Terminal 6: API Gateway (ÚLTIMO)
cd api-gateway
mvn spring-boot:run
```

### Script de levantamiento (Linux/Mac)

```bash
# Terminal 1
cd account-service && mvn spring-boot:run

# Terminal 2
cd transaction-service && mvn spring-boot:run

# Terminal 3
cd payment-service && mvn spring-boot:run

# Terminal 4
cd fraud-detection-service && mvn spring-boot:run

# Terminal 5
cd notification-service && mvn spring-boot:run

# Terminal 6
cd api-gateway && mvn spring-boot:run
```

### Verificar que todos estén corriendo

```bash
# Health checks
curl http://localhost:8080/actuator/health  # API Gateway
curl http://localhost:8081/actuator/health  # Account Service
curl http://localhost:8082/actuator/health  # Transaction Service
curl http://localhost:8083/actuator/health  # Payment Service
curl http://localhost:8084/actuator/health  # Fraud Detection
curl http://localhost:8085/actuator/health  # Notification Service
```

---

## 7. Endpoints REST — Postman Ready

> **Base URL via Gateway:** `http://localhost:8080`
> **Base URL directa:** `http://localhost:{puerto}`

---

### 7.1 ACCOUNT SERVICE (8081)

---

#### 1. Crear Cuenta Bancaria

```
POST http://localhost:8080/api/v1/accounts
Content-Type: application/json

{
    "accountHolderName": "Juan Pérez",
    "email": "juan.perez@email.com",
    "phone": "5551234567",
    "accountType": "SAVINGS",
    "initialDeposit": 10000.00
}
```

**Tipos de cuenta válidos:** `SAVINGS`, `CURRENT`, `FIXED_DEPOSIT`

**Respuesta (201 Created):**
```json
{
    "id": "uuid-aqui",
    "accountNumber": "000123456789",
    "accountHolderName": "Juan Pérez",
    "email": "juan.perez@email.com",
    "phone": "5551234567",
    "accountType": "SAVINGS",
    "status": "ACTIVE",
    "balance": 10000.00,
    "dailyTransactionLimit": 100000.00,
    "createdAt": "2026-07-23T10:30:00"
}
```

**GUARDA el `accountNumber` — lo necesitarás para todos los demás requests.**

---

#### 2. Consultar Cuenta por Número

```
GET http://localhost:8080/api/v1/accounts/{accountNumber}
```

**Ejemplo:**
```
GET http://localhost:8080/api/v1/accounts/000123456789
```

---

#### 3. Consultar Saldo

```
GET http://localhost:8080/api/v1/accounts/{accountNumber}/balance
```

**Respuesta:** `10000.00` (BigDecimal)

---

#### 4. Bloquear Cuenta

```
PUT http://localhost:8080/api/v1/accounts/{accountNumber}/block
```

**Respuesta:** `"Account 000123456789 has been blocked successfully"`

---

#### 5. Debitar Cuenta (interno — SAGA step 1)

```
PUT http://localhost:8080/api/v1/accounts/{accountNumber}/deduct?amount=500.00
```

**Nota:** Este endpoint es usado internamente por Transaction Service vía Feign. También puedes probarlo directamente.

---

#### 6. Acreditar Cuenta (interno — compensación SAGA)

```
PUT http://localhost:8080/api/v1/accounts/{accountNumber}/credit?amount=500.00
```

---

### 7.2 TRANSACTION SERVICE (8082)

---

#### 1. Iniciar Transferencia

```
POST http://localhost:8080/api/v1/transactions/transfer
Content-Type: application/json

{
    "senderAccountNumber": "000123456789",
    "receiverAccountNumber": "000987654321",
    "amount": 500.00,
    "description": "Pago de servicios"
}
```

**Respuesta (201 Created):**
```json
{
    "id": "uuid-transaccion",
    "senderAccountNumber": "000123456789",
    "receiverAccountNumber": "000987654321",
    "amount": 500.00,
    "type": "TRANSFER",
    "status": "PROCESSING",
    "description": "Pago de servicios",
    "referenceNumber": null,
    "failureReason": null,
    "createdAt": "2026-07-23T10:35:00",
    "completedAt": null
}
```

**IMPORTANTE:** La transacción inicia como `PROCESSING`. El estado final depende del resultado de fraud detection:
- Si es limpia → `COMPLETED` (automático, ~1-2 segundos)
- Si es sospechosa → `PENDING_VERIFICATION` (necesitas OTP)

---

#### 2. Consultar Transacción por ID

```
GET http://localhost:8080/api/v1/transactions/{transactionId}
```

---

#### 3. Historial de Transacciones por Cuenta

```
GET http://localhost:8080/api/v1/transactions/account/{accountNumber}
```

**Respuesta:** Array de `TransactionResponse[]`

---

#### 4. Verificar OTP (cuando fraud detection flaggea)

```
POST http://localhost:8080/api/v1/transactions/{transactionId}/verify?otp=123456
```

**Escenarios:**
- OTP correcto → status: `COMPLETED`
- OTP incorrecto → status: `FLAGGED` + cuenta bloqueada + reembolso automático
- OTP expirado (5 min) → status: `FLAGGED` + reembolso automático

---

### 7.3 PAYMENT SERVICE (8083)

---

#### 1. Crear Orden de Pago Razorpay

```
POST http://localhost:8080/api/v1/payments/create-order
Content-Type: application/json

{
    "accountNumber": "000123456789",
    "amount": 500.00,
    "description": "Compra en tienda"
}
```

**Respuesta (201 Created):**
```json
{
    "paymentId": "uuid-pago",
    "razorpayOrderId": "order_abc123",
    "amount": 500.00,
    "currency": "INR",
    "razorpayKeyId": "your_key_id",
    "status": "CREATED"
}
```

**Nota:** Requiere credenciales Razorpay válidas en `application.yml`.

---

#### 2. Webhook de Razorpay (callback)

```
POST http://localhost:8083/api/v1/payments/webhook
Content-Type: application/json

{
    "event": "payment.captured",
    "payload": {
        "payment": {
            "entity": {
                "order_id": "order_abc123",
                "id": "pay_xyz789"
            }
        }
    }
}
```

**Nota:** Este endpoint se llama directamente (no vía gateway) porque Razorpay lo invoca.

---

### 7.4 FRAUD DETECTION SERVICE (8084)

> **No tiene endpoints REST.** Es completamente event-driven via Kafka.

---

### 7.5 NOTIFICATION SERVICE (8085)

> **No tiene endpoints REST.** Es completamente event-driven via Kafka.

---

### 7.6 ACTUATOR ENDPOINTS

```
GET http://localhost:8080/actuator/health    # API Gateway
GET http://localhost:8081/actuator/health    # Account Service
GET http://localhost:8082/actuator/health    # Transaction Service
GET http://localhost:8083/actuator/health    # Payment Service
GET http://localhost:8084/actuator/health    # Fraud Detection
GET http://localhost:8085/actuator/health    # Notification Service
```

---

## 8. Escenarios de Prueba — Flujo Completo

### ESCENARIO 1: Transferencia limpia (sin fraude)

> Transferencia pequeña que no activa ninguna regla de fraude.

**Paso 1:** Crear cuenta emisora
```
POST http://localhost:8080/api/v1/accounts
Content-Type: application/json

{
    "accountHolderName": "Juan Pérez",
    "email": "juan@email.com",
    "phone": "5551000001",
    "accountType": "SAVINGS",
    "initialDeposit": 10000.00
}
```
→ Guarda `accountNumber` como `SENDER_ACC`

**Paso 2:** Crear cuenta receptora
```
POST http://localhost:8080/api/v1/accounts
Content-Type: application/json

{
    "accountHolderName": "María García",
    "email": "maria@email.com",
    "phone": "5551000002",
    "accountType": "SAVINGS",
    "initialDeposit": 5000.00
}
```
→ Guarda `accountNumber` como `RECEIVER_ACC`

**Paso 3:** Verificar saldos iniciales
```
GET http://localhost:8080/api/v1/accounts/{SENDER_ACC}/balance
GET http://localhost:8080/api/v1/accounts/{RECEIVER_ACC}/balance
```

**Paso 4:** Transferir monto pequeño (limpio)
```
POST http://localhost:8080/api/v1/transactions/transfer
Content-Type: application/json

{
    "senderAccountNumber": "{SENDER_ACC}",
    "receiverAccountNumber": "{RECEIVER_ACC}",
    "amount": 200.00,
    "description": "Cena entre amigos"
}
```
→ Guarda `transactionId`

**Paso 5:** Esperar 2-3 segundos, luego verificar
```
GET http://localhost:8080/api/v1/transactions/{transactionId}
```
→ status debe ser `COMPLETED`

**Paso 6:** Verificar saldos finales
```
GET http://localhost:8080/api/v1/accounts/{SENDER_ACC}/balance
→ Debe ser 9800.00 (10000 - 200)

GET http://localhost:8080/api/v1/accounts/{RECEIVER_ACC}/balance
→ Debe ser 5200.00 (5000 + 200)
```

**Paso 7:** Ver logs de notificación en la terminal de Notification Service:
```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
NOTIFICATION SENT
Account : {SENDER_ACC}
Subject : DEBIT ALERT
Message : ₹200.0 debited from account {SENDER_ACC}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
NOTIFICATION SENT
Account : {RECEIVER_ACC}
Subject : CREDIT ALERT
Message : ₹200.0 credited to account {RECEIVER_ACC}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

---

### ESCENARIO 2: Fraude por VELOCIDAD (>5 transacciones en 60 segundos)

> Envía 6 transferencias rápidas para superar el límite de 5/min.

**Setup:** Usa las cuentas del Escenario 1.

**Paso 1:** Ejecutar 6 transferencias rápidas (una tras otra, sin pausa)

Transferencia 1:
```
POST http://localhost:8080/api/v1/transactions/transfer
Content-Type: application/json

{
    "senderAccountNumber": "{SENDER_ACC}",
    "receiverAccountNumber": "{RECEIVER_ACC}",
    "amount": 10.00,
    "description": "Velocity test 1"
}
```

Transferencia 2:
```
POST http://localhost:8080/api/v1/transactions/transfer
Content-Type: application/json

{
    "senderAccountNumber": "{SENDER_ACC}",
    "receiverAccountNumber": "{RECEIVER_ACC}",
    "amount": 10.00,
    "description": "Velocity test 2"
}
```

Transferencia 3:
```
POST http://localhost:8080/api/v1/transactions/transfer
Content-Type: application/json

{
    "senderAccountNumber": "{SENDER_ACC}",
    "receiverAccountNumber": "{RECEIVER_ACC}",
    "amount": 10.00,
    "description": "Velocity test 3"
}
```

Transferencia 4:
```
POST http://localhost:8080/api/v1/transactions/transfer
Content-Type: application/json

{
    "senderAccountNumber": "{SENDER_ACC}",
    "receiverAccountNumber": "{RECEIVER_ACC}",
    "amount": 10.00,
    "description": "Velocity test 4"
}
```

Transferencia 5:
```
POST http://localhost:8080/api/v1/transactions/transfer
Content-Type: application/json

{
    "senderAccountNumber": "{SENDER_ACC}",
    "receiverAccountNumber": "{RECEIVER_ACC}",
    "amount": 10.00,
    "description": "Velocity test 5"
}
```

Transferencia 6 (esta debería ser flaggeada):
```
POST http://localhost:8080/api/v1/transactions/transfer
Content-Type: application/json

{
    "senderAccountNumber": "{SENDER_ACC}",
    "receiverAccountNumber": "{RECEIVER_ACC}",
    "amount": 10.00,
    "description": "Velocity test 6 - FLAGGED"
}
```

**Paso 2:** Verificar la transacción 6
```
GET http://localhost:8080/api/v1/transactions/{transactionId6}
```
→ status debe ser `PENDING_VERIFICATION`

**Paso 3:** Verificar OTP en los logs de Notification Service
```
NOTIFICATION SENT
Account : {SENDER_ACC}
Subject : 🔐 TRANSACTION VERIFICATION REQUIRED
Message : Suspicious activity detected... Your OTP is: 123456...
```

**Paso 4:** Responder con OTP correcto
```
POST http://localhost:8080/api/v1/transactions/{transactionId6}/verify?otp={OTP_DEL_LOG}
```

**Paso 5:** Verificar que la transacción se completó
```
GET http://localhost:8080/api/v1/transactions/{transactionId6}
→ status: COMPLETED
```

**Alternativa — OTP incorrecto (cuenta bloqueada + reembolso):**
```
POST http://localhost:8080/api/v1/transactions/{transactionId6}/verify?otp=000000
```
→ status: `FLAGGED` + cuenta bloqueada + reembolso automático

---

### ESCENARIO 3: Fraude por MONTO SOSPECHOSO (>3x promedio)

> Primero establece un promedio con transferencias normales, luego envía una grande.

**Setup:** Crea una cuenta nueva para aislar el test.

**Paso 1:** Crear cuenta
```
POST http://localhost:8080/api/v1/accounts
Content-Type: application/json

{
    "accountHolderName": "Test Amount Fraud",
    "email": "amount.fraud@test.com",
    "phone": "5552000001",
    "accountType": "SAVINGS",
    "initialDeposit": 100000.00
}
```
→ Guarda como `AMOUNT_TEST_ACC`

**Paso 2:** Crear cuenta receptora
```
POST http://localhost:8080/api/v1/accounts
Content-Type: application/json

{
    "accountHolderName": "Receiver Amount Test",
    "email": "receiver.amount@test.com",
    "phone": "5552000002",
    "accountType": "SAVINGS",
    "initialDeposit": 10000.00
}
```
→ Guarda como `AMOUNT_RECEIVER`

**Paso 3:** Establecer promedio con 3 transferencias normales (~100 cada una)
```
POST http://localhost:8080/api/v1/transactions/transfer
Content-Type: application/json

{
    "senderAccountNumber": "{AMOUNT_TEST_ACC}",
    "receiverAccountNumber": "{AMOUNT_RECEIVER}",
    "amount": 100.00,
    "description": "Normal txn 1"
}
```
Repetir 2 más veces con amount=100.

**Paso 4:** Transferir un monto grande (>3x promedio = >300)
```
POST http://localhost:8080/api/v1/transactions/transfer
Content-Type: application/json

{
    "senderAccountNumber": "{AMOUNT_TEST_ACC}",
    "receiverAccountNumber": "{AMOUNT_RECEIVER}",
    "amount": 500.00,
    "description": "SUSPICIOUS - 5x average"
}
```

**Paso 5:** Verificar
```
GET http://localhost:8080/api/v1/transactions/{transactionId}
→ status: PENDING_VERIFICATION
→ Fraud reason: "Unusual transaction amount - exceeds 3x your average"
```

---

### ESCENARIO 4: Fraude por SALDO (>90% del balance)

> Transferir más del 90% del saldo de la cuenta.

**Paso 1:** Crear cuenta con saldo conocido
```
POST http://localhost:8080/api/v1/accounts
Content-Type: application/json

{
    "accountHolderName": "Balance Fraud Test",
    "email": "balance.fraud@test.com",
    "phone": "5553000001",
    "accountType": "SAVINGS",
    "initialDeposit": 10000.00
}
```
→ Guarda como `BAL_ACC`

**Paso 2:** Crear cuenta receptora
```
POST http://localhost:8080/api/v1/accounts
Content-Type: application/json

{
    "accountHolderName": "Receiver Balance Test",
    "email": "receiver.balance@test.com",
    "phone": "5553000002",
    "accountType": "SAVINGS",
    "initialDeposit": 1000.00
}
```
→ Guarda como `BAL_RECEIVER`

**Paso 3:** Transferir >90% del saldo (9100 de 10000)
```
POST http://localhost:8080/api/v1/transactions/transfer
Content-Type: application/json

{
    "senderAccountNumber": "{BAL_ACC}",
    "receiverAccountNumber": "{BAL_RECEIVER}",
    "amount": 9100.00,
    "description": "Almost all balance"
}
```

**Paso 4:** Verificar
```
GET http://localhost:8080/api/v1/transactions/{transactionId}
→ status: PENDING_VERIFICATION
→ Fraud reason: "Transaction exceed 90% of account balance"
```

**Paso 5:** Verificar OTP correcto desde los logs de Notification Service y completar.

---

### ESCENARIO 5: OTP incorrecto → Cuenta bloqueada + Reembolso

**Paso 1:** Trigger fraude (cualquier escenario anterior que genere PENDING_VERIFICATION)

**Paso 2:** Enviar OTP incorrecto
```
POST http://localhost:8080/api/v1/transactions/{transactionId}/verify?otp=000000
```

**Paso 3:** Verificar resultado
```
GET http://localhost:8080/api/v1/transactions/{transactionId}
→ status: FLAGGED
```

**Paso 4:** Verificar que la cuenta fue bloqueada
```
GET http://localhost:8080/api/v1/accounts/{SENDER_ACC}
→ status: BLOCKED
```

**Paso 5:** Verificar reembolso en logs de Notification Service
```
NOTIFICATION SENT
Account : {SENDER_ACC}
Subject : 💰 REFUND PROCESSED
Message : Your transaction of ₹X was cancelled... ₹X has been refunded to account {SENDER_ACC}
```

**Paso 6:** Verificar saldo restaurado
```
GET http://localhost:8080/api/v1/accounts/{SENDER_ACC}/balance
→ Debe ser igual al saldo anterior a la transferencia
```

---

### ESCENARIO 6: OTP expirado (después de 5 minutos)

**Paso 1:** Trigger fraude y obtener PENDING_VERIFICATION

**Paso 2:** Esperar 5+ minutos

**Paso 3:** Intentar verificar
```
POST http://localhost:8080/api/v1/transactions/{transactionId}/verify?otp=123456
```

**Paso 4:** Verificar resultado
```
→ status: FLAGGED
→ Reembolso automático al emisor
```

---

### ESCENARIO 7: Bloquear cuenta manualmente

```
PUT http://localhost:8080/api/v1/accounts/{accountNumber}/block
```

**Paso 2:** Intentar transferir desde la cuenta bloqueada
```
POST http://localhost:8080/api/v1/transactions/transfer
Content-Type: application/json

{
    "senderAccountNumber": "{BLOCKED_ACC}",
    "receiverAccountNumber": "{RECEIVER_ACC}",
    "amount": 100.00,
    "description": "Should fail"
}
```
→ Error: La cuenta está bloqueada

---

## 9. Detección de Fraude — Guía Detallada

### Reglas de Detección

| # | Regla | Umbral | Redis Key | TTL | Acción |
|---|-------|--------|-----------|-----|--------|
| 1 | **Velocidad** | >5 transacciones en 60s | `fraud:velocity{accountNumber}` | 60s | Flaggea + OTP |
| 2 | **Monto Sospechoso** | >3x promedio histórico | `fraud:avg_amount{accountNumber}` | Sin TTL | Flaggea + OTP |
| 3 | **Porcentaje de Saldo** | >90% del saldo | N/A | N/A | Flaggea + OTP |

### Flujo de Decisión

```
Transacción Recibida (Kafka: transaction.initiated)
    │
    ▼
Obtener saldo real vía Feign GET /accounts/{sender}/balance
    │
    ▼
┌─────────────────────────────────────┐
│ ¿Velocity > 5 en 60s?              │
│ Redis: INCR fraud:velocity{acc}     │
│ Si count > 5 → FRAUD               │
└──────────────────┬──────────────────┘
                   │ No
                   ▼
┌─────────────────────────────────────┐
│ ¿Amount > 3x promedio?             │
│ Redis: GET fraud:avg_amount{acc}    │
│ Si amount > avg*3 → FRAUD          │
│ Actualiza promedio en Redis         │
└──────────────────┬──────────────────┘
                   │ No
                   ▼
┌─────────────────────────────────────┐
│ ¿Amount > 90% del saldo?           │
│ maxAllowed = balance * 0.90         │
│ Si amount > maxAllowed → FRAUD     │
└──────────────────┬──────────────────┘
                   │ No
                   ▼
              ✅ CLEAN
    Publica a fraud.check.clean
    → Transacción COMPLETED
```

### Redis Patterns Utilizados

| Key | Servicio | TTL | Descripción |
|-----|----------|-----|-------------|
| `fraud:velocity{accountNumber}` | Fraud Detection | 60 segundos | Contador de transacciones por minuto |
| `fraud:avg_amount{accountNumber}` | Fraud Detection | Sin TTL | Promedio ejecutivo de montos |
| `verification:otp{transactionId}` | Transaction Service | 5 minutos | OTP para verificación step-up |

### Testing de Fraud Detection con Redis CLI

```bash
# Ver velocity counter de una cuenta
docker exec redis redis-cli GET "fraud:velocity000123456789"

# Ver promedio de monto
docker exec redis redis-cli GET "fraud:avg_amount000123456789"

# Ver OTP almacenado
docker exec redis redis-cli GET "verification:otp{transactionId}"

# Limpiar datos de fraude para re-testear
docker exec redis redis-cli DEL "fraud:velocity000123456789"
docker exec redis redis-cli DEL "fraud:avg_amount000123456789"
```

---

## 10. Mapa de Topics Kafka

| # | Topic | Publicador | Consumidor | Payload |
|---|-------|-----------|-----------|---------|
| 1 | `transaction.initiated` | Transaction Service | Fraud Detection | transactionId, senderAccountNumber, receiverAccountNumber, amount, description |
| 2 | `fraud.check.clean` | Fraud Detection | Transaction Service | transactionId, isFraud=false, reason=null |
| 3 | `verification.required` | Fraud Detection | Transaction Service | transactionId, accountNumber, amount, reason |
| 4 | `transaction.otp.generated` | Transaction Service | Notification Service | transactionId, accountNumber, reason, otp, amount |
| 5 | `transaction.completed` | Transaction Service | Account Service, Notification Service | transactionId, senderAccountNumber, receiverAccountNumber, amount |
| 6 | `fraud.detected` | Transaction Service | Account Service (bloquea), Notification Service | transactionId, accountNumber, reason |
| 7 | `transaction.refunded` | Transaction Service | Notification Service | transactionId, senderAccountNumber, amount, reason |
| 8 | `payment.completed` | Payment Service | Notification Service | paymentId, accountNumber, amount, razorpayPaymentId |
| 9 | `payment.failed` | Payment Service | Notification Service | paymentId, accountNumber, amount, reason |

### Verificar topics en Kafka

```bash
# Listar todos los topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Ver detalles de un topic
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic transaction.initiated

# Consumir mensajes de un topic (para debugging)
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic transaction.completed --from-beginning
```

---

## 11. Flujo SAGA — Transferencia

El patrón SAGA gestiona la transferencia distribuida con compensación automática en caso de fallo.

```
┌─────────────────────────────────────────────────────────────────┐
│                    SAGA: Money Transfer                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Step 1: Deduct Sender Balance                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ Transaction Service ──Feign PUT──> Account Service      │    │
│  │                                    deduct(amount)       │    │
│  │  ✓ Balance suficiente → deducted                        │    │
│  │  ✗ Balance insuficiente → SAGA ABORT (400 error)        │    │
│  └─────────────────────────────────────────────────────────┘    │
│                          │                                       │
│                          ▼                                       │
│  Step 2: Publish to Kafka                                       │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ Transaction Service ──Kafka──> Fraud Detection          │    │
│  │               topic: transaction.initiated              │    │
│  └─────────────────────────────────────────────────────────┘    │
│                          │                                       │
│               ┌──────────┴──────────┐                           │
│               ▼                     ▼                            │
│        FRAUD DETECTED           CLEAN                           │
│               │                     │                            │
│               ▼                     ▼                            │
│  ┌─────────────────────┐  ┌─────────────────────────────┐      │
│  │ OTP Verification    │  │ Auto-Complete               │      │
│  │                     │  │                             │      │
│  │ OTP correcto:       │  │ status → COMPLETED          │      │
│  │  → COMPLETED        │  │ → Credit receiver           │      │
│  │                     │  │ → Notify both               │      │
│  │ OTP incorrecto:     │  └─────────────────────────────┘      │
│  │  → BLOCK account    │                                        │
│  │  → COMPENSATE:      │                                        │
│  │    credit sender    │                                        │
│  │  → FLAGGED          │                                        │
│  │                     │                                        │
│  │ OTP expirado:       │                                        │
│  │  → COMPENSATE:      │                                        │
│  │    credit sender    │                                        │
│  │  → FLAGGED          │                                        │
│  └─────────────────────┘                                        │
│                                                                 │
│  COMPENSATION:                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ Transaction Service ──Feign PUT──> Account Service      │    │
│  │                                    credit(amount)       │    │
│  │  → Reembolsa al emisor                                  │    │
│  │  → Publica transaction.refunded                         │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 12. Implementación de Correos con Plantilla Bancaria

### Estado Actual

El `NotificationService` actualmente solo hace `log.info()` de las notificaciones. Ya tiene `spring-boot-starter-mail` en el `pom.xml` pero no está configurado ni implementado.

### Plan de Implementación

#### 12.1 Configurar Mail en `application.yml`

```yaml
spring:
  mail:
    host: smtp.gmail.com
    port: 587
    username: tu-email@gmail.com
    password: tu-app-password    # Usa App Password de Google, NO tu contraseña normal
    properties:
      mail:
        smtp:
          auth: true
          starttls:
            enable: true
          connectiontimeout: 5000
          timeout: 5000
          writetimeout: 5000

notification:
  mail:
    from: "DigitalBank <no-reply@digitalbank.com>"
    base-url: "https://tudominio.com"
```

#### 12.2 Crear Servicio de Email

```java
@Service
@RequiredArgsConstructor
@Slf4j
public class EmailService {

    private final JavaMailSender mailSender;
    private final TemplateEngine templateEngine; // Thymeleaf

    @Value("${notification.mail.from}")
    private String fromEmail;

    public void sendHtmlEmail(String to, String subject, String htmlContent) {
        try {
            MimeMessage message = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(message, true, "UTF-8");

            helper.setFrom(fromEmail);
            helper.setTo(to);
            helper.setSubject(subject);
            helper.setText(htmlContent, true);

            mailSender.send(message);
            log.info("Email sent to {} | Subject: {}", to, subject);
        } catch (Exception e) {
            log.error("Failed to send email to {}: {}", to, e.getMessage());
        }
    }
}
```

#### 12.3 Plantilla HTML Bancaria (Estilo MercadoLibre)

Crear el archivo `src/main/resources/templates/email/banking-notification.html`:

```html
<!DOCTYPE html>
<html xmlns:th="http://www.thymeleaf.org">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin:0;padding:0;background-color:#EEEEEE;font-family:'Helvetica Neue',Helvetica,Arial,sans-serif;">

<!-- Container -->
<table width="100%" cellpadding="0" cellspacing="0" style="background-color:#EEEEEE;padding:30px 0;">
    <tr>
        <td align="center">
            <table width="600" cellpadding="0" cellspacing="0" style="background-color:#FFFFFF;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08);">

                <!-- Header -->
                <tr>
                    <td style="background-color:#3483FA;padding:24px 32px;text-align:center;">
                        <table width="100%" cellpadding="0" cellspacing="0">
                            <tr>
                                <td style="color:#FFFFFF;font-size:24px;font-weight:700;letter-spacing:0.5px;">
                                    💳 DigitalBank
                                </td>
                            </tr>
                            <tr>
                                <td style="color:rgba(255,255,255,0.85);font-size:12px;padding-top:4px;letter-spacing:1px;text-transform:uppercase;">
                                    Notificación de tu cuenta
                                </td>
                            </tr>
                        </table>
                    </td>
                </tr>

                <!-- Alert Badge -->
                <tr>
                    <td style="padding:32px 32px 0;text-align:center;">
                        <div th:if="${alertType == 'DEBIT'}" style="display:inline-block;background-color:#FF6B6B;color:#FFFFFF;padding:8px 20px;border-radius:20px;font-size:12px;font-weight:700;letter-spacing:1px;text-transform:uppercase;">
                            🔻 Débito
                        </div>
                        <div th:if="${alertType == 'CREDIT'}" style="display:inline-block;background-color:#00A650;color:#FFFFFF;padding:8px 20px;border-radius:20px;font-size:12px;font-weight:700;letter-spacing:1px;text-transform:uppercase;">
                            🔺 Crédito
                        </div>
                        <div th:if="${alertType == 'FRAUD'}" style="display:inline-block;background-color:#FF4444;color:#FFFFFF;padding:8px 20px;border-radius:20px;font-size:12px;font-weight:700;letter-spacing:1px;text-transform:uppercase;">
                            🚨 Alerta de Seguridad
                        </div>
                        <div th:if="${alertType == 'OTP'}" style="display:inline-block;background-color:#FF9900;color:#FFFFFF;padding:8px 20px;border-radius:20px;font-size:12px;font-weight:700;letter-spacing:1px;text-transform:uppercase;">
                            🔐 Verificación
                        </div>
                        <div th:if="${alertType == 'REFUND'}" style="display:inline-block;background-color:#00A650;color:#FFFFFF;padding:8px 20px;border-radius:20px;font-size:12px;font-weight:700;letter-spacing:1px;text-transform:uppercase;">
                            💰 Reembolso
                        </div>
                        <div th:if="${alertType == 'PAYMENT_OK'}" style="display:inline-block;background-color:#00A650;color:#FFFFFF;padding:8px 20px;border-radius:20px;font-size:12px;font-weight:700;letter-spacing:1px;text-transform:uppercase;">
                            ✅ Pago Exitoso
                        </div>
                        <div th:if="${alertType == 'PAYMENT_FAIL'}" style="display:inline-block;background-color:#FF4444;color:#FFFFFF;padding:8px 20px;border-radius:20px;font-size:12px;font-weight:700;letter-spacing:1px;text-transform:uppercase;">
                            ❌ Pago Fallido
                        </div>
                    </td>
                </tr>

                <!-- Title -->
                <tr>
                    <td style="padding:20px 32px 0;text-align:center;">
                        <h1 style="margin:0;color:#333333;font-size:22px;font-weight:600;" th:text="${title}">
                            Título de la notificación
                        </h1>
                    </td>
                </tr>

                <!-- Amount (big) -->
                <tr th:if="${amount != null}">
                    <td style="padding:16px 32px 0;text-align:center;">
                        <div style="font-size:36px;font-weight:700;color:#333333;" th:text="${amount}">$1,500.00</div>
                        <div style="font-size:13px;color:#999999;padding-top:4px;" th:text="${currency}">MXN</div>
                    </td>
                </tr>

                <!-- Details Card -->
                <tr>
                    <td style="padding:24px 32px 0;">
                        <table width="100%" cellpadding="0" cellspacing="0" style="background-color:#F5F5F5;border-radius:8px;overflow:hidden;">
                            <!-- Account -->
                            <tr th:if="${accountNumber != null}">
                                <td style="padding:14px 20px;border-bottom:1px solid #E0E0E0;">
                                    <table width="100%" cellpadding="0" cellspacing="0">
                                        <tr>
                                            <td style="color:#666666;font-size:13px;font-weight:400;">Cuenta</td>
                                            <td align="right" style="color:#333333;font-size:13px;font-weight:600;font-family:monospace;" th:text="${accountNumber}">
                                                000123456789
                                            </td>
                                        </tr>
                                    </table>
                                </td>
                            </tr>
                            <!-- Sender/Receiver -->
                            <tr th:if="${senderAccount != null}">
                                <td style="padding:14px 20px;border-bottom:1px solid #E0E0E0;">
                                    <table width="100%" cellpadding="0" cellspacing="0">
                                        <tr>
                                            <td style="color:#666666;font-size:13px;">Cuenta Origen</td>
                                            <td align="right" style="color:#333333;font-size:13px;font-weight:600;font-family:monospace;" th:text="${senderAccount}">0001...</td>
                                        </tr>
                                    </table>
                                </td>
                            </tr>
                            <tr th:if="${receiverAccount != null}">
                                <td style="padding:14px 20px;border-bottom:1px solid #E0E0E0;">
                                    <table width="100%" cellpadding="0" cellspacing="0">
                                        <tr>
                                            <td style="color:#666666;font-size:13px;">Cuenta Destino</td>
                                            <td align="right" style="color:#333333;font-size:13px;font-weight:600;font-family:monospace;" th:text="${receiverAccount}">0009...</td>
                                        </tr>
                                    </table>
                                </td>
                            </tr>
                            <!-- Date -->
                            <tr>
                                <td style="padding:14px 20px;border-bottom:1px solid #E0E0E0;">
                                    <table width="100%" cellpadding="0" cellspacing="0">
                                        <tr>
                                            <td style="color:#666666;font-size:13px;">Fecha</td>
                                            <td align="right" style="color:#333333;font-size:13px;font-weight:600;" th:text="${date}">23/07/2026 10:30</td>
                                        </tr>
                                    </table>
                                </td>
                            </tr>
                            <!-- Reference -->
                            <tr th:if="${reference != null}">
                                <td style="padding:14px 20px;">
                                    <table width="100%" cellpadding="0" cellspacing="0">
                                        <tr>
                                            <td style="color:#666666;font-size:13px;">Referencia</td>
                                            <td align="right" style="color:#333333;font-size:13px;font-weight:600;font-family:monospace;" th:text="${reference}">TXN-001</td>
                                        </tr>
                                    </table>
                                </td>
                            </tr>
                        </table>
                    </td>
                </tr>

                <!-- OTP Section -->
                <tr th:if="${otp != null}">
                    <td style="padding:24px 32px 0;text-align:center;">
                        <div style="background-color:#FFF3CD;border:2px dashed #FF9900;border-radius:8px;padding:20px;">
                            <div style="color:#856404;font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:1px;margin-bottom:8px;">Tu código de verificación</div>
                            <div style="font-size:32px;font-weight:800;color:#333333;letter-spacing:8px;font-family:monospace;" th:text="${otp}">123456</div>
                            <div style="color:#856404;font-size:11px;margin-top:8px;">Válido por 5 minutos</div>
                        </div>
                    </td>
                </tr>

                <!-- Reason (for fraud/refund) -->
                <tr th:if="${reason != null}">
                    <td style="padding:16px 32px 0;">
                        <div style="background-color:#FFF3CD;border-left:4px solid #FF9900;padding:12px 16px;border-radius:0 4px 4px 0;">
                            <div style="color:#856404;font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px;">Motivo</div>
                            <div style="color:#856404;font-size:13px;padding-top:4px;" th:text="${reason}">Motivo de la alerta</div>
                        </div>
                    </td>
                </tr>

                <!-- Message -->
                <tr>
                    <td style="padding:20px 32px;">
                        <p style="margin:0;color:#666666;font-size:14px;line-height:1.6;" th:text="${message}">
                            Descripción del evento.
                        </p>
                    </td>
                </tr>

                <!-- CTA Button -->
                <tr th:if="${ctaUrl != null}">
                    <td style="padding:0 32px 24px;text-align:center;">
                        <a th:href="${ctaUrl}" style="display:inline-block;background-color:#3483FA;color:#FFFFFF;padding:14px 40px;border-radius:6px;text-decoration:none;font-size:14px;font-weight:600;letter-spacing:0.3px;">
                            Ver detalles
                        </a>
                    </td>
                </tr>

                <!-- Divider -->
                <tr>
                    <td style="padding:0 32px;">
                        <hr style="border:none;border-top:1px solid #E0E0E0;margin:0;">
                    </td>
                </tr>

                <!-- Footer -->
                <tr>
                    <td style="padding:20px 32px 24px;text-align:center;">
                        <p style="margin:0;color:#999999;font-size:11px;line-height:1.6;">
                            Este es un correo automático, por favor no responder.<br>
                            Si no reconoces esta actividad, contacta inmediatamente a soporte.
                        </p>
                        <p style="margin:12px 0 0;color:#BBBBBB;font-size:10px;">
                            © 2026 DigitalBank — Todos los derechos reservados
                        </p>
                    </td>
                </tr>

            </table>
        </td>
    </tr>
</table>

</body>
</html>
```

#### 12.4 Variables Thymeleaf por Tipo de Notificación

| Tipo | alertType | title | amount | extras |
|------|-----------|-------|--------|--------|
| Débito | `DEBIT` | Transacción realizada | `-$1,500.00` | senderAccount, receiverAccount |
| Crédito | `CREDIT` | Depósito recibido | `+$1,500.00` | senderAccount, receiverAccount |
| Fraude | `FRAUD` | ¡Tu cuenta fue bloqueada! | null | reason, accountNumber |
| OTP | `OTP` | Verificación de seguridad | `$500.00` | otp, reason |
| Reembolso | `REFUND` | Reembolso procesado | `+$500.00` | reason |
| Pago OK | `PAYMENT_OK` | Pago completado | `$500.00` | reference (razorpayId) |
| Pago Fallido | `PAYMENT_FAIL` | Pago no procesado | `$500.00` | reason |

#### 12.5 Integración en NotificationService

```java
@Service
@RequiredArgsConstructor
@Slf4j
public class NotificationService {

    private final EmailService emailService;
    private final AccountRepository accountRepository; // Para obtener email por accountNumber

    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(@Payload Map<String, Object> payload) {
        try {
            String senderAccount = (String) payload.get("senderAccountNumber");
            String receiverAccount = (String) payload.get("receiverAccountNumber");
            String amount = payload.get("amount").toString();

            // Email al emisor (débito)
            String senderEmail = accountRepository.findByAccountNumber(senderAccount)
                    .map(Account::getEmail).orElse(null);
            if (senderEmail != null) {
                Map<String, Object> vars = new HashMap<>();
                vars.put("alertType", "DEBIT");
                vars.put("title", "Transacción realizada");
                vars.put("amount", "-$" + amount);
                vars.put("senderAccount", senderAccount);
                vars.put("receiverAccount", receiverAccount);
                vars.put("date", LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm")));
                vars.put("message", "Se ha realizado una transferencia desde tu cuenta.");
                vars.put("ctaUrl", "https://tudominio.com/transactions");

                String html = emailService.processTemplate("email/banking-notification", vars);
                emailService.sendHtmlEmail(senderEmail, "💳 DigitalBank - Transacción realizada", html);
            }

            // Email al receptor (crédito)
            String receiverEmail = accountRepository.findByAccountNumber(receiverAccount)
                    .map(Account::getEmail).orElse(null);
            if (receiverEmail != null) {
                Map<String, Object> vars = new HashMap<>();
                vars.put("alertType", "CREDIT");
                vars.put("title", "Depósito recibido");
                vars.put("amount", "+$" + amount);
                vars.put("senderAccount", senderAccount);
                vars.put("receiverAccount", receiverAccount);
                vars.put("date", LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm")));
                vars.put("message", "Has recibido una transferencia en tu cuenta.");
                vars.put("ctaUrl", "https://tudominio.com/transactions");

                String html = emailService.processTemplate("email/banking-notification", vars);
                emailService.sendHtmlEmail(receiverEmail, "💳 DigitalBank - Depósito recibido", html);
            }
        } catch (Exception e) {
            log.error("Error: {}", e.getMessage());
        }
    }
}
```

#### 12.6 Agregar Thymeleaf al pom.xml

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-thymeleaf</artifactId>
</dependency>
```

#### 12.7 Testing con MailHog (Local)

Para probar envío de correos sin configurar Gmail:

```yaml
# Añadir al docker-compose.yml
  mailhog:
    image: mailhog/mailhog:latest
    container_name: mailhog
    ports:
      - "1025:1025"   # SMTP
      - "8025:8025"   # Web UI
    networks:
      - banking-network
```

```yaml
# Configurar en notification-service application.yml
spring:
  mail:
    host: localhost
    port: 1025
```

Accede a la interfaz web en `http://localhost:8025` para ver todos los correos enviados.

---

## 13. Consideraciones para Dockerizar los Microservicios

### 13.1 Dockerfile Base (para cada microservicio)

```dockerfile
# Stage 1: Build
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -B
COPY src ./src
RUN mvn package -DskipTests -B

# Stage 2: Run
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
```

### 13.2 docker-compose.yml Actualizado (con microservicios)

```yaml
version: '3.8'

services:

  # ==================== INFRAESTRUCTURA ====================

  redis:
    image: redis:latest
    container_name: redis
    ports:
      - "6379:6379"
    networks:
      - banking-network
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5

  mysql:
    image: mysql:8.0
    container_name: mysql
    environment:
      MYSQL_ROOT_PASSWORD: root
      MYSQL_DATABASE: account_db
      MYSQL_USER: banking
      MYSQL_PASSWORD: banking123
    ports:
      - "3306:3306"
    volumes:
      - mysql-data:/var/lib/mysql
    networks:
      - banking-network
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost", "-uroot", "-proot"]
      interval: 10s
      timeout: 5s
      retries: 10

  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    networks:
      - banking-network

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    container_name: kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    networks:
      - banking-network
    healthcheck:
      test: ["CMD", "kafka-broker-api-versions", "--bootstrap-server", "localhost:9092"]
      interval: 10s
      timeout: 10s
      retries: 10

  mailhog:
    image: mailhog/mailhog:latest
    container_name: mailhog
    ports:
      - "1025:1025"
      - "8025:8025"
    networks:
      - banking-network

  # ==================== MICROSERVICIOS ====================

  account-service:
    build: ./account-service
    container_name: account-service
    ports:
      - "8081:8081"
    environment:
      SPRING_DATASOURCE_URL: jdbc:mysql://mysql:3306/account_db?createDatabaseIfNotExist=true
      SPRING_DATASOURCE_USERNAME: root
      SPRING_DATASOURCE_PASSWORD: root
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092
    depends_on:
      mysql:
        condition: service_healthy
      kafka:
        condition: service_healthy
      redis:
        condition: service_healthy
    networks:
      - banking-network

  transaction-service:
    build: ./transaction-service
    container_name: transaction-service
    ports:
      - "8082:8082"
    environment:
      SPRING_DATASOURCE_URL: jdbc:mysql://mysql:3306/transaction_db?createDatabaseIfNotExist=true
      SPRING_DATASOURCE_USERNAME: root
      SPRING_DATASOURCE_PASSWORD: root
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      SPRING_DATA_REDIS_HOST: redis
      ACCOUNT_SERVICE_URL: http://account-service:8081
    depends_on:
      mysql:
        condition: service_healthy
      kafka:
        condition: service_healthy
      redis:
        condition: service_healthy
      account-service:
        condition: service_started
    networks:
      - banking-network

  payment-service:
    build: ./payment-service
    container_name: payment-service
    ports:
      - "8083:8083"
    environment:
      SPRING_DATASOURCE_URL: jdbc:mysql://mysql:3306/payment_db?createDatabaseIfNotExist=true
      SPRING_DATASOURCE_USERNAME: root
      SPRING_DATASOURCE_PASSWORD: root
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092
    depends_on:
      mysql:
        condition: service_healthy
      kafka:
        condition: service_healthy
    networks:
      - banking-network

  fraud-detection-service:
    build: ./fraud-detection-service
    container_name: fraud-detection-service
    ports:
      - "8084:8084"
    environment:
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      SPRING_DATA_REDIS_HOST: redis
      ACCOUNT_SERVICE_URL: http://account-service:8081
    depends_on:
      kafka:
        condition: service_healthy
      redis:
        condition: service_healthy
      account-service:
        condition: service_started
    networks:
      - banking-network

  notification-service:
    build: ./notification-service
    container_name: notification-service
    ports:
      - "8085:8085"
    environment:
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      SPRING_MAIL_HOST: mailhog
      SPRING_MAIL_PORT: 1025
    depends_on:
      kafka:
        condition: service_healthy
      mailhog:
        condition: service_started
    networks:
      - banking-network

  api-gateway:
    build: ./api-gateway
    container_name: api-gateway
    ports:
      - "8080:8080"
    environment:
      SPRING_DATA_REDIS_HOST: redis
    depends_on:
      redis:
        condition: service_healthy
      account-service:
        condition: service_started
      transaction-service:
        condition: service_started
      payment-service:
        condition: service_started
    networks:
      - banking-network

volumes:
  mysql-data:

networks:
  banking-network:
    driver: bridge
```

### 13.3 Cambios Necesarios en application.yml para Docker

Cada microservicio necesita un `application-docker.yml` o usar variables de entorno:

**account-service/src/main/resources/application.yml** — agregar perfiles:
```yaml
---
spring:
  config:
    activate:
      on-profile: docker
  datasource:
    url: jdbc:mysql://mysql:3306/account_db?createDatabaseIfNotExist=true
  kafka:
    bootstrap-servers: kafka:29092
```

**transaction-service** — para Docker, el Feign client URL debe ser:
```yaml
account:
  service:
    url: http://account-service:8081  # nombre del contenedor Docker
```

**fraud-detection-service** — similar:
```yaml
account:
  service:
    url: http://account-service:8081
spring:
  data:
    redis:
      host: redis
  kafka:
    bootstrap-servers: kafka:29092
```

### 13.4 Problemas Comunes y Soluciones

| Problema | Causa | Solución |
|----------|-------|---------|
| `Connection refused` a MySQL | MySQL no está listo | Usar `healthcheck` + `depends_on: condition: service_healthy` |
| `Connection refused` a Kafka | Kafka tarda en arrancar | Esperar 30s o usar healthcheck |
| Feign no resuelve `localhost` | En Docker, localhost = contenedor actual | Usar nombre del servicio: `http://account-service:8081` |
| Kafka topics no se crean | Auto-create puede tardar | Verificar con `kafka-topics --list` antes de iniciar servicios |
| Redis connection refused | Host incorrecto | En Docker usar `redis`, fuera usar `localhost` |

### 13.5 Build y Levantar Todo con Docker

```bash
# Build y levantar todo
docker compose up -d --build

# Ver logs en tiempo real
docker compose logs -f

# Ver logs de un servicio específico
docker compose logs -f fraud-detection-service

# Detener todo
docker compose down

# Limpiar todo (incluyendo datos)
docker compose down -v
```

---

## 14. Troubleshooting

### Kafka no conecta

```bash
# Verificar que Kafka está corriendo
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Reiniciar Kafka si es necesario
docker compose restart kafka zookeeper
```

### MySQL no crea las bases de datos

```bash
# Verificar bases de datos
docker exec mysql mysql -uroot -proot -e "SHOW DATABASES;"

# Crear manualmente si es necesario
docker exec mysql mysql -uroot -proot -e "CREATE DATABASE IF NOT EXISTS account_db;"
docker exec mysql mysql -uroot -proot -e "CREATE DATABASE IF NOT EXISTS transaction_db;"
docker exec mysql mysql -uroot -proot -e "CREATE DATABASE IF NOT EXISTS payment_db;"
```

### OTP no aparece en logs

```bash
# Verificar que Redis tiene el OTP
docker exec redis redis-cli KEYS "verification:otp*"

# Verificar que Fraud Detection está procesando
docker compose logs fraud-detection-service | grep -i "fraud"
```

### Transacción queda en PROCESSING forever

Causa común: Fraud Detection no está corriendo o no puede conectar a Kafka.

```bash
# Verificar logs de Fraud Detection
docker compose logs fraud-detection-service

# Verificar que el topic transaction.initiated tiene mensajes
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic transaction.initiated --from-beginning --max-messages 1
```

### Limpiar Redis para re-testear fraud

```bash
# Eliminar todos los patrones de fraude
docker exec redis redis-cli FLUSHDB
```

---

## Referencia Rápida de Puertos

| Puerto | Servicio |
|--------|----------|
| 6379 | Redis |
| 3306 | MySQL |
| 9092 | Kafka |
| 8025 | MailHog Web UI |
| 8080 | API Gateway |
| 8081 | Account Service |
| 8082 | Transaction Service |
| 8083 | Payment Service |
| 8084 | Fraud Detection Service |
| 8085 | Notification Service |
