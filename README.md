# Offline Mesh Payment Routing and Settlement System

This project is a Spring Boot simulation exploring how digital payments can work in offline environments with zero cellular or internet connectivity. It models an ad-hoc device mesh where encrypted payment packets hop between nearby devices over simulated Bluetooth Low Energy (BLE) gossip until an internet-connected bridge node uploads them to a backend settlement service.

The backend handles duplicate suppression, payload decryption, replay verification, optimistic concurrency control, and double-entry ledger settlement.

> Note: This is an educational prototype built for my backend portfolio to explore distributed systems patterns (delay-tolerant messaging, idempotency, hybrid cryptography, and transactional accounting). It is not an official UPI implementation and does not connect to NPCI or live banking networks.

---

## Features

- **Hybrid encryption (RSA-OAEP + AES-256-GCM)**: Payment instructions are encrypted on the sender's device using a one-time AES-256 key in GCM mode. The AES key is encrypted using the backend's RSA-2048 public key. Intermediate mesh devices forward packets without being able to read or modify transaction details.
- **Mesh gossip simulation**: Simulates offline phones and internet-connected bridge devices. Packets hop between devices with a decremented Time-To-Live (TTL) counter to prevent infinite broadcast loops.
- **Idempotency and duplicate suppression**:
  - In-memory gate using `ConcurrentHashMap.putIfAbsent()` for fast duplicate drops during concurrent bridge uploads.
  - Database-level unique constraint on `packetHash` in MySQL (`idx_packet_hash`) as durable defense-in-depth.
- **Double-entry ledger**: Every settled payment writes balanced `DEBIT` and `CREDIT` entries into an immutable ledger table within a single `@Transactional` boundary.
- **Optimistic concurrency control**: Uses JPA `@Version` on account entities to prevent lost balance updates under concurrent requests, backed by bounded retries with randomized backoff.
- **Replay protection and validation**: Rejects stale packets older than a configurable maximum age (`upi.mesh.packet-max-age-seconds`). Validates request inputs including positive amounts, valid VPAs, and 4-6 digit PIN formats.
- **Audit tracking for rejections**: Failed payments (insufficient balance or self-transfer) are recorded in the database with status `REJECTED` and an explicit rejection reason.
- **Interactive dashboard**: A lightweight web UI (Thymeleaf + JavaScript) to inspect virtual devices, run gossip rounds, trigger bridge flushes, and view live balances and transactions.

---

## Tech Stack

- **Backend**: Java 17, Spring Boot 3.3.5 (Spring MVC, Spring Data JPA, Bean Validation)
- **Database**: MySQL 8.0+ (runtime persistence), H2 (in-memory for automated tests)
- **ORM**: Hibernate 6.5
- **Cryptography**: Java Cryptography Architecture (RSA-2048, AES-256-GCM, SHA-256)
- **Frontend**: Thymeleaf, HTML5, CSS, Vanilla JavaScript
- **Build & Test**: Maven 3.9+, JUnit 5, AssertJ

---

## How the Payment Flow Works

### Flow Breakdown

1. **Packet creation**: The sender enters the payment amount and PIN. A `PaymentInstruction` is constructed with a unique UUID nonce, a signed timestamp, and a SHA-256 hash of the PIN.
2. **Encryption**: The payload is serialized to JSON, encrypted using a fresh AES-256-GCM key, and the AES key is encrypted using the server's RSA public key.
3. **Mesh gossip**: The packet enters the virtual mesh. Each round of gossip forwards the packet to neighboring devices, reducing TTL by 1. Packets with `ttl <= 0` are not forwarded further.
4. **Bridge upload**: When an online device (`phone-bridge`) receives the packet, it POSTs it to `/api/bridge/ingest`.
5. **Ingestion & settlement**:
   - The server computes the SHA-256 hash of the raw ciphertext. This hash serves as the idempotency key.
   - `IdempotencyService` checks if the hash has already been seen. If so, it immediately drops the duplicate.
   - The server decrypts the payload and checks that the packet was signed within the allowed time window.
   - `SettlementService` executes the balance transfer, writes balanced debit and credit entries to the ledger, and records the transaction.
   - If a concurrent write conflict occurs on the account, Spring Data JPA triggers an optimistic lock retry.

---

## Project Structure

```text
OfflinePaymentSystem/
├── pom.xml
├── mvnw / mvnw.cmd
├── src/
│   ├── main/
│   │   ├── java/com/demo/upimesh/
│   │   │   ├── UpiMeshApplication.java          # Application entry point
│   │   │   ├── config/AppConfig.java            # Scheduling config
│   │   │   ├── controller/
│   │   │   │   ├── ApiController.java           # REST endpoints
│   │   │   │   └── DashboardController.java     # Web view controller
│   │   │   ├── crypto/
│   │   │   │   ├── HybridCryptoService.java     # AES-GCM + RSA-OAEP logic
│   │   │   │   └── ServerKeyHolder.java         # RSA key management
│   │   │   ├── model/
│   │   │   │   ├── Account.java                 # Account entity (@Version)
│   │   │   │   ├── LedgerEntry.java             # Immutable double-entry records
│   │   │   │   ├── MeshPacket.java              # Wire format DTO
│   │   │   │   ├── PaymentInstruction.java      # Decrypted payment payload
│   │   │   │   └── Transaction.java             # Settlement transaction record
│   │   │   └── service/
│   │   │       ├── BridgeIngestionService.java  # Deduplication and retry pipeline
│   │   │       ├── DemoService.java             # Account seeding and packet builder
│   │   │       ├── IdempotencyService.java      # ConcurrentHashMap cache
│   │   │       ├── MeshSimulatorService.java    # Hop simulation and bridge collector
│   │   │       ├── SettlementService.java       # @Transactional balance updates
│   │   │       └── VirtualDevice.java           # In-memory device packet store
│   │   └── resources/
│   │       ├── application.properties           # Database and application properties
│   │       └── templates/dashboard.html         # Live demo web dashboard
│   └── test/java/com/demo/upimesh/
│       ├── IdempotencyConcurrencyTest.java      # Concurrency and settlement test suite
│       └── MeshSimulatorServiceTest.java        # Mesh gossip and forwarding unit tests
└── README.md
```

---

## Prerequisites and Setup

### Prerequisites

- Java 17 or higher
- MySQL 8.0 or higher running locally on port 3306
- Maven 3.8+ (or use the included `mvnw.cmd`)

### Database Setup

Create the database in MySQL:

```sql
CREATE DATABASE IF NOT EXISTS upimesh;
```

By default, the application connects to `localhost:3306/upimesh` with user `root` and an empty password.

### Environment Variables

You can override database settings using environment variables:

| Variable | Default | Description |
| :--- | :--- | :--- |
| `SPRING_DATASOURCE_URL` or `MYSQL_URL` | `jdbc:mysql://localhost:3306/upimesh?...` | JDBC connection URL |
| `SPRING_DATASOURCE_USERNAME` or `MYSQL_USER` | `root` | Database username |
| `SPRING_DATASOURCE_PASSWORD` or `MYSQL_PASSWORD` | *(empty)* | Database password |
| `MYSQL_HOST` | `localhost` | Database host |
| `MYSQL_PORT` | `3306` | Database port |
| `MYSQL_DATABASE` | `upimesh` | Database name |

---

## Running the Application and Tests

### Run on Windows (PowerShell)

Set your database password if needed, then start the server:

```powershell
$env:MYSQL_PASSWORD="your_password"
.\mvnw.cmd spring-boot:run
```

Or using system Maven:

```powershell
mvn spring-boot:run
```

Open `http://localhost:8080` in your browser to access the dashboard.

The application automatically seeds four test accounts on first startup:
- `alice@demo`: ₹5,000.00
- `bob@demo`: ₹1,000.00
- `carol@demo`: ₹2,500.00
- `dave@demo`: ₹500.00

### Run Tests

Tests run against an in-memory H2 database, so you do not need MySQL running to execute tests:

```powershell
mvn clean test
```

To run the full Maven verification:

```powershell
mvn clean verify
```

The test suite contains 26 automated tests covering:
- `MeshSimulatorServiceTest` (10 tests): Packet injection, gossip hop forwarding, TTL decrementing, duplicate suppression on nodes, bridge collection, and mesh reset.
- `IdempotencyConcurrencyTest` (16 tests): Concurrent bridge duplicate storm, tamper rejection, stale packet rejection, insufficient balance rejection, self-transfer rejection, optimistic locking retries, double-entry ledger verification, state reset, and pre-reset packet invalidation.

---

## API Endpoints

### Core Endpoints

#### 1. Fetch Server Public Key
- **`GET /api/server-key`**
- Returns the RSA-2048 public key used by devices to encrypt payment instructions.

#### 2. Create and Inject Payment (Demo Helper)
- **`POST /api/demo/send`**
- Simulates a sender device composing a payment, hybrid-encrypting it, and injecting it into the mesh.
- Request body:
  ```json
  {
    "senderVpa": "alice@demo",
    "receiverVpa": "bob@demo",
    "amount": 250.00,
    "pin": "1234",
    "ttl": 5,
    "startDevice": "phone-alice"
  }
  ```
- Validation rules: `senderVpa` and `receiverVpa` required; `amount` must be greater than zero; `pin` must be 4 to 6 numeric digits.

#### 3. Reset Demo State
- **`POST /api/demo/reset`**
- Transactionally restores all demo accounts to initial balances, deletes all transactions and ledger entries, clears virtual device packet queues, resets the idempotency cache, and prevents pre-reset packets from settling.
- Response format:
  ```json
  {
    "status": "SUCCESS",
    "message": "State reset successfully: accounts restored, transactions and ledger cleared, mesh cleared"
  }
  ```

#### 4. Ingest Packet from Bridge
- **`POST /api/bridge/ingest`**
- The endpoint called when an internet-connected bridge node uploads held packets to the backend.
- Headers:
  - `X-Bridge-Node-Id` (optional, e.g. `phone-bridge`)
  - `X-Hop-Count` (optional, e.g. `2`)
- Request body (`MeshPacket`):
  ```json
  {
    "packetId": "8f2a1b7c-3d4e-4f5a-b6c7-d8e9f0a1b2c3",
    "ttl": 3,
    "createdAt": 1729060000000,
    "ciphertext": "BAgAEhUA+...base64-ciphertext..."
  }
  ```
- Response format:
  ```json
  {
    "outcome": "SETTLED",
    "packetHash": "4b5a47ae68cf...",
    "reason": null,
    "transactionId": 1
  }
  ```
- Possible `outcome` values: `SETTLED`, `REJECTED`, `DUPLICATE_DROPPED`, `INVALID`.

### Simulator & Inspection Endpoints

| Endpoint | Method | Purpose |
| :--- | :--- | :--- |
| `/api/mesh/state` | `GET` | Returns all virtual devices, connectivity status, and held packets. |
| `/api/mesh/gossip` | `POST` | Executes one round of mesh gossip across virtual devices. |
| `/api/mesh/flush` | `POST` | Collects packets from online bridge devices and uploads them in parallel. |
| `/api/mesh/reset` | `POST` | Clears all held packets in devices and resets the idempotency cache. |
| `/api/demo/reset` | `POST` | Full system state reset: restores accounts, empties DB records, clears mesh. |
| `/api/accounts` | `GET` | Returns all accounts and current balances. |
| `/api/transactions` | `GET` | Returns recent settlement transaction records. |

---

## Limitations

- **Educational prototype**: Designed to demonstrate backend patterns; does not integrate with real UPI switches or NPCI infrastructure.
- **PIN authentication**: The PIN format is validated (4–6 digits) and hashed into `pinHash`, but the backend does not verify it against a bank-stored secret.
- **Offline double-spending**: Disconnected mesh devices cannot check balances in real time. If a sender creates multiple conflicting payments while offline, all will gossip through the mesh; the conflict is only caught when the transactions hit the backend settlement service.
- **In-memory mesh state**: Virtual devices and held packets live in JVM memory. Restarting the server clears the mesh and generates a new RSA keypair.
- **Simplified network topology**: Gossip is simulated through complete device rounds rather than modeling Bluetooth radio range, signal strength, or real physical peer discovery.

---

## Future Improvements

- **Asymmetric sender signatures**: Use Ed25519 client-side keypairs so the backend can verify the sender's identity cryptographically.
- **Hardware-backed balance reservations**: Use device secure enclaves (e.g. Android Keystore) to lock offline balances locally and mitigate double-spending.
- **Persistent key management**: Store the server private key in a Hardware Security Module (HSM) or KMS rather than generating an ephemeral key on startup.
- **Distributed caching**: Replace the in-memory `ConcurrentHashMap` with Redis (`SETNX` + TTL) for horizontally scaled backend instances.
- **Reverse receipt propagation**: Implement mesh gossip back to the sender device to deliver settlement acknowledgments.

---

## License and Attribution

This project is licensed under the MIT License.

Original repository and project tracking:
[Offline-Mesh-Payment-Routing-and-Settlement-System](https://github.com/ManjushFarad/Offline-Mesh-Payment-Routing-and-Settlement-System)
