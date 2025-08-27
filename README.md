# go-redis

A Redis-compatible key-value database implemented in Golang.

## Features

- **Data Structures**:

  - String
  - Set
  - List
  - Hash
  - Stream
  - Sorted Set

- **Redis Serialization Protocol (RESP)**:  
   Implements RESP for client-server communication.

- **Persistence**:  
   Snapshot-based persistence mechanism for data durability.

- **Replication**:  
   Supports master-slave replication.

- **Pub/Sub**:  
   Publish/Subscribe messaging pattern.

- **Transactions**:  
   Supports atomic execution of multiple commands.

## Getting Started

1. **Clone the repository**

   ```bash
   git clone https://github.com/yourusername/go-redis.git
   cd go-redis
   ```

2. **Build**

   ```bash
   go build
   ```

3. **Run**
   ```bash
   ./go-redis
   ```

## Usage

Connect using any Redis client. Supported commands include operations for all implemented data structures and transactions.

## Contributing

Contributions are welcome! Please open issues or submit pull requests.
