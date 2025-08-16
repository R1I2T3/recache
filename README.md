# go-redis (Redis Clone)

This project is an attempt to replicate the core functionality of Redis using Go. It does **not** use the official Redis server or client libraries. The goal is to build a simple in-memory key-value store inspired by Redis, focusing on learning and understanding distributed systems concepts.

## Features

- In-memory key-value storage
- Basic Redis-like commands (e.g., `SET`, `GET`, `DEL`)
- Simple TCP server for client connections
- Extensible architecture for adding more commands

## Getting Started

### Prerequisites

- Go 1.18 or higher

### Installation

```bash
git clone https://github.com/r1i2t3/go-redis.git
cd go-redis
go build
```

### Usage

```bash
./go-redis
```
