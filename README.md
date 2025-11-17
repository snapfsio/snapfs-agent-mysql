# SnapFS MySQL Agent

The SnapFS MySQL Agent consumes file events from the SnapFS Gateway and
writes them into a MySQL database.

Agents connect to the gateway via WebSocket:

```
ws://gateway/stream?subject=snapfs.files&durable=mysql&batch=100
```

The gateway acts as a bridge to NATS JetStream. Agents never speak NATS directly.

## Responsibilities

- Receive event batches from `/stream`
- Apply `file.upsert` events to MySQL
- ACK batches (which ACKs JetStream messages)
- Reconnect automatically with exponential backoff

## Requirements

- SnapFS Gateway running with NATS JetStream enabled
- MySQL 8.x or MariaDB 10.5+
- Python 3.8+

## Running

```bash
snapfs-agent-mysql
```

Configure via environment variables:

```
MYSQL_URL=mysql+pymysql://user:pass@mysql:3306/snapfs
GATEWAY_WS=ws://gateway:8000
SNAPFS_SUBJECT=snapfs.files
SNAPFS_DURABLE=mysql
SNAPFS_BATCH=100
```

## License

Apache 2.0
