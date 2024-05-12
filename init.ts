import { EventStoreDBClient, jsonEvent } from '@eventstore/db-client';

const client = EventStoreDBClient.connectionString`esdb+discover://localhost:2113?tls=false&keepAliveTimeout=10&keepAliveInterval=10000`;

await client.appendToStream('test-stream', jsonEvent({ type: 'test', data: {} }));

client.dispose();

// idk, I can't get it to exit normally lol
process.exit(0);
