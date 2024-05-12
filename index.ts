import { BunContext, BunRuntime } from '@effect/platform-bun';
import { Schema } from '@effect/schema';
import * as Sql from '@effect/sql';
import * as Pg from '@effect/sql-pg';
import { EventStoreDBClient, type ReadStreamOptions } from '@eventstore/db-client';
import { fileURLToPath } from 'bun';
import { Config, Context, Data, Effect, Layer, Secret, Stream } from 'effect';

export class EventDBError extends Data.TaggedError('EventDBError')<{
	message: string;
}> {}

const getClient = (url: string) => {
	const connect = Effect.try({
		try: () => EventStoreDBClient.connectionString`esdb+discover://${url}?tls=false&keepAliveTimeout=10000&keepAliveInterval=10000`,
		catch: (e) => new EventDBError({ message: `Failed to connect to EventStoreDB: ${e}` })
	}).pipe(Effect.tap(Effect.log('Connected to EventStoreDB')));

	const disconnect = (client: EventStoreDBClient) =>
		Effect.tryPromise({
			try: () => client.dispose(),
			catch: (e) => new EventDBError({ message: `Failed to disconnect from EventStoreDB: ${e}` })
		}).pipe(
			Effect.catchTag('EventDBError', (e) => Effect.log(`Failed to disconnect from EventStoreDB: ${e}`)),
			Effect.tap(Effect.log('Disconnected from EventStoreDB'))
		);

	return Effect.acquireRelease(connect, disconnect);
};

const getReaderResource = (client: EventStoreDBClient, streamName: string, opts?: ReadStreamOptions) => {
	const subscribe = Effect.try({
		try: () => client.readStream(streamName, opts),
		catch: (e) => new EventDBError({ message: `Failed to subscribe to stream: ${e}` })
	}).pipe(Effect.tap(Effect.log(`Reading stream: ${streamName}`)));

	return subscribe.pipe(
		Stream.flatMap((sub) => Stream.fromAsyncIterable(sub, (e) => new EventDBError({ message: `Failed to read from stream: ${e}` })))
	);
};

const makeEventDB = (url: string) =>
	Effect.gen(function* () {
		const client = yield* getClient(url);

		const readStream = (streamName: string, opts?: ReadStreamOptions) => getReaderResource(client, streamName, opts);

		const reader =
			<S extends Schema.Schema.AnyNoContext>(schema: S) =>
			(streamName: string, opts?: ReadStreamOptions) => {
				const decode = Schema.decode(schema);

				return readStream(streamName, opts).pipe(Stream.flatMap(decode));
			};

		return {
			readStream,
			reader
		};
	});

class EventDB extends Context.Tag('EventDB')<EventDB, Effect.Effect.Success<ReturnType<typeof makeEventDB>>>() {
	static Live(url: string) {
		return Layer.effect(EventDB, makeEventDB(url));
	}
}

const program = Effect.gen(function* () {
	const eventDB = yield* EventDB;

	yield* Effect.log('Reading events from test-stream');

	const reader = eventDB.reader(Schema.Struct({}))('test-stream');

	yield* Effect.log('Subscribed to test-stream');

	yield* Stream.runForEach(reader, (event) => Effect.log(event));
});

const EventDBLive = EventDB.Live('localhost:2113');

const SqlLive = Pg.client.layer({
	host: Config.succeed('localhost'),
	port: Config.succeed(5432),
	database: Config.succeed('postgres'),
	username: Config.succeed('postgres'),
	password: Config.succeed(Secret.fromString('postgres')),
});
const MigratorLive = Pg.migrator.layer({
	loader: Sql.migrator.fromFileSystem(fileURLToPath(new URL('migrations', import.meta.url))),
	schemaDirectory: './migrations'
}).pipe(Layer.provide(SqlLive));
const DBLive = Layer.mergeAll(SqlLive, MigratorLive).pipe(Layer.provide(BunContext.layer));

program.pipe(Effect.provide(EventDBLive), Effect.provide(DBLive), Effect.scoped, BunRuntime.runMain);
