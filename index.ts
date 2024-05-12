import { Schema } from '@effect/schema';
import { EventStoreDBClient, type EventType, type ReadStreamOptions, type ResolvedEvent } from '@eventstore/db-client';
import { Context, Data, Effect, Layer, Stream } from 'effect';

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
	const FakeEventStream = async function* () {
		while (true) {
			yield {} as ResolvedEvent<EventType>;
		}
	};
	const fakeSubscribe = Effect.sync(FakeEventStream);

	const realSubscribe = Effect.try({
		try: () => client.readStream(streamName, opts),
		catch: (e) => new EventDBError({ message: `Failed to subscribe to stream: ${e}` })
	});

	const subscribe = realSubscribe.pipe(Effect.tap(Effect.log(`Reading stream: ${streamName}`)));

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

	const reader = eventDB.reader(Schema.Struct({}))('test-stream');

	yield* Stream.runForEach(reader, (event) => Effect.log(event));
});

program.pipe(Effect.provide(EventDB.Live('localhost:2113')), Effect.scoped, Effect.runPromise);
