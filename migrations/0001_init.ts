import * as Sql from "@effect/sql"
import { Effect } from "effect"

export default Effect.flatMap(
  Sql.client.Client,
  (sql) => sql``
)