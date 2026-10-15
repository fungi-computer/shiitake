import * as Schema from "effect/Schema";

import type { Json } from "../extensions.js";

/** Any finite JSON value; an extension's own schema decodes it further. */
export const JsonValueSchema: Schema.Codec<Json> = Schema.Json;
