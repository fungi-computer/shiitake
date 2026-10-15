import * as Struct from "effect/Struct";
import type {
  EmptyExtensionFieldsV0,
  AnyCollectionSpecV0,
  CollectionSpecV0,
  CollectionsV0,
  CollectionV0,
  ExtensionContextV0,
  Json,
  StateHandleV0,
  UpdateCollectionV0,
  ReadStateV0,
  UpdateStateV0,
} from "../extensions.js";
import type {
  DocumentsV0,
  DocumentSpecV0,
  DocumentHandleV0,
  DocumentHandlesV0,
} from "../session-documents.js";
import { invokeSyncHandler } from "./extension-hook.js";

/** Core retains only these erased results; the author retains each concrete factory. */
export type CollectionBindingV0 = Readonly<{
  read: Omit<CollectionV0<unknown>, "put"> & Pick<CollectionV0<never>, "put">;
  update: Omit<UpdateCollectionV0<unknown>, "put"> &
    Pick<UpdateCollectionV0<never>, "put">;
}>;
/** State operations capture their typed declaration before entering core. */
export type ExtensionRuntimePortV0 = Readonly<{
  context: Omit<
    ExtensionContextV0<EmptyExtensionFieldsV0, Json, EmptyExtensionFieldsV0>,
    "config" | "state" | "documents"
  >;
  /** Build read/write operations for the declaring collection and schema. */
  collection: <A, I>(
    name: string,
    spec: CollectionSpecV0<A, I>,
  ) => CollectionV0<A>;
  /** Build synchronous operations guarded by the owner's state transaction. */
  updateCollection: <A, I>(
    name: string,
    spec: CollectionSpecV0<A, I>,
  ) => UpdateCollectionV0<A>;
  /** Build a Session document handle using its own codec and byte bound. */
  document: <A, I>(
    name: string,
    spec: DocumentSpecV0<A, I>,
  ) => DocumentHandleV0<A>;
  /** Execute a synchronous mutation in the declaring extension's transaction. */
  update: <A>(body: () => A) => A;
  /** Publish a Session only after its state mutation commits. */
  observeUpdate: (sessionId: string) => void;
}>;

// TypeScript permits polymorphic `this` in classes; these type-level lambdas emit no classes.
declare class NamedFactoryMapping implements Struct.Lambda {
  readonly "~lambda.in": unknown;
  readonly "~lambda.out": this["~lambda.in"] extends (name: string) => object
    ? ReturnType<this["~lambda.in"]>
    : never;
}
type NamedFactory = NamedFactoryMapping &
  (<A extends object>(factory: (name: string) => A) => A);
function invokeNamedFactories<
  F extends Readonly<Record<string, (name: string) => object>>,
>(factories: F): { readonly [K in keyof F]: ReturnType<F[K]> } {
  const names = new WeakMap<object, string>();
  for (const [name, factory] of Object.entries(factories))
    names.set(factory, name);
  return Struct.map(
    factories,
    Struct.lambda<NamedFactory>((factory) => {
      const name = names.get(factory);
      if (name === undefined)
        throw new Error("Extension handle factory is not registered");
      return factory(name);
    }),
  );
}
declare class CollectionFactoryMapping implements Struct.Lambda {
  readonly "~lambda.in": unknown;
  readonly "~lambda.out": this["~lambda.in"] extends AnyCollectionSpecV0
    ? (name: string) => ReturnType<this["~lambda.in"]["bind"]>
    : never;
}
type CollectionFactory = CollectionFactoryMapping &
  (<S extends AnyCollectionSpecV0>(
    spec: S,
  ) => (name: string) => ReturnType<S["bind"]>);
declare class ReadHandleMapping implements Struct.Lambda {
  readonly "~lambda.in": unknown;
  readonly "~lambda.out": this["~lambda.in"] extends CollectionBindingV0
    ? this["~lambda.in"]["read"]
    : never;
}
type ReadHandle = ReadHandleMapping &
  (<B extends CollectionBindingV0>(binding: B) => B["read"]);
declare class UpdateHandleMapping implements Struct.Lambda {
  readonly "~lambda.in": unknown;
  readonly "~lambda.out": this["~lambda.in"] extends CollectionBindingV0
    ? this["~lambda.in"]["update"]
    : never;
}
declare class ReadOnlyHandleMapping implements Struct.Lambda {
  readonly "~lambda.in": unknown;
  readonly "~lambda.out": this["~lambda.in"] extends CollectionBindingV0
    ? Pick<this["~lambda.in"]["update"], "get" | "page">
    : never;
}
type ReadOnlyHandle = ReadOnlyHandleMapping &
  (<B extends CollectionBindingV0>(
    binding: B,
  ) => Pick<B["update"], "get" | "page">);
type UpdateHandle = UpdateHandleMapping &
  (<B extends CollectionBindingV0>(binding: B) => B["update"]);
/** Each collection factory is new even when two names share the same declaration. */
export function bindExtensionState<S extends CollectionsV0>(
  collections: S,
  port: ExtensionRuntimePortV0,
): StateHandleV0<S> {
  const factories = Struct.map(
    collections,
    Struct.lambda<CollectionFactory>((spec) => (name) => spec.bind(port, name)),
  );
  const bindings = invokeNamedFactories(factories);
  const state = Struct.map(
    bindings,
    Struct.lambda<ReadHandle>((binding) => binding.read),
  );
  const updateState: UpdateStateV0<S> = Object.freeze({
    ...Struct.map(
      bindings,
      Struct.lambda<UpdateHandle>((binding) => binding.update),
    ),
    observe: port.observeUpdate,
  });
  // A separate frozen view: `read` must not be able to write.
  const readState: ReadStateV0<S> = Object.freeze(
    Struct.map(
      bindings,
      Struct.lambda<ReadOnlyHandle>((binding) =>
        Object.freeze({ get: binding.update.get, page: binding.update.page }),
      ),
    ),
  );
  return Object.freeze({
    ...state,
    /** Execute a synchronous mutation in the declaring extension's transaction. */
    update: <A>(handler: (tx: UpdateStateV0<S>) => A): A =>
      port.update(() => invokeSyncHandler(handler, updateState)),
    read: <A>(handler: (tx: ReadStateV0<S>) => A): A =>
      port.update(() => invokeSyncHandler(handler, readState)),
  });
}
declare class DocumentFactoryMapping implements Struct.Lambda {
  readonly "~lambda.in": unknown;
  readonly "~lambda.out": this["~lambda.in"] extends DocumentsV0[string]
    ? (name: string) => DocumentHandleV0<this["~lambda.in"]["schema"]["Type"]>
    : never;
}
type DocumentFactory = DocumentFactoryMapping &
  (<A, I>(spec: DocumentSpecV0<A, I>) => (name: string) => DocumentHandleV0<A>);
/** Capture the document codec before handing its name to the runtime. */
export function bindExtensionDocuments<D extends DocumentsV0>(
  documents: D,
  port: ExtensionRuntimePortV0,
): DocumentHandlesV0<D> {
  return Object.freeze(
    invokeNamedFactories(
      Struct.map(
        documents,
        Struct.lambda<DocumentFactory>(
          (spec) => (name) => port.document(name, spec),
        ),
      ),
    ),
  );
}
