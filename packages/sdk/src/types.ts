/**
 * Vendored subset of @standard-schema/spec v1.
 * Implemented by zod v3.24+/v4, valibot, arktype — any of them work as event schemas.
 */
export interface StandardSchemaV1<Input = unknown, Output = Input> {
  readonly "~standard": StandardSchemaV1.Props<Input, Output>;
}

export declare namespace StandardSchemaV1 {
  export interface Props<Input = unknown, Output = Input> {
    readonly version: 1;
    readonly vendor: string;
    readonly validate: (
      value: unknown,
    ) => Result<Output> | Promise<Result<Output>>;
    readonly types?: Types<Input, Output> | undefined;
  }

  export type Result<Output> = SuccessResult<Output> | FailureResult;

  export interface SuccessResult<Output> {
    readonly value: Output;
    readonly issues?: undefined;
  }

  export interface FailureResult {
    readonly issues: ReadonlyArray<Issue>;
  }

  export interface Issue {
    readonly message: string;
    readonly path?: ReadonlyArray<PropertyKey | PathSegment> | undefined;
  }

  export interface PathSegment {
    readonly key: PropertyKey;
  }

  export interface Types<Input = unknown, Output = Input> {
    readonly input: Input;
    readonly output: Output;
  }

  export type InferInput<S extends StandardSchemaV1> = NonNullable<
    S["~standard"]["types"]
  >["input"];

  export type InferOutput<S extends StandardSchemaV1> = NonNullable<
    S["~standard"]["types"]
  >["output"];
}

export type Result<T, E = string> =
  | { ok: true; value: T }
  | { ok: false; error: E };

export const ok = <T>(value: T): { ok: true; value: T } => ({ ok: true, value });
export const err = <E>(error: E): { ok: false; error: E } => ({ ok: false, error });

/**
 * Runs fn, capturing sync throws, rejections, and cross-realm thenables as
 * err. Never rejects.
 */
export const toResult = <T>(
  fn: () => T | PromiseLike<T>,
  describe: (cause: unknown) => string,
): Promise<Result<T>> =>
  Promise.resolve()
    .then(fn)
    .then(ok, (cause: unknown) => err(describe(cause)));

export type EventMap = Record<string, StandardSchemaV1>;

/**
 * What a value looks like after JSON.stringify + JSON.parse: Date becomes
 * string (via toJSON), bigint/function/symbol become never. Approximate but
 * catches the common wire-type lies.
 */
export type Jsonify<T> = [unknown] extends [T]
  ? T
  : T extends { toJSON(): infer R }
    ? Jsonify<R>
    : T extends string | number | boolean | null
      ? T
      : T extends Array<infer U>
        ? Jsonify<U>[]
        : T extends object
          ? { [K in keyof T]: Jsonify<T[K]> }
          : never;

/**
 * Discriminated union of all events in a map, as they appear ON THE WIRE
 * (after the JSON round-trip — e.g. a z.date() field is a string here).
 * Export this from your sender app so consumers can do
 * `verifier.verify<Events>(req)` with a type-only import.
 */
export type EventUnion<E extends EventMap> = {
  [K in keyof E]: {
    event: K & string;
    subject: string | null;
    data: Jsonify<StandardSchemaV1.InferOutput<E[K]>>;
  };
}[keyof E];

export interface Subscriber {
  url: string;
  /** Falls back to the top-level `signing.secret` when omitted. */
  secret?: string;
}

/**
 * What the subscriber resolver receives, discriminated on `event`. Unlike
 * EventUnion this is the validator OUTPUT before the JSON round-trip — a
 * z.date() field is still a Date here.
 */
export type ResolverEvent<E extends EventMap> = {
  [K in keyof E]: {
    event: K & string;
    subject: string | null;
    data: StandardSchemaV1.InferOutput<E[K]>;
  };
}[keyof E];

export type SubscriberResolver<E extends EventMap = EventMap> = (
  event: ResolverEvent<E>,
) => Subscriber[] | Promise<Subscriber[]>;
