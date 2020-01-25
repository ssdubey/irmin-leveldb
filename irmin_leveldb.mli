val config : string -> Irmin.config
(** Configuration values. *)

module Append_only : Irmin.APPEND_ONLY_STORE_MAKER
(** An in-memory store for append-only values. *)

module Atomic_write : Irmin.ATOMIC_WRITE_STORE_MAKER
(** An in-memory store with atomic-write guarantees. *)

module Make : Irmin.S_MAKER
(** An in-memory Irmin store. *)

module KV : Irmin.KV_MAKER
