(*
 * Copyright (c) 2013-2017 Thomas Gazagnaire <thomas@gazagnaire.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

open Lwt.Infix

let src = Logs.Src.create "irmin.leveldb" ~doc:"LevelDB backend store"

module Log = (val Logs.src_log src : Logs.LOG)

let config dir =
  let config = Irmin.Private.Conf.empty in
  let module C = Irmin.Private.Conf in
  let config = C.add config C.root (Some dir) in
  config

let db_ap = LevelDB.open_db "/home/shashank/work/leveldb/store/levelstore2/ap" 
let db_at = LevelDB.open_db "/home/shashank/work/leveldb/store/levelstore2/at" 
                

module Read_only (K : Irmin.Type.S) (V : Irmin.Type.S) = struct
  type key = K.t

  type value = V.t

  type 'a t = { mutable db : LevelDB.db; } 

  let v _config = 
    let map = { db = db_ap } in
        
    Lwt.return map


  let close t =
  
    LevelDB.close t.db;
    Lwt.return_unit

  let pp_key = Irmin.Type.pp K.t

  let find { db; _ } key = 

     Log.debug (fun f -> f "find %a" pp_key key);

    let keyStr = Irmin.Type.to_string K.t key in
    let valStr = LevelDB.get db keyStr in
    let valStr = (match valStr with 
                | Some v -> v
                | None -> "") in

    match (Irmin.Type.of_string V.t valStr) with 
    | Ok s -> Lwt.return_some s
    | _ -> Lwt.return_none 
    
  let mem {db; _ } key =
    Log.debug (fun f -> f "mem %a" pp_key key);
    let keyStr = Irmin.Type.to_string K.t key in
    let b = LevelDB.mem db keyStr in
      Lwt.return b

  let cast t = (t :> [ `Read | `Write ] t)

  let batch t f = f (cast t)
end

module Append_only (K : Irmin.Type.S) (V : Irmin.Type.S) = struct
  include Read_only (K) (V)

  let add t key value =
    Log.debug (fun f -> f "add -> %a" pp_key key);

    let keyStr = Irmin.Type.to_string K.t key in
    let valStr = Irmin.Type.to_string V.t value in

    LevelDB.put t.db keyStr valStr;
    
    Lwt.return_unit
end

module Atomic_write (K : Irmin.Type.S) (V : Irmin.Type.S) = struct
  module RO = Read_only (K) (V)
  module W = Irmin.Private.Watch.Make (K) (V)
  module L = Irmin.Private.Lock.Make (K)

  exception WatchNotImplemented

  type t = { db : LevelDB.db; w : W.t; lock : L.t }

  type key = RO.key

  type value = RO.value

  type watch = W.watch

  let watches = W.v () 

  let lock = L.v ()

  
  let v _config = Lwt.return { db = db_at; w = watches; lock } 

  let close t = W.clear t.w >>= fun () -> RO.close { db = t.db;}

  let aw_find t key = 
    let keyStr = Irmin.Type.to_string K.t key in
    let valStr = LevelDB.get t.db keyStr in
    let valStr = (match valStr with 
                | Some v -> v
                | None -> "") in

    match (Irmin.Type.of_string V.t valStr) with 
    | Ok s -> Lwt.return_some s
    | _ -> Lwt.return_none 

  let find t = aw_find t

  let aw_mem t key =
    let keyStr = Irmin.Type.to_string K.t key in
    let b = LevelDB.mem t.db keyStr in
    Lwt.return b
   
  let mem t = aw_mem t 

  let watch_key t =
    ignore t;
    raise WatchNotImplemented

  let watch t =
    ignore t;
    raise WatchNotImplemented

  let unwatch t =
    ignore t;
    raise WatchNotImplemented

  let rec iterate it = 
    try 
    match (LevelDB.Iterator.valid it) with 
    | true -> (let v = LevelDB.Iterator.get_key it in
                v ::(LevelDB.Iterator.next it; iterate it)
        )
    | false -> []
    with _ ->  []

  let rec converttoK keylist = 
    match keylist with
    | h::t -> 
        (match Irmin.Type.of_string K.t h with
        | Ok s -> s :: converttoK t
        | _ -> [] )
    | [] -> []

  let list t = 
     Log.debug (fun f -> f "list");

    let iter = LevelDB.iterator t.db in
    ignore @@ LevelDB.Iterator.seek_to_first iter;
    let keylist = iterate iter in 
    let listK = converttoK keylist in 

    listK |> Lwt.return 

  let set t key value =
    L.with_lock t.lock key (fun () ->
        let keyStr = Irmin.Type.to_string K.t key in
        let valStr = Irmin.Type.to_string V.t value in
        LevelDB.put t.db keyStr valStr;
        Lwt.return_unit)

  let remove t key = 
    L.with_lock t.lock key (fun () ->
        let keyStr = Irmin.Type.to_string K.t key in
        LevelDB.delete t.db keyStr;
        Lwt.return_unit)

  let test_and_set t key ~test ~set = 
    Log.debug (fun f -> f "test_and_set");

    L.with_lock t.lock key (fun () ->
        let keyStr = Irmin.Type.to_string K.t key in
        let testStr =
          match test with Some v -> Irmin.Type.to_string V.t v | None -> ""
        in
        let setStr =
          match set with Some v -> Irmin.Type.to_string V.t v | None -> ""
        in
        let tns = 
          match setStr with
          | "" ->
              remove t key; Lwt.return true
          | _ ->
              if 
                 testStr = "" then (
                 LevelDB.put t.db keyStr setStr;
                 Lwt.return true )
              else
                find t key >>= fun findval ->
                  let valu =(
                  match findval with
                  | Some v -> Irmin.Type.to_string V.t v
                  | None -> "" ) in
                  if valu = testStr then (
                    LevelDB.put t.db keyStr setStr;
                    Lwt.return true
                  ) else (
                    Lwt.return false
                  )
        in
        tns)
end

module Make =
  Irmin.Make (Irmin.Content_addressable (Append_only)) (Atomic_write)
module KV (C : Irmin.Contents.S) =
  Make (Irmin.Metadata.None) (C) (Irmin.Path.String_list) (Irmin.Branch.String)
    (Irmin.Hash.BLAKE2B)
