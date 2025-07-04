# Multi-link/multi-prop init / change semantics

## Setting the stage

* `model.pointer = [ ... ]` ALWAYS means "replace with new data"
* `model.pointer.append(...)` ALWAYS translates into `+=`
* `model.pointer.remove(...)` ALWAYS translates into `-=`
* `model.pointer.clear()` only works when the collection is in *read-write mode*.

## Modes

* `write mode`: the collection allows `.append()` and `.remove()`
  operations. Its `__iter__`, `__len__`, `__contains__`, `__bool__`
  raise an error `"cannot access unfetched data"`.

* `read-write mode`: the collection allows all operations on itself.

## Cases

1. `u = User(); u.friends` -- `u.friends` is a *read-write mode* collection

   Why: the user object is new, its state is known.

   Examples:

   * `u.friends.append(u2)` -- translates into `u.friends += u2` in EdgeQL.

   * `iter(u.friends)` -- works just fine.

   * `u.friends = [ ... ]` -- I'm starting over; will be `u.friends = { ... }`
     in EdgeQL; `friends` is *read-write mode*.

   * `u.friends.clear()` before `save()`-- reset any changes
     I've done to the collection -- nothing is synced to the database yet.

   * `u.friends.clear()` after `save()` -- `-=` the just added friends.

   * `u.friends -= obj` -- `-=` *obj* in the db.

2. `u = User(known_id); u.friends` -- `u.friends` is a *write mode* collection.

   Why: the user object must exist in the database, but we do not know
   its state.

   Examples:

   * `u.friends.append(u2)` -- translates into `u.friends += u2` in EdgeQL.

   * `len(u.friends)` -- raises an error `"cannot access unfetched data"`;

      - does not matter if anything was appended to the collection with
        `u.friends.append(...)` or not -- we still don't know the state.

      - does not matter is `client.save(u)` is called or not -- we still
        don't know the state (we never fetched the collection).

    * `u.friends.clear()` -- clear changes that have been made so `save()`
      won't do anything.

    * `u.friends -= obj` -- `-=` *obj* in the db.

    * `u.friends = []` -- `friends` becomes *read-write mode*.

3. `u = User(known_id, friends=[]); u.friends` and
   `u = User(known_id); u.friends = []; u.friends`
    -- `u.friends` is a *read-write mode* collection.

    Why: the state is known, the user tells that they want to reset
    the collection to a new list in the db (reset the link to a new set!)

4. `u = client.get(User.select('*'))` -- `u.friends` is a *write mode* collection.

   Why: `u.friends` wasn't fetched, we don't know what's its state.

   Examples:

   * `u.friends = []` -- `friends` becomes *read-write mode* -- the user intends to
     replace the link with new data.

5. `u = client.get(User.select('*', friends=lambda u: u.friends.limit(0)))`
   -- `u.friends` is a *read-write mode* collection.

   Why: `u.friends` was fetched. No matter what the filter was, we know that
   the application was expecting a certain state of the collection and it
   must be intentional.

   Examples:

   * `u.friends = [ ... ]` -- replace the link with new data; `friends`
     is still *read-write mode*.

   * `u.friends.clear()` -- `-=` the fetched friends from the db.

   * `u.friends -= obj` -- `-=` *obj* in the db.
