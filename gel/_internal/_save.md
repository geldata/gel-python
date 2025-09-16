# Save and Sync

This guide helps explain some of the details of how the save/sync functionality
works and interacts with other systems.


## Usage and functionality

Users can use `save` to upload changes from python into a gel database. They can
use `sync` to additionally refetch changes from the database.

These functions make it easier to keep application state consistent with its
database.

A typical use of these functions might look like this:
```py
foo = default.Foo(n=1)
bar = default.Bar(foo=foo)
client.save(foo, bar)

# after foo is changed somewhere else
client.sync(foo)
```

New objects can be inserted using either `save` or `sync` and will have its `id`
set. When `sync` is called, all fields are refetched as well.

See: `_save.push_refetch_new` and `_save.SaveExecutor._commit`

Existing objects will only refetch fields which were previously set or fetched.
If a single database object is represented by multiple python objects, they are
all updated, but with the appropriate subset of fields.

See: `_save.push_refetch_existing`


### Reachable objects

Both `save` and `sync` will apply to objects directly passed as arguments, as
well as any objects which can be reached via links.

For example, here both objects are synced:
```py
foo = default.Foo(n=1)
bar = default.Bar(foo=foo)
client.save(bar)
```

See: `_save.make_plan` for where `existing_objects` is updated.

For `sync`, all reachable objects are refetched, even though these are the same
objects that get scanned for changes that need to be saved in the first place.
The reasoning is that even if there were no direct changes, they might be
affected by changes in other objects (e.g. because of backlink computeds).


### Refetching links

When links are refetched, the source object will be updated with a target object
according to the following priority:
- The existing link target
- A reachable object, either existing or a refetched new
    - Chosen arbitrarily when multiple are available
- A new object with only `id`

See: `_descriptors.reconcile_link` and `_descriptors.reconcile_proxy_link`.

Multi links aren't refetched entirely to avoid performance issues. Instead,
existing data is reconciled with the delta (new and updated object IDs) using a
filter.

The refetch filter includes:

- All existing link target IDs from the Python field
- All IDs from the delta

This captures both additions and removals to the multi-link.

**Note**: For partially-fetched multi-links, original filtering criteria
(filter, offset, limit) may no longer apply after reconciliation.

See: `_save._compile_refetch` where `ptr.cardinality.is_multi()`

### Link properties

Link properties follow the python object model, instead of the gel model.
As a result, overwriting a link with the same object will overwrite all its
link properties.

This is illustrated in the following examples:
```
foo = default.Foo()
bar = default.Bar(foo=default.Bar.foo.link(foo, a=1, b=2, c=3))
client.sync(bar)

# updates a, keeps b and c
bar.foo.__linkprops__.a = 9

# resets a, b, and c
bar.foo = foo
```


## Implementation

When either `save` or `sync` is called on a client, it calls an underlying
`_save_impl` which does the actual work.

The general order of operations is:
- Make a save plan
- Compile and execute batch queries
- If sync, compile and execute refetch queries
- Commit the changes


### Save plan

Unlike a call to `query` or `execute`, a call to `save` or `sync` may be split
into multiple sub-queries. The save plan forms the general outline of how these
are arranged.

In `make_save_executor_constructor`, after creating a save plan, these are
stored in a `SaveExecutor` which tracks different objects throughout the
save/sync process.

Reachable objects are traversed in graph order and checked whether they are new
and which properties and links were changed. It then creates `ModelChange`
bundled into a list of `QueryBatch`s.

Which fields to change is determined using `__gel_get_changed_fields__`.

A `ModelChange` represents changes to a single object. A `QueryBatch` represents
changes to the model which can be run independently of each other. Batches are
grouped into insert and update batches.

See: `_save.make_plan`

When syncing, `_save._add_refetch_shape` tracks the fields to refetch for each
object.


### Batch queries

The `_save.SaveExecutor.__iter__` function iterates over the insert and update
batches, compiles them into queries, and groups them by similar queries.

The `_save.SaveExecutor._compile_batch` does the actual compiling by generating
edgeql for each property and link change, and assembling them into a shape.
The shape is then applied to an insert or update, and the resulting statement
is wrapped in a `select` which differs between `sync` and `select`:
- for `save`: `select (...).id`
- for `sync`: `select (...) { * }`

For `save`, only the `id` of a new object is updated. But for sync,
`GelModel` instances of the new objects get stored in
`_save.SaveExecutor.new_objects`. Theseare used when updating refetched links.

Query arguments are assembled into a `__data` (or `__all_data` for multi)
argument which is a tuple any new data for that object, including the object id
for updates.

The compiled queries are grouped by their query string into `QueryBatch`s.

Finally the results of executed queries is stored using
`_save.QueryBatch.record_inserted_data`.


### Refetch queries

The `_save.SaveExecutor.get_refetch_queries` function compiles the refetch
queries and groups them by object type.

It works similarly to `_compile_batch` in that it generates edgeql for each
prroperty and link and assembles them into a shape.

A refetch query has 3 parameters:
- `__new`: ids of all new objects
- `__existing`: ids of all existing objects
- `__spec`: an array of tuples of:
    - object id
    - an array of tuples of:
        - link indexes
        - ids of objects previously in that link

The `__spec` parameter is used to filter multi links as discussed above.

The refetched data is a sequence of `GelModel` instances which are stored in
`_save.SaveExecutor.refetched_data` using
`_save.QueryRefetch.record_refetched_data`.

### Commiting changes

Up until this point, no changes are actually applied to user objects yet.
Only once all the refetches are executed and their results recorded are any
changes made.

In `_save.SaveExecutor._commit`:
- Ids are applied to new objects
- Refetch data is applied to existing object
- Refetch data is applied to new objects

Existing objects will have two (or more) `GelModel` instances:
- the user instance(s)
- the refetch instance

The refetch data can be simply applied to the user instance(s) using
`_save.QueryRefetch._apply_refetched_data_shape`.

In contrast, new objects have three `GelModel` instances:
- the user instance
- the batch instance (stored in `_save.SaveExecutor.new_objects`)
- the refetch instance

Since the batch instance is used to update refetched links, both the
batch and user instances need to be updated. This is done by updating
the batch instance in `_apply_refetched_data_shape`, then later updating
the user instance.

After all changes are made, `_save.SaveExecutor._commit_recursive` "locks in"
changes to the models by resetting the changed fields flags, resetting
`_added_items` and `_removed_items` in tracked lists, etc.

Finally, there is a post-commit check step which ensures that no changes are
made and that re-running save would essentially be a no-op.
