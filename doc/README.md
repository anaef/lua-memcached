# Lua memcached Documentation

This page describes the types, functions, and methods provided by Lua memcached.


## Types

### `memcached`

A memcached instance with methods as documented below.


### `memcached.buffer`

A buffer representing binary data. Buffers are used as performance optimization to avoid
internalizing intermittent data as Lua strings. They can be manipulated directly in C, or converted
to a Lua string using the `tostring` function.


## Functions

### `memcached.open ([args])`

Returns a new memcached instance, optionally configured by the table `args` which can have the
following keys:

- `host`: A string representing the memcached server host to connect to. Defaults to `"localhost"`.
- `port`: A string (or integer) representing the memcached server port to connect to. Defaults to
`"11211"`.
- `timeout`: An positive int representing the connect timeout in milliseconds. Defaults to `1000`.
- `reconnect`: A boolean indicating whether to reconnect after an error. Defaults to `true`.
- `encode`: A function that takes a value as its sole argument and returns a buffer or a string
representing its encoding. Defaults to `memcached.encode`.
- `decode`: A function that takes a buffer or a string representing an encoding as its sole
argument and returns its value. Defaults to `memcached.decode`.


### `memcached.encode (value)`

The default implementation of the encode function supports the types boolean, number (including
integer), string, and table. When encoding tables, pairs with an unsupported key *or* value are
not encoded but silently dropped. Recursive table structures are preserved. The function returns a
buffer with a reasonably efficient binary encoding of `value`.


### `memcached.decode (encoding)`

The default implementation of the decode function reconstructs a value from `encoding` which can
be a buffer or a string in the format returned by the `memcached.encode` function.


## `memcached` Methods

### `memcached:get (key)`

Retrieves the value of `key` from the memcached server. The method returns the value and its CAS
(check-and-set) value if the key is present on the server, and `nil` otherwise.


### `memcached:set (key, value [, expiration [, cas]]])`

Sets `value` as the value of `key` in the memcached server. If `value` is `nil`, the key is deleted
instead. The optional non-negative `expiration` argument specifies a positive time in seconds after
which the set value expires; it defaults to `0` implying no expiration. The optional `cas` argument
causes the method to fail in case of mismatch. The method returns `true` and a new CAS
(check-and-set) value if it succeeds, and `false` otherwise.


### `memcached:add (key, value [, expiration])`

Works similarly to the `set` method, but additionally fails if the key is present.


### `memcached:replace (key, value [, expiration [, cas]])`

Works similarly to the `set` method, but additionally fails if the key is *not* present.


### `memcached:inc (key [, delta [, initial [, expiration]]])`

Increases the value of `key` in the memcached server by `delta`. If the key is not present, the
method sets its value to `initial` instead. Both arguments must be non-negative integers and
default to `1`. The optional `expiration` argument works similar to the `set` method. The method
returns the resulting integer value, or `nil` if the server has an incompatible value set for the
key.


### `memcached:dec (key [, delta [, initial [, expiration]]])`

Works similar to the `inc` method, but *decreases* the value of `key` by `delta`.


### `memcached:flush ([expiration])`

Clears the cache of the memcached server. The optional non-negative `expiration` argument delays the
operation by a time in seconds; it defaults to `0` implying immediate clearing.


### `memcached:stats ([key])`

Returns a table with information from the memcached server. The optional `key` argument identifies
specific information to return. Please refer to the memcached documentation to learn more about the
information returned and the values `key` can take.


### `memcached:close ()`

Closes the memcached instance, disconnecting its associated socket as needed. After calling the
method, the instance can no longer be used.