# Lua memcached 


## Introduction

[Lua memcached](https://github.com/anaef/lua-memcached) provides a binding for
[memcached](https://memcached.org/), a high-performance, distributed memory object caching system.
It is implemented in C using the binary protocol and includes a codec for encoding and decoding Lua
values.

Here is a quick example:

```lua
local memcached = require("memcached")

local function makeValue ()
	-- ... some logic, such as a database query ...
	return {
		hello = "world"
	}
end

local client = memcached.open()
local key = "my-key"
local value = client:get(key)
if not value then
	value = makeValue()
	client:set(key, value, 60)
end
print(value.hello)
```


## Build, Test, and Install

### Building and Installing with LuaRocks

To build and install with LuaRocks, run:

```
luarocks install lua-memcached
```


### Building, Testing and Installing with Make

Lua memcached comes with a simple Makefile. Please adapt the Makefile to your environment, and
then run:

```
make
make test
make install
```

## Release Notes

Please see the [release notes](NEWS.md) document.


## Documentation

Please see the [documentation](doc/) folder.


## Limitations

Lua memcached supports Lua 5.3 and Lua 5.4.

Lua memcached has been built and tested on Ubuntu Linux (64-bit).

Lua memcached supports a (relevant) subset of memcached commands.

Lua memcached's default encoder function limits buffers to 256 megabytes.


## License

Lua memcached is released under the MIT license. See LICENSE for license terms.