LUA_INCDIR=/usr/include/lua5.4
LUA_BIN=/usr/bin/lua5.4
LIBDIR=/usr/local/lib/lua/5.4
CFLAGS=-Wall -Wextra -Wpointer-arith -Werror -fPIC -O3 -D_REENTRANT -D_GNU_SOURCE
LDFLAGS=-shared -fPIC

export LUA_CPATH=$(PWD)/?.so

default: all

all: memcached.so

memcached.so: memcached.o
	gcc $(LDFLAGS) -o memcached.so memcached.o

memcached.o: src/memcached.h src/memcached.c
	gcc -c -o memcached.o $(CFLAGS) -I$(LUA_INCDIR) src/memcached.c

.PHONY: test
test:
	$(LUA_BIN) test/test.lua

install:
	cp memcached.so $(LIBDIR)

clean:
	-rm -f memcached.o memcached.so