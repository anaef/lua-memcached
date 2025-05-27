/*
 * Lua memcached module
 *
 * Copyright (C) 2011-2025 Andre Naef
 */


#ifndef _MEMCACHED_INCLUDED
#define _MEMCACHED_INCLUDED


#include <lua.h>


#define MEMCACHED_METATABLE "memcached"
#define MEMCACHED_BUFFER_METATABLE  "memcached.buffer"


typedef struct memcached_buffer {
	char   *b;         /* pointer to the buffer */
	size_t  pos;       /* current position in the buffer (<= len) */
	size_t  len;       /* used capacity of the buffer (<= capacity) */
	size_t  capacity;  /* maximum capacity of the buffer */
} memcached_buffer_t;


int luaopen_memcached (lua_State *L);


#endif  /* _MEMCACHED_INCLUDED */