/*
 * Lua memcached module
 *
 * Copyright (C) 2011-2025 Andre Naef
 */


#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <endian.h>
#include <memcached/protocol_binary.h>
#include <lua.h>
#include <lauxlib.h>
#include "memcached.h"


/* buffer */
#define MEMCACHED_BUFFER_SIZE  1024
#ifndef MEMCACHED_BUFFER_MAX
#define MEMCACHED_BUFFER_MAX   (256 * 1024 * 1024)  /* 256 MB */
#endif  /* MEMCACHED_BUFFER_MAX */

/* additional 'types' */
#define MEMCACHED_TYPE_BOOLEANTRRUE  LUA_TBOOLEAN + 64
#define MEMCACHED_TYPE_INTEGER       LUA_TNUMBER + 64
#define MEMCACHED_TYPE_STRINGSHORT   LUA_TSTRING + 64
#define MEMCACHED_TYPE_TABLE8        LUA_TTABLE
#define MEMCACHED_TYPE_TABLE16       LUA_TTABLE + 16
#define MEMCACHED_TYPE_TABLE32       LUA_TTABLE + 32
#define MEMCACHED_TYPE_TABLE64       LUA_TTABLE + 32 + 16
#define MEMCACHED_TYPE_TABLEREF      LUA_TTABLE + 64
#define MEMCACHED_CODEC_VERSION  "LM\xf6\x02"  /* version 2 */

/* response flags */
#define MEMCACHED_EXTRAS        1
#define MEMCACHED_KEY           2
#define MEMCACHED_VALUE         4
#define MEMCACHED_VALUE_BUFFER  8

/* request extras */
#define MEMCACHED_REQUEST_BASE  \
		(sizeof(((protocol_binary_request_no_extras *)0)->bytes))
#define MEMCACHED_REQUEST_GET_EXTRAS  \
		((sizeof(((protocol_binary_request_get *)0)->bytes)) - MEMCACHED_REQUEST_BASE)
#define MEMCACHED_REQUEST_SET_EXTRAS  \
		((sizeof(((protocol_binary_request_set *)0)->bytes)) - MEMCACHED_REQUEST_BASE)
#define MEMCACHED_REQUEST_DELETE_EXTRAS  \
		((sizeof(((protocol_binary_request_delete *)0)->bytes)) - MEMCACHED_REQUEST_BASE)
#define MEMCACHED_REQUEST_INCR_EXTRAS  \
		((sizeof(((protocol_binary_request_incr *)0)->bytes)) - MEMCACHED_REQUEST_BASE)
#define MEMCACHED_REQUEST_FLUSH_EXTRAS  \
		((sizeof(((protocol_binary_request_flush *)0)->bytes)) - MEMCACHED_REQUEST_BASE)
#define MEMCACHED_REQUEST_STATS_EXTRAS  \
		((sizeof(((protocol_binary_request_stats *)0)->bytes)) - MEMCACHED_REQUEST_BASE)


typedef struct memcached {
	int  host_index;    /* network host (string) */
	int  port_index;    /* network port/service (string) */
	int  encode_index;  /* encode function */
	int  decode_index;  /* decode function */
	int  timeout;       /* connect timeout (milliseconds) */
	int  fd;            /* socket */
	int  reconnect:1;   /* reconnect on error */
	int  closed:1;      /* closed */
} memcached_t;

typedef struct backref {
	int          index;
	lua_Integer  cnt;
} backref_t;


/* buffer */
static int buffer_require(lua_State *L, memcached_buffer_t *b, size_t cnt);
static int buffer_avail(lua_State *L, memcached_buffer_t *b, size_t cnt);
static int buffer_tostring(lua_State *L);
static int buffer_free(lua_State *L);

/* codec */
static inline int supported(lua_State *L, int index);
static int encode(lua_State *L, memcached_buffer_t *b, backref_t *br, int index);
static int decode(lua_State *L, memcached_buffer_t *b, backref_t *br);
static int decodetable(lua_State *L, memcached_buffer_t *b, backref_t *br, int64_t narr,
		int64_t nrec);
static int mencode(lua_State *L);
static int mdecode(lua_State *L);

/* network */
static int getsocket(lua_State *L, memcached_t *m);
static ssize_t checkresult(lua_State *L, memcached_t *m, ssize_t result);
static ssize_t sendnosig(lua_State *L, memcached_t *m, const void *buf, size_t len);
static ssize_t sendmsgnosig(lua_State *L, memcached_t *m, const struct iovec *iov, int iovcnt);
static int recvnosig(lua_State *L, memcached_t *m, void *buf, size_t len);
static int recvstring(lua_State *L, memcached_t *m, size_t len);

/* main */
static int getstring(lua_State *L, int index, const char *field, const char *dflt);
static int getfunction(lua_State *L, int index, const char *field, lua_CFunction dflt);
static int getint(lua_State *L, int index, const char *field, int dflt);
static int getboolean(lua_State *L, int index, const char *field, int dflt);
static int mopen(lua_State *L);
static int recvresponse(lua_State *L, memcached_t *m, uint16_t *status, uint64_t *cas, int flags);
static int get(lua_State *L);
static int set(lua_State *L);
static int incr(lua_State *L);
static int flush(lua_State *L);
static int stats(lua_State *L);
static int quit(lua_State *L);
static int mclose(lua_State *L);
static int tostring(lua_State *L);


static const luaL_Reg functions[] = {
	{ "open", mopen },
	{ "encode", mencode },
	{ "decode", mdecode },
	{ NULL, NULL }
};


/*
* buffer
*/

static int buffer_require (lua_State *L, memcached_buffer_t * b, size_t cnt) {
	char   *bnew;
	size_t  required, capacity;

	if (cnt > SIZE_MAX - b->pos || b->pos + cnt > MEMCACHED_BUFFER_MAX) {
		return luaL_error(L, "buffer overflow");
	}
	required = b->pos + cnt;
	if (b->capacity >= required) {
		return 0;
	}

	/* calculate new capacity using hybrid strategy */
	capacity = b->capacity;
	if (capacity == 0) {
		capacity = MEMCACHED_BUFFER_SIZE;
	}
	while (capacity < required) {
		if (capacity < 64 * 1024) {
			/* double for small buffers */
			if (capacity <= SIZE_MAX / 2) {
				capacity = capacity * 2;
			} else {
				capacity = required;
			}
		} else {
			/* grow by 50% for large buffers */
			if (capacity <= SIZE_MAX / 3 * 2) {
				capacity = capacity + capacity / 2;
			} else {
				capacity = required;
			}
		}
	}

	/* reallocate */
	bnew = realloc(b->b, capacity);
	if (!bnew) {
		return luaL_error(L, "out of memory");
	}
	b->b = bnew;
	b->capacity = capacity;

	return 0;
}

static int buffer_avail (lua_State *L, memcached_buffer_t *b, size_t cnt) {
	if (cnt > SIZE_MAX - b->pos || b->pos + cnt > b->len) {
		return luaL_error(L, "buffer underflow");
	}
	return 0;
}

static int buffer_free (lua_State *L) {
	memcached_buffer_t  *b;

	b = luaL_checkudata(L, -1, MEMCACHED_BUFFER_METATABLE);
	if (b->b != NULL) {
		free(b->b);
		b->b = NULL;
	}
	return 0;
}

static int buffer_tostring (lua_State *L) {
	memcached_buffer_t  *b;
	
	b = luaL_checkudata(L, -1, MEMCACHED_BUFFER_METATABLE);
	if (b->b) {
		lua_pushlstring(L, b->b, b->len);
	} else {
		lua_pushliteral(L, "");
	}
	return 1;
}


/*
 * codec
 */

 static inline int supported (lua_State *L, int index) {
	switch (lua_type(L, index)) {
	case LUA_TBOOLEAN:
	case LUA_TNUMBER:
	case LUA_TSTRING:
	case LUA_TTABLE:
		return 1;

	default:
		return 0;
	}
}

static int encode (lua_State *L, memcached_buffer_t *b, backref_t *br, int index) {
	double       d;
	size_t       len, size_pos;
	uint16_t     n16;
	uint32_t     n32;
	int64_t      i, t, narr, nrec;
	uint64_t     nlen;
	const char  *s;

	switch (lua_type(L, index)) {
	case LUA_TBOOLEAN:
		buffer_require(L, b, 1);
		b->b[b->pos++] = (char)lua_toboolean(L, index) ? MEMCACHED_TYPE_BOOLEANTRRUE : LUA_TBOOLEAN;
		break;

	case LUA_TNUMBER:
		if (lua_isinteger(L, index)) {
			buffer_require(L, b, 1 + sizeof(i));
			b->b[b->pos++] = (char)MEMCACHED_TYPE_INTEGER;
			i = htobe64(lua_tointeger(L, index));
			memcpy(&b->b[b->pos], &i, sizeof(i));
			b->pos += sizeof(i);
		} else {
			buffer_require(L, b, 1 + sizeof(d));
			b->b[b->pos++] = (char)LUA_TNUMBER;
			d = (double)lua_tonumber(L, index);
			memcpy(&b->b[b->pos], &d, sizeof(d));
			b->pos += sizeof(d);
		}
		break;
		
	case LUA_TSTRING:
		s = lua_tolstring(L, index, &len);
		if (len > UINT64_MAX - (1 + sizeof(len))) {
			return luaL_error(L, "string too long");
		}
		if (len <= 255) {
			buffer_require(L, b, 1 + sizeof(uint8_t) + len);
			b->b[b->pos++] = (char)MEMCACHED_TYPE_STRINGSHORT;
			b->b[b->pos++] = (uint8_t)len;
		} else {
			buffer_require(L, b, 1 + sizeof(len) + len);
			b->b[b->pos++] = (char)LUA_TSTRING;
			nlen = htobe64(len);
			memcpy(&b->b[b->pos], &nlen, sizeof(nlen));
			b->pos += sizeof(nlen);
		}
		memcpy(&b->b[b->pos], s, len);
		b->pos += len;
		break;

	case LUA_TTABLE:
		/* check stack */
		luaL_checkstack(L, 2, "encoding table");

		/* test if the table has already been encoded */
		lua_pushvalue(L, index);
		lua_rawget(L, br->index);
		if (!lua_isnil(L, -1)) {
			/* encode backref */
			buffer_require(L, b, 1 + sizeof(t));
			b->b[b->pos++] = (char)MEMCACHED_TYPE_TABLEREF;
			t = htobe64(lua_tointeger(L, -1));
			memcpy(&b->b[b->pos], &t, sizeof(t));
			b->pos += sizeof(t);
			lua_pop(L, 1);
			break;
		}

		/* store table for backrefs */
		if (br->cnt == LUA_MAXINTEGER) {
			return luaL_error(L, "too many tables");
		}
		lua_pop(L, 1);
		lua_pushvalue(L, index);
		lua_pushinteger(L, ++br->cnt);
		lua_rawset(L, br->index);

		/* analyze and write table */
		buffer_require(L, b, 1 + 2);
		b->b[b->pos++] = (char)MEMCACHED_TYPE_TABLE8;
		size_pos = b->pos;
		b->pos += 2;
		narr = nrec = 0;
		lua_pushnil(L);
		while (lua_next(L, index)) {
			if (supported(L, -2) && supported(L, -1)) {
				if (nrec == 0 && lua_tointeger(L, -2) == narr + 1) {
					if (narr == INT64_MAX) {
						return luaL_error(L, "too many array elements");
					}
					narr++;
				} else {
					if (nrec == INT64_MAX) {
						return luaL_error(L, "too many record elements");
					}
					nrec++;
				}
				encode(L, b, br, lua_gettop(L) - 1);
				encode(L, b, br, lua_gettop(L));
			}	
			lua_pop(L, 1);
		}
		if (narr <= UINT8_MAX && nrec <= UINT8_MAX) {
			b->b[size_pos++] = (char)narr;
			b->b[size_pos] = (char)nrec;
		} else if (narr <= UINT16_MAX && nrec <= UINT16_MAX) {
			b->b[size_pos - 1] = MEMCACHED_TYPE_TABLE16;
			buffer_require(L, b, 4 - 2);
			memmove(&b->b[size_pos + 4], &b->b[size_pos + 2], b->pos - size_pos - 2);
			b->pos += 4 - 2;
			n16 = htobe16((uint16_t)narr);
			memcpy(&b->b[size_pos], &n16, sizeof(n16));
			size_pos += sizeof(n16);
			n16 = htobe16((uint16_t)nrec);
			memcpy(&b->b[size_pos], &n16, sizeof(n16));
		} else if (narr <= UINT32_MAX && nrec <= UINT32_MAX) {
			b->b[size_pos - 1] = MEMCACHED_TYPE_TABLE32;
			buffer_require(L, b, 8 - 2);
			memmove(&b->b[size_pos + 8], &b->b[size_pos + 2], b->pos - size_pos - 2);
			b->pos += 8 - 2;
			n32 = htobe32((uint32_t)narr);
			memcpy(&b->b[size_pos], &n32, sizeof(n32));
			size_pos += sizeof(n32);
			n32 = htobe32((uint32_t)nrec);
			memcpy(&b->b[size_pos], &n32, sizeof(n32));
		} else {
			b->b[size_pos - 1] = MEMCACHED_TYPE_TABLE64;
			buffer_require(L, b, 16 - 2);
			memmove(&b->b[size_pos + 16], &b->b[size_pos + 2], b->pos - size_pos - 2);
			b->pos += 16 - 2;
			narr = htobe64(narr);
			memcpy(&b->b[size_pos], &narr, sizeof(narr));
			size_pos += sizeof(narr);
			nrec = htobe64(nrec);
			memcpy(&b->b[size_pos], &nrec, sizeof(nrec));
		}
		break;

	default:
		return luaL_error(L, "unsupported type");
	}

	return 0;
}

static int decode (lua_State *L, memcached_buffer_t *b, backref_t *br) {
	size_t    len;
	double    d;
	uint8_t   narr8, nrec8;
	uint16_t  narr16, nrec16;
	uint32_t  narr32, nrec32;
	int64_t   i, t, narr, nrec;
	uint64_t  nlen;

	buffer_avail(L, b, 1);
	switch (b->b[b->pos++]) {
	case LUA_TBOOLEAN:
		lua_pushboolean(L, 0);
		break;

	case MEMCACHED_TYPE_BOOLEANTRRUE:
		lua_pushboolean(L, 1);
		break;

	case LUA_TNUMBER:
		buffer_avail(L, b, sizeof(d));
		memcpy(&d, &b->b[b->pos], sizeof(d));
		b->pos += sizeof(d);
		lua_pushnumber(L, d);
		break;

	case MEMCACHED_TYPE_INTEGER:
		buffer_avail(L, b, sizeof(i));
		memcpy(&i, &b->b[b->pos], sizeof(i));
		b->pos += sizeof(i);
		lua_pushinteger(L, be64toh(i));
		break;

	case LUA_TSTRING:
		buffer_avail(L, b, sizeof(nlen));
		memcpy(&nlen, &b->b[b->pos], sizeof(nlen));
		b->pos += sizeof(nlen);
		len = be64toh(nlen);
		buffer_avail(L, b, len);
		lua_pushlstring(L, &b->b[b->pos], len);
		b->pos += len;
		break;

	case MEMCACHED_TYPE_STRINGSHORT:
		buffer_avail(L, b, sizeof(uint8_t));
		len = (uint8_t)b->b[b->pos++];
		buffer_avail(L, b, len);
		lua_pushlstring(L, &b->b[b->pos], len);
		b->pos += len;
		break;

	case MEMCACHED_TYPE_TABLE8:
		buffer_avail(L, b, sizeof(narr8) + sizeof(nrec8));
		narr8 = (uint8_t)b->b[b->pos++];
		nrec8 = (uint8_t)b->b[b->pos++];
		decodetable(L, b, br, (int64_t)narr8, (int64_t)nrec8);
		break;

	case MEMCACHED_TYPE_TABLE16:
		buffer_avail(L, b, sizeof(narr16) + sizeof(nrec16));
		memcpy(&narr16, &b->b[b->pos], sizeof(narr16));
		b->pos += sizeof(narr16);
		narr16 = be16toh(narr16);
		memcpy(&nrec16, &b->b[b->pos], sizeof(nrec16));
		b->pos += sizeof(nrec16);
		nrec16 = be16toh(nrec16);
		decodetable(L, b, br, (int64_t)narr16, (int64_t)nrec16);
		break;

	case MEMCACHED_TYPE_TABLE32:
		buffer_avail(L, b, sizeof(narr32) + sizeof(nrec32));
		memcpy(&narr32, &b->b[b->pos], sizeof(narr32));
		b->pos += sizeof(narr32);
		narr32 = be32toh(narr32);
		memcpy(&nrec32, &b->b[b->pos], sizeof(nrec32));
		b->pos += sizeof(nrec32);
		nrec32 = be32toh(nrec32);
		decodetable(L, b, br, (int64_t)narr32, (int64_t)nrec32);
		break;

	case MEMCACHED_TYPE_TABLE64:
		buffer_avail(L, b, sizeof(narr) + sizeof(nrec));
		memcpy(&narr, &b->b[b->pos], sizeof(narr));
		b->pos += sizeof(narr);
		narr = be64toh(narr);
		memcpy(&nrec, &b->b[b->pos], sizeof(nrec));
		b->pos += sizeof(nrec);
		nrec = be64toh(nrec);
		if (narr < 0 || nrec < 0) {
			return luaL_error(L, "bad table size");
		}
		decodetable(L, b, br, narr, nrec);
		break;	

	case MEMCACHED_TYPE_TABLEREF:
		buffer_avail(L, b, sizeof(t));
		memcpy(&t, &b->b[b->pos], sizeof(t));
		b->pos += sizeof(t);
		lua_rawgeti(L, br->index, be64toh(t));
		if (lua_isnil(L, -1)) {
			return luaL_error(L, "bad backref");
		}
		break;

	default:
		return luaL_error(L, "unsupported type");
	}

	return 1;
}

static int decodetable (lua_State *L, memcached_buffer_t *b, backref_t *br, int64_t narr,
		int64_t nrec) {
	/* store the table for backrefs */
	luaL_checkstack(L, 3, "decoding table");
	lua_createtable(L, narr <= INT_MAX ? (int)narr : INT_MAX,
			nrec <= INT_MAX ? (int)nrec : INT_MAX);
	lua_pushvalue(L, -1);
	lua_rawseti(L, br->index, ++br->cnt);

	/* decode table content */
	for (; narr > 0; narr--) {
		decode(L, b, br);
		decode(L, b, br);
		lua_rawset(L, -3);
	}
	for (; nrec > 0; nrec--) {
		decode(L, b, br);
		decode(L, b, br);
		lua_rawset(L, -3);
	}
	return 1;
}

static int mencode (lua_State *L) {
	backref_t            br;
	memcached_buffer_t  *b;

	/* check arguments */
	luaL_checkany(L, 1);

	/* prepare backrefs */
	br.cnt = 0;
	lua_newtable(L);
	br.index = lua_gettop(L);

	/* prepare buffer */
	b = lua_newuserdata(L, sizeof(memcached_buffer_t));
	memset(b, 0, sizeof(memcached_buffer_t));
	luaL_getmetatable(L, MEMCACHED_BUFFER_METATABLE);
	lua_setmetatable(L, -2);
	b->b = malloc(MEMCACHED_BUFFER_SIZE);
	if (b->b == NULL) {
		return luaL_error(L, "out of memory");
	}
	b->capacity = MEMCACHED_BUFFER_SIZE;

	/* write codec version */
	buffer_require(L, b, sizeof(MEMCACHED_CODEC_VERSION) - 1);
	memcpy(&b->b[b->pos], MEMCACHED_CODEC_VERSION, sizeof(MEMCACHED_CODEC_VERSION) - 1);
	b->pos += sizeof(MEMCACHED_CODEC_VERSION) - 1;

	/* encode */
	encode(L, b, &br, 1);
	b->len = b->pos;

	/* return buffer */
	return 1;
}

static int mdecode (lua_State *L) {
	backref_t            br;
	memcached_buffer_t  *b, bs;

	/* check arguments and prepare buffer */
	b = luaL_testudata(L, 1, MEMCACHED_BUFFER_METATABLE);
	if (!b) {
		bs.b = (char *)luaL_checklstring(L, 1, &bs.len);
		bs.capacity = bs.len;
		b = &bs;
	}
	b->pos = 0;

	/* prepare backrefs */
	br.cnt = 0;
	lua_newtable(L);
	br.index = lua_gettop(L);

	/* check codec version */
	buffer_avail(L, b, sizeof(MEMCACHED_CODEC_VERSION) - 1);
	if (memcmp(&b->b[b->pos], MEMCACHED_CODEC_VERSION, sizeof(MEMCACHED_CODEC_VERSION) - 1) != 0) {
		return luaL_error(L, "bad codec version");
	}
	b->pos += sizeof(MEMCACHED_CODEC_VERSION) - 1;

	/* decode */
	decode(L, b, &br);
	if (b->pos < b->len) {
		return luaL_error(L, "extra data in buffer");
	}

	return 1;
}


/*
 * network
*/

static int getsocket (lua_State *L, memcached_t *m) {
	int               fd, flags, result, err;
	socklen_t         len;
	const char       *host, *port;
	struct pollfd     pfd;
	struct addrinfo   hints, *results, *rp;

	/* check state */
	if (m->closed) {
		return luaL_error(L, "closed");
	}

	/* nothing to do? */
	if (m->fd >= 0) {
		return 0;
	}

	/* resolve */
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	lua_rawgeti(L, LUA_REGISTRYINDEX, m->host_index);
	host = lua_tostring(L, -1);
	lua_rawgeti(L, LUA_REGISTRYINDEX, m->port_index);
	port = lua_tostring(L, -1);
	lua_pop(L, 2);
	if (getaddrinfo(host, port, &hints, &results)) {
		return luaL_error(L, "error resolving '%s:%s'", host, port);
	}

	/* connect */
	err = 0;
	for (rp = results; rp != NULL; rp = rp->ai_next) {
		/* create socket */
		fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
		if (fd == -1) {
			err = errno;
			continue;
		}

		/* disable Nagle algorithm */
		if (rp->ai_protocol == IPPROTO_TCP) {
			flags = 1;
			if (setsockopt(fd, rp->ai_protocol, TCP_NODELAY, &flags, sizeof(flags)) == -1) {
				err = errno;
				close(fd);
				continue;
			}
		}

		/* reuse address */
		flags = 1;
		if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &flags, sizeof(flags)) == -1) {
			err = errno;
			close(fd);
			continue;
		}

		/* temporarily make non-blocking */
		flags = fcntl(fd, F_GETFL, 0);
		if (flags == -1 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
			err = errno;
			close(fd);
			continue;
		}

		/* connect */
		result = connect(fd, rp->ai_addr, rp->ai_addrlen);
		if (result == 0) {
			/* connected immediately */
			if (fcntl(fd, F_SETFL, flags) == -1) {
				err = errno;
				close(fd);
				continue;
			}
			break;
		} else if (errno != EINPROGRESS) {
			err = errno;
			close(fd);
			continue;
		}

		/* wait for connection */
		pfd.fd = fd;
		pfd.events = POLLOUT;
		interrupted:
		result = poll(&pfd, 1, m->timeout);
		if (result == 1) {
			/* check outcome */
			len = sizeof(err);
			if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) == -1) {
				err = errno;
				close(fd);
				continue;
			}
			if (err != 0) {
				close(fd);
				continue;
			}

			/* connected */
			if (fcntl(fd, F_SETFL, flags) == -1) {
				err = errno;
				close(fd);
				continue;
			}
			break;
		}
		if (result < 0 && errno == EINTR) {
			/* interrupted by signal, try again */
			goto interrupted;
		}

		/* poll timed out, or failed for other reason */
		err = result == 0 ? ETIMEDOUT : errno;
		close(fd);
	}
	freeaddrinfo(results);
	if (rp == NULL) {
		return luaL_error(L, "error connecting to '%s:%s': %s (%d)", host, port, strerror(err),
				err);
	}

	/* store socket */
	m->fd = fd;

	return 0;
}

static ssize_t checkresult (lua_State *L, memcached_t *m, ssize_t result) {
	int  err;

	if (result > 0) {
		return result;
	}
	err = errno;
	if (result < 0 && err == EINTR) {
		/* interrupted by signal, try again */
		return 0;
	}
	close(m->fd);
	m->fd = -1;
	if (!m->reconnect) {
		m->closed = 1;
	}
	if (result == 0) {
		return luaL_error(L, "socket closed");
	} else {
		return luaL_error(L, "socket error: %s (%d)", strerror(err), err);
	}
}

static ssize_t sendnosig (lua_State *L , memcached_t *m, const void *buf, size_t len) {
	return checkresult(L, m, send(m->fd, buf, len, MSG_NOSIGNAL));
}

static ssize_t sendmsgnosig (lua_State *L, memcached_t *m, const struct iovec *iov, int iovcnt) {
	struct msghdr  msg;

	memset(&msg, 0, sizeof(msg));
	msg.msg_iov = (struct iovec *)iov;
	msg.msg_iovlen = iovcnt;
	return checkresult(L, m, sendmsg(m->fd, &msg, MSG_NOSIGNAL));
}

static int recvnosig (lua_State *L, memcached_t *m, void *buf, size_t len) {
	char    *b;
	ssize_t  result;

	b = buf;
	do {
		result = checkresult(L, m, recv(m->fd, b, len, 0));
		b += result;
		len -= result;
	} while (len > 0);
	return 0;
}

static int recvstring (lua_State *L, memcached_t *m, size_t len) {
	char        *b;
	luaL_Buffer  B;

	b = luaL_buffinitsize(L, &B, len);
	recvnosig(L, m, b, len);
	luaL_pushresultsize(&B, len);
	return 1;
}


/*
 * main
*/

static int getstring (lua_State *L, int index, const char *field, const char *dflt) {
	if (lua_isnoneornil(L, index)) {
		lua_pushstring(L, dflt);
	} else {
		switch (lua_getfield(L, index, field)) {
		case LUA_TNIL:
			lua_pop(L, 1);
			lua_pushstring(L, dflt);
			break;

		case LUA_TSTRING:
			break;

		case LUA_TNUMBER:
			lua_tostring(L, -1);
			break;

		default:
			return luaL_error(L, "bad field '%s' (string expected, got %s)", field,
						luaL_typename(L, -1));
		}
	}
	return luaL_ref(L, LUA_REGISTRYINDEX);
}

static int getfunction (lua_State *L, int index, const char *field, lua_CFunction dflt) {
	if (lua_isnoneornil(L, index)) {
		lua_pushcfunction(L, dflt);
	} else {
		switch (lua_getfield(L, index, field)) {
		case LUA_TNIL:
			lua_pop(L, 1);
			lua_pushcfunction(L, dflt);
			break;

		case LUA_TFUNCTION:
			break;

		default:
			return luaL_error(L, "bad field '%s' (function expected, got %s)", field, 
					luaL_typename(L, -1));
		}
	}
	return luaL_ref(L, LUA_REGISTRYINDEX);
}

static int getint (lua_State *L, int index, const char *field, int dflt) {
	int          isinteger;
	lua_Integer  result;

	if (lua_isnoneornil(L, index)) {
		return dflt;
	} else {
		switch (lua_getfield(L, index, field)) {
		case LUA_TNIL:
			lua_pop(L, 1);
			return dflt;

		case LUA_TNUMBER:
			result = lua_tointegerx(L, -1, &isinteger);
			if (isinteger && result >= INT_MIN && result <= INT_MAX) {
				lua_pop(L, 1);
				return result;
			}
			/* fall through */

		default:
			return luaL_error(L, "bad field '%s' (int expected, got %s)", field,
					luaL_typename(L, -1));
		}
	}
}

static int getboolean (lua_State *L, int index, const char *field, int dflt) {
	int  result;

	if (lua_isnoneornil(L, index)) {
		return dflt;
	} else {
		switch (lua_getfield(L, index, field)) {
		case LUA_TNIL:
			lua_pop(L, 1);
			return dflt;

		case LUA_TBOOLEAN:
			result = lua_toboolean(L, -1);
			lua_pop(L, 1);
			return result;

		default:
			return luaL_error(L, "bad field '%s' (boolean expected, got %s)", field,
					luaL_typename(L, -1));
		}
	}
}

static int mopen (lua_State *L) {
	memcached_t  *m;

	/* check arguments */
	if (!lua_isnoneornil(L, 1)) {
		luaL_checktype(L, 1, LUA_TTABLE);
	}

	/* create memcached */
	m = lua_newuserdata(L, sizeof(memcached_t));
	m->host_index = m->port_index = m->encode_index = m->decode_index = LUA_NOREF;
	m->closed = 0;
	m->fd = -1;
	luaL_getmetatable(L, MEMCACHED_METATABLE);
	lua_setmetatable(L, -2);

	/* get configuration */
	m->host_index = getstring(L, 1, "host", "localhost");
	m->port_index = getstring(L, 1, "port", "11211");
	m->encode_index = getfunction(L, 1, "encode", mencode);
	m->decode_index = getfunction(L, 1, "decode", mdecode);
	m->timeout = getint(L, 1, "timeout", 1000);
	luaL_argcheck(L, m->timeout > 0, 1, "bad timeout");
	m->reconnect = getboolean(L, 1, "reconnect", 1);
	
	return 1;
}

static int recvresponse (lua_State *L, memcached_t *m, uint16_t *status, uint64_t *cas, int flags) {
	int                                 nret;
	uint8_t                             extlen;
	uint16_t                            keylen;
	uint32_t                            bodylen, valuelen;
	memcached_buffer_t                 *b;
	protocol_binary_response_no_extras  response;

	/* receive */
	recvnosig(L, m, &response, sizeof(response.bytes));
	if (response.message.header.response.magic != PROTOCOL_BINARY_RES) {
		close(m->fd);
		m->fd = -1;
		if (!m->reconnect) {
			m->closed = 1;
		}
		return luaL_error(L, "bad response");
	}

	/* status */
	if (status) {
		*status = be16toh(response.message.header.response.status);
	}

	/* CAS */
	if (cas) {
		*cas = be64toh(response.message.header.response.cas);
	}

	/* extras */
	nret = 0;
	extlen = response.message.header.response.extlen;
	if (extlen) {
		recvstring(L, m, extlen);
		if (flags & MEMCACHED_EXTRAS) {
			nret++;
		} else {
			lua_pop(L, 1);
		}
	}

	/* key */
	keylen = be16toh(response.message.header.response.keylen);
	if (keylen) {
		recvstring(L, m, keylen);
		if (flags & MEMCACHED_KEY) {
			nret++;
		} else {
			lua_pop(L, 1);
		}
	}

	/* value */
	bodylen = be32toh(response.message.header.response.bodylen);
	if (bodylen > extlen + keylen) {
		valuelen = bodylen - (extlen + keylen);
		if (flags & MEMCACHED_VALUE_BUFFER) {
			b = lua_newuserdata(L, sizeof(memcached_buffer_t));
			memset(b, 0, sizeof(memcached_buffer_t));
			luaL_getmetatable(L, MEMCACHED_BUFFER_METATABLE);
			lua_setmetatable(L, -2);
			b->b = malloc(valuelen);
			if (b->b == NULL) {
				return luaL_error(L, "out of memory");
			}
			b->capacity = valuelen;
			recvnosig(L, m, b->b, valuelen);
			b->len = b->pos = valuelen;
		} else {
			recvstring(L, m, valuelen);
		}
		if (flags & MEMCACHED_VALUE) {
			nret++;
		} else {
			lua_pop(L, 1);
		}
	} else {
		if (flags & MEMCACHED_VALUE) {
			if (flags & MEMCACHED_VALUE_BUFFER) {
				b = lua_newuserdata(L, sizeof(memcached_buffer_t));
				memset(b, 0, sizeof(memcached_buffer_t));
				luaL_getmetatable(L, MEMCACHED_BUFFER_METATABLE);
				lua_setmetatable(L, -2);
			} else {
				lua_pushliteral(L, "");
			}
			nret++;
		}
	}

	return nret;
}

static int get (lua_State *L) {
	int                          nret;
	size_t                       keylen;
	uint16_t                     status;
	uint64_t                     cas;
	const char                  *key;
	memcached_t                 *m;
	struct iovec                 iov[2];
	protocol_binary_request_get  request;

	/* check arguments */
	m = luaL_checkudata(L, 1, MEMCACHED_METATABLE);
	key = luaL_checklstring(L, 2, &keylen);
	luaL_argcheck(L, keylen > 0 && keylen <= UINT16_MAX, 2, "bad key length");

	/* prepare request */
	memset(&request, 0, sizeof(request));
	request.message.header.request.magic = PROTOCOL_BINARY_REQ;
	request.message.header.request.opcode = PROTOCOL_BINARY_CMD_GET;
	request.message.header.request.extlen = MEMCACHED_REQUEST_GET_EXTRAS;
	request.message.header.request.keylen = htobe16((uint16_t)keylen);
	request.message.header.request.bodylen = htobe32((uint32_t)(MEMCACHED_REQUEST_GET_EXTRAS
			+ keylen));

	/* send request */
	getsocket(L, m);
	iov[0].iov_base = &request;
	iov[0].iov_len = sizeof(request.bytes);
	iov[1].iov_base = (void *)key;
	iov[1].iov_len = (uint16_t)keylen;
	sendmsgnosig(L, m, iov, 2);

	/* push decode function */
	lua_rawgeti(L, LUA_REGISTRYINDEX, m->decode_index);

	/* read response */
	nret = recvresponse(L, m, &status, &cas, MEMCACHED_VALUE | MEMCACHED_VALUE_BUFFER);
	switch (status) {
	case PROTOCOL_BINARY_RESPONSE_SUCCESS:
		if (nret != 1) {
			return luaL_error(L, "protocol error");
		}
		lua_call(L, 1, 1);
		lua_pushinteger(L, cas);
		return 2;

	case PROTOCOL_BINARY_RESPONSE_KEY_ENOENT:
		lua_pushnil(L);
		return 1;

	default:
		return luaL_error(L, "memcached error (%d)", (int)status);
	}
}

static int set (lua_State *L) {
	size_t                          keylen, valuelen;
	uint16_t                        status;
	uint64_t                        cas;
	const char                     *key, *value;
	lua_Integer                     expiration;
	memcached_t                    *m;
	struct iovec                    iov[3];
	memcached_buffer_t             *b;
	protocol_binary_request_set     srequest;
	protocol_binary_request_delete  drequest;

	/* check arguments */
	m = luaL_checkudata(L, 1, MEMCACHED_METATABLE);
	key = luaL_checklstring(L, 2, &keylen);
	luaL_argcheck(L, keylen > 0 && keylen <= UINT16_MAX, 2, "bad key length");
	if (lua_tointeger(L, lua_upvalueindex(1)) == PROTOCOL_BINARY_CMD_SET) {
		luaL_checkany(L, 3);
	} else {
		luaL_argcheck(L, !lua_isnoneornil(L, 3), 3, "value required");
	}
	expiration = luaL_optinteger(L, 4, 0);
	luaL_argcheck(L, expiration >= 0 && expiration <= UINT32_MAX, 4, "bad expiration");
	cas = luaL_optinteger(L, 5, 0);

	/* handle both set and delete */
	if (!lua_isnil(L, 3)) {
		/* encode */
		lua_rawgeti(L, LUA_REGISTRYINDEX, m->encode_index);
		lua_pushvalue(L, 3);
		lua_call(L, 1, 1);
		b = luaL_testudata(L, -1, MEMCACHED_BUFFER_METATABLE);
		if (b) {
			value = b->b;
			valuelen = b->pos;
		} else if (lua_isstring(L, -1)) {
			value = lua_tolstring(L, -1, &valuelen);
		} else {
			return luaL_error(L, "encoder must return buffer or string");
		}
		if (valuelen > UINT32_MAX - (MEMCACHED_REQUEST_SET_EXTRAS + keylen)) {
			return luaL_error(L, "encoded value too long");
		}

		/* prepare request */
		memset(&srequest, 0, sizeof(srequest));
		srequest.message.header.request.magic = PROTOCOL_BINARY_REQ;
		srequest.message.header.request.opcode = (uint8_t)lua_tointeger(L, lua_upvalueindex(1));
		srequest.message.header.request.extlen = MEMCACHED_REQUEST_SET_EXTRAS;
		srequest.message.header.request.keylen = htobe16((uint16_t)keylen);
		srequest.message.header.request.bodylen = htobe32((uint32_t)(MEMCACHED_REQUEST_SET_EXTRAS
				+ keylen + valuelen));
		srequest.message.header.request.cas = htobe64(cas);
		srequest.message.body.expiration = htobe32((uint32_t)expiration);

		/* send request */
		getsocket(L, m);
		iov[0].iov_base = &srequest;
		iov[0].iov_len = sizeof(srequest.bytes);
		iov[1].iov_base = (void *)key;
		iov[1].iov_len = (uint16_t)keylen;
		iov[2].iov_base = (void *)value;
		iov[2].iov_len = valuelen;
		sendmsgnosig(L, m, iov, 3);
	} else {
		/* prepare request */
		memset(&drequest, 0, sizeof(drequest));
		drequest.message.header.request.magic = PROTOCOL_BINARY_REQ;
		drequest.message.header.request.opcode = PROTOCOL_BINARY_CMD_DELETE;
		drequest.message.header.request.extlen = MEMCACHED_REQUEST_DELETE_EXTRAS;
		drequest.message.header.request.keylen = htobe16(keylen);
		drequest.message.header.request.bodylen = htobe32((uint32_t)(MEMCACHED_REQUEST_DELETE_EXTRAS
				+ keylen));
		drequest.message.header.request.cas = htobe64(cas);

		/* send request */
		getsocket(L, m);
		iov[0].iov_base = &drequest;
		iov[0].iov_len = sizeof(drequest.bytes);
		iov[1].iov_base = (void *)key;
		iov[1].iov_len = (uint16_t)keylen;
		sendmsgnosig(L, m, iov, 2);
	}

	/* read response */
	recvresponse(L, m, &status, &cas, 0);
	switch (status) {
	case PROTOCOL_BINARY_RESPONSE_SUCCESS:
		lua_pushboolean(L, 1);
		lua_pushinteger(L, cas);
		return 2;

	case PROTOCOL_BINARY_RESPONSE_KEY_ENOENT:
	case PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS:
		lua_pushboolean(L, 0);
		return 1;

	default:
		return luaL_error(L, "memcached error (%d)", (int)status);
	}
}

static int incr (lua_State *L) {
	int                           nret;
	size_t                        keylen;
	uint16_t                      status;
	uint64_t                      value;
	const char                   *key;
	lua_Integer                   delta, initial, expiration;
	memcached_t                  *m;
	struct iovec                  iov[2];
	protocol_binary_request_incr  request;

	/* check arguments */
	m = luaL_checkudata(L, 1, MEMCACHED_METATABLE);
	key = luaL_checklstring(L, 2, &keylen);
	luaL_argcheck(L, keylen > 0 && keylen <= UINT16_MAX, 2, "bad key length");
	delta = luaL_optinteger(L, 3, 1);
	luaL_argcheck(L, delta >= 0 && delta <= INT64_MAX, 3, "bad delta");
	initial = luaL_optinteger(L, 4, 1);
	luaL_argcheck(L, initial >= 0 && initial <= INT64_MAX, 4, "bad initial value");
	expiration = luaL_optinteger(L, 5, 0);
	luaL_argcheck(L, expiration >= 0 && expiration <= UINT32_MAX, 5, "bad expiration");

	/* prepare request */
	memset(&request, 0, sizeof(request));
	request.message.header.request.magic = PROTOCOL_BINARY_REQ;
	request.message.header.request.opcode = (uint8_t)lua_tointeger(L, lua_upvalueindex(1));
	request.message.header.request.extlen = MEMCACHED_REQUEST_INCR_EXTRAS;
	request.message.header.request.keylen = htobe16((uint16_t)keylen);
	request.message.header.request.bodylen = htobe32((uint32_t)(MEMCACHED_REQUEST_INCR_EXTRAS
			+ keylen));
	request.message.body.delta = htobe64((uint64_t)delta);
	request.message.body.initial = htobe64((uint64_t)initial);
	request.message.body.expiration = htobe32((uint32_t)expiration);

	/* send request */
	getsocket(L, m);
	iov[0].iov_base = &request;
	iov[0].iov_len = sizeof(request.bytes);
	iov[1].iov_base = (void *)key;
	iov[1].iov_len = (uint16_t)keylen;
	sendmsgnosig(L, m, iov, 2);

	/* read response */
	nret = recvresponse(L, m, &status, NULL, MEMCACHED_VALUE);
	switch (status) {
	case PROTOCOL_BINARY_RESPONSE_SUCCESS:
		if (nret != 1) {
			return luaL_error(L, "protocol error");
		}
		memcpy(&value, lua_tostring(L, -1), sizeof(value));
		lua_pushinteger(L, be64toh(value));
		return 1;

	case PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL:
		lua_pushnil(L);
		return 1;

	default:
		return luaL_error(L, "memcached error (%d)", (int)status);
	}
}

static int flush (lua_State *L) {
	uint16_t                       status;
	lua_Integer                    expiration;
	memcached_t                   *m;
	protocol_binary_request_flush  request;

	/* check arguments */
	m = luaL_checkudata(L, 1, MEMCACHED_METATABLE);
	expiration = luaL_optinteger(L, 2, 0);
	luaL_argcheck(L, expiration >= 0 && expiration <= UINT32_MAX, 2, "bad expiration");

	/* prepare request */
	memset(&request, 0, sizeof(request));
	request.message.header.request.magic = PROTOCOL_BINARY_REQ;
	request.message.header.request.opcode = PROTOCOL_BINARY_CMD_FLUSH;
	request.message.header.request.extlen = MEMCACHED_REQUEST_FLUSH_EXTRAS;
	request.message.header.request.bodylen = htobe32((uint32_t)MEMCACHED_REQUEST_FLUSH_EXTRAS);
	request.message.body.expiration = htobe32((uint32_t)expiration);

	/* send request */
	getsocket(L, m);
	sendnosig(L, m, &request, sizeof(request.bytes));

	/* read response */
	recvresponse(L, m, &status, NULL, 0);
	switch (status) {
	case PROTOCOL_BINARY_RESPONSE_SUCCESS:
		return 0;

	default:
		return luaL_error(L, "memcached error (%d)", (int)status);
	}
}

static int stats (lua_State *L) {
	int                            nret;
	size_t                         keylen;
	uint16_t                       status;
	const char                    *key;
	memcached_t                   *m;
	struct iovec                   iov[2];
	protocol_binary_request_stats  request;

	/* check arguments */
	m = luaL_checkudata(L, 1, MEMCACHED_METATABLE);
	key = luaL_optlstring(L, 2, NULL, &keylen);
	luaL_argcheck(L, !key || (keylen > 0 && keylen <= UINT16_MAX), 2, "bad key length");

	/* prepare request */
	memset(&request, 0, sizeof(request));
	request.message.header.request.magic = PROTOCOL_BINARY_REQ;
	request.message.header.request.opcode = PROTOCOL_BINARY_CMD_STAT;
	request.message.header.request.extlen = MEMCACHED_REQUEST_STATS_EXTRAS;
	request.message.header.request.keylen = htobe16((uint16_t)keylen);
	request.message.header.request.bodylen = htobe32((uint32_t)(MEMCACHED_REQUEST_STATS_EXTRAS
			+ keylen));

	/* send request */
	getsocket(L, m);
	iov[0].iov_base = &request;
	iov[0].iov_len = sizeof(request.bytes);
	iov[1].iov_base = (void *)key;
	iov[1].iov_len = (uint16_t)keylen;
	sendmsgnosig(L, m, iov, key ? 2 : 1);

	/* read response */
	lua_newtable(L);
	while (1) {
		nret = recvresponse(L, m, &status, NULL, MEMCACHED_KEY | MEMCACHED_VALUE);
		switch (status) {
		case PROTOCOL_BINARY_RESPONSE_SUCCESS:
			switch (nret) {
			case 1:
				lua_pop(L, 1);  /* pop empty value */
				return 1;
			
			case 2:
				lua_rawset(L, -3);
				break;

			default:
				return luaL_error(L, "protocol error");
			}
			break;

		default:
			return luaL_error(L, "memcached error (%d)", (int)status);
		}
	}
}

static int quit (lua_State *L) {
	memcached_t                   *m;
	protocol_binary_request_quit  request;

	/* check arguments */
	m = luaL_checkudata(L, 1, MEMCACHED_METATABLE);

	/* prepare request */
	memset(&request, 0, sizeof(request));
	request.message.header.request.magic = PROTOCOL_BINARY_REQ;
	request.message.header.request.opcode = PROTOCOL_BINARY_CMD_QUITQ;

	/* send request */
	sendnosig(L, m, &request, sizeof(request.bytes));

	return 0;
}

static int mclose (lua_State *L) {
	memcached_t  *m;

	m = luaL_checkudata(L, 1, MEMCACHED_METATABLE);
	m->closed = 1;
	if (m->host_index != LUA_NOREF) {
		luaL_unref(L, LUA_REGISTRYINDEX, m->host_index);
		m->host_index = LUA_NOREF;
	}
	if (m->port_index != LUA_NOREF) {
		luaL_unref(L, LUA_REGISTRYINDEX, m->port_index);
		m->port_index = LUA_NOREF;
	}
	if (m->encode_index != LUA_NOREF) {
		luaL_unref(L, LUA_REGISTRYINDEX, m->encode_index);
		m->encode_index = LUA_NOREF;
	}
	if (m->decode_index != LUA_NOREF) {
		luaL_unref(L, LUA_REGISTRYINDEX, m->decode_index);
		m->decode_index = LUA_NOREF;
	}
	if (m->fd >= 0) {
		/* send quit command */
		lua_pushcfunction(L, quit);
		lua_pushvalue(L, 1);
		if (lua_pcall(L, 1, 0, 0) != LUA_OK) {
			/* ignore error, if any */
			lua_pop(L, 1);
		}

		/* close socket */
		close(m->fd);
		m->fd = -1;
	}
	return 0;
}

static int tostring (lua_State *L) {
	const char   *state;
	memcached_t  *m;

	m = luaL_checkudata(L, 1, MEMCACHED_METATABLE);
	if (m->closed) {
		state = "closed";
	} else if (m->fd < 0) {
		state = "disconnected";
	} else {
		state = "connected";
	}
	lua_pushfstring(L, MEMCACHED_METATABLE " [%s]: %p", state, m);
	return 1;
}


/*
 * exports
 */

int luaopen_memcached (lua_State *L) {
	/* register functions */
	luaL_newlib(L, functions);

	/* create buffer metatable */
	luaL_newmetatable(L, MEMCACHED_BUFFER_METATABLE);
	lua_pushcfunction(L, buffer_free);
	lua_setfield(L, -2, "__gc");
	lua_pushcfunction(L, buffer_tostring);
	lua_setfield(L, -2, "__tostring");
	lua_pop(L, 1);

	/* create metatable */
	luaL_newmetatable(L, MEMCACHED_METATABLE);
	lua_pushcfunction(L, mclose);
	lua_setfield(L, -2, "__gc");
	lua_pushcfunction(L, tostring);
	lua_setfield(L, -2, "__tostring");
	lua_newtable(L);
	lua_pushcfunction(L, get);
	lua_setfield(L, -2, "get");
	lua_pushinteger(L, PROTOCOL_BINARY_CMD_SET);
	lua_pushcclosure(L, set, 1);
	lua_setfield(L, -2, "set");
	lua_pushinteger(L, PROTOCOL_BINARY_CMD_ADD);
	lua_pushcclosure(L, set, 1);
	lua_setfield(L, -2, "add");
	lua_pushinteger(L, PROTOCOL_BINARY_CMD_REPLACE);
	lua_pushcclosure(L, set, 1);
	lua_setfield(L, -2, "replace");
	lua_pushinteger(L, PROTOCOL_BINARY_CMD_INCREMENT);
	lua_pushcclosure(L, incr, 1);
	lua_setfield(L, -2, "inc");
	lua_pushinteger(L, PROTOCOL_BINARY_CMD_DECREMENT);
	lua_pushcclosure(L, incr, 1);
	lua_setfield(L, -2, "dec");
	lua_pushcfunction(L, flush);
	lua_setfield(L, -2, "flush");
	lua_pushcfunction(L, stats);
	lua_setfield(L, -2, "stats");
	lua_pushcfunction(L, mclose);
	lua_setfield(L, -2, "close");
	lua_setfield(L, -2, "__index");
	lua_pop(L, 1);

	return 1;
}