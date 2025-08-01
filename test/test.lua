local memcached = require("memcached")

math.randomseed(os.time())
local PREFIX = tostring("test-" .. math.floor(math.random() * math.maxinteger))

local function equals (a, b)
	if type(a) ~= type(b) then
		return false
	end
	if type(a) == "table" then
		for k, v in pairs(a) do
			if not equals(v, b[k]) then
				return false
			end
		end
		for k, v in pairs(b) do
			if not equals(v, a[k]) then
				return false
			end
		end
		return true
	else
		return a == b or a ~= a and b ~= b
	end
end

local function testCodec ()
	local function encodeAndDecode (value)
		local encoded = memcached.encode(value)
		assert(type(encoded) == "userdata")
		assert(string.len(tostring(encoded)) > 0)
		local decoded = memcached.decode(encoded)
		assert(equals(decoded, value))
	end
	encodeAndDecode(true)
	encodeAndDecode(false)
	encodeAndDecode(1)
	encodeAndDecode(0)
	encodeAndDecode(1.5)
	encodeAndDecode(0.5)
	encodeAndDecode("test")
	encodeAndDecode(string.rep("test ", 20000))
	local t1 = { 1, 2, 3 }
	encodeAndDecode(t1)
	local t2 = { a = 1, b = 2, c = 3 }
	encodeAndDecode(t2)
	local t3 = { 1, 2, 3, a = 4, b = 5 }
	encodeAndDecode(t3)
	local t4 = { a = 1, b = 2, c = { d = 3, e = 4 } }
	encodeAndDecode(t4)
	local t5 = { }
	for i = 1, 2^8 do
		t5[i] = { u = math.random(), v = "test16" }
	end
	encodeAndDecode(t5)
	local t6 = { }
	for i = 1, 2^16 do
		t6[i] = { u = math.random(), v = "test32" }
	end
	encodeAndDecode(t6)
	local t7, t8 = { a = 1 }, { b = 2, f = print }
	t7.other, t8.other = t8, t7
	local encoded = memcached.encode(t7)
	local decoded = memcached.decode(encoded)
	assert(type(decoded) == "table")
	assert(decoded.a == 1)
	assert(type(decoded.other) == "table")
	assert(decoded.other.b == 2)
	assert(decoded.other.other == decoded)
	assert(decoded.other.f == nil)
end

local function testOpenClose ()
    local client = memcached.open()
	assert(client)
	local status = "%[(%w+)%]"
	assert(string.match(tostring(client), status) == "disconnected")
	local key = PREFIX .. "-test-open-close"
	client:get(key)
	assert(string.match(tostring(client), status) == "connected")
	client:close()
	assert(string.match(tostring(client), status) == "closed")

	-- Configured client
	local encoded, decoded
	client = memcached.open({
		host = "localhost",
		port = 11211,
		timeout = 1000,
		reconnect = true,
		encode = function (_value)
			encoded = true
			return tostring(_value)
		end,
		decode = function (encoding)
			decoded = true
			return tostring(encoding)
		end,
	})
	assert(client)
	local value = "test-value"
	assert(client:set(key, value))
	assert(encoded)
	local result = client:get(key)
	assert(decoded)
	assert(result == value)
	client:close()
end

local function testSetGet ()
	local client = memcached.open()
	assert(client)
	local key = PREFIX .. "-test-set-get"
	local value = "test-value"
	assert(client:set(key, value))
	local result = client:get(key)
	assert(result == value)
	assert(client:set(key, nil))
	result = client:get(key)
	assert(result == nil)

	-- 'Large' data
	value = { }
	for i = 1, 10000 do
		value[i] = { u = math.random(), v = "test" }
	end
	assert(client:set(key, value))
	result = client:get(key)
	assert(equals(result, value))

	-- Special case: empty value at protocol level
	client:close()
	client = memcached.open({
		encode = function (_value)
			return tostring(_value)
		end,
		decode = function (encoding)
			return tostring(encoding)
		end,
	})
	value = ""
	assert(client:set(key, value))
	result = client:get(key)
	assert(result == value)
	client:close()
end

local function testExpiration ()
	local client = memcached.open()
	assert(client)
	local key = PREFIX .. "-test-expiration"
	local value = "test-value"
	assert(client:set(key, value, 1))
	local result = client:get(key)
	assert(result == value)

	-- Wait for expiration
	os.execute("sleep 1.1")
	result = client:get(key)
	assert(result == nil)

	client:close()
end

local function testCas ()
	local client = memcached.open()
	assert(client)
	local key = PREFIX .. "-test-cas"
	local value = "test-value"
	local success, cas = client:set(key, value)
	assert(success)
	assert(math.type(cas) == "integer")

	-- Get with CAS
	local result, casValue = client:get(key)
	assert(result == value)
	assert(casValue == cas)

	-- Test CAS success
	local newValue = "new-test-value"
	success, cas = client:set(key, newValue, nil, cas)
	assert(success)
	assert(cas ~= casValue)

	-- Test CAS failure
	local failedValue = "failed-test-value"
	success, cas = client:set(key, failedValue, nil, casValue)
	assert(not success)

	client:close()
end

local function testAddReplace ()
	local client = memcached.open()
	assert(client)

	-- Test add
	local key = PREFIX .. "-test-add-replace"
	local value = "test-value"
	assert(client:add(key, value))
	local result = client:get(key)
	assert(result == value)

	-- Test replace
	local newValue = "new-test-value"
	assert(client:replace(key, newValue))
	result = client:get(key)
	assert(result == newValue)

	-- Test add failure
	assert(not client:add(key, "should-fail"))
	result = client:get(key)
	assert(result == newValue)

	-- Test replace failure
	assert(not client:replace(PREFIX .. "-nonexistent", "should-fail"))

	client:close()
end

function testIncDec ()
	local client = memcached.open()
	assert(client)

	-- Increment
	local key = PREFIX .. "-test-inc-dec"
	local initial = 10
	local incValue = 5
	local newValue = client:inc(key, incValue, initial)
	assert(newValue == initial)
	newValue = client:inc(key, incValue)
	assert(newValue == initial + incValue)

	-- Decrement
	newValue = client:dec(key, incValue)
	assert(newValue == initial + incValue - incValue)

        -- Incompatible types
        client:set(key, "not-a-number")
        assert(client:inc(key, incValue) == nil)
        assert(client:dec(key, incValue) == nil)

	client:close()
end

function testFlush ()
	local client = memcached.open()
	assert(client)
	local key = PREFIX .. "-test-flush"
	local value = "test-value"
	assert(client:set(key, value))
	local result = client:get(key)
	assert(result == value)

	-- Flush the cache immediately
	client:flush()
	result = client:get(key)
	assert(result == nil)

	-- Flush the cache with a delay
	assert(client:set(key, value))
	client:flush(2)  -- 1 would be immediate, as the server subtracts 1 second
	result = client:get(key)
	assert(result == value)
	os.execute("sleep 1.1")
	result = client:get(key)
	assert(result == nil)
	client:close()
end

function testStats ()
	local client = memcached.open()
	assert(client)

	-- General stats
	local stats = client:stats()
	assert(type(stats) == "table")
	assert(stats.uptime)
	assert(stats.version)
	assert(stats.pid)

	-- Specific stats
	stats = client:stats("settings")
	assert(type(stats) == "table")
	assert(stats.maxbytes)
	assert(stats.item_size_max)
	assert(stats.evictions)

	client:close()
end

-- Run tests
testCodec()
testOpenClose()
testSetGet()
testExpiration()
testCas()
testAddReplace()
testIncDec()
if os.getenv("MEMCACHED_TEST_FLUSH") then
	testFlush()  -- only run flush test if environment variable is set
end
testStats()