-- Atomic Token Bucket implementation in Lua for Redis.
--
-- Why Lua?
-- Executing the read-replenish-decrement cycle inside a single Lua script
-- guarantees ACID atomicity within Redis's single-threaded event loop.
-- This prevents race conditions (lost updates / phantom tokens) when multiple
-- gateway instances concurrently evaluate rate limits for the same client.
--
-- KEYS[1]: Rate limit key (e.g. "ratelimit:<client_ip>")
-- ARGV[1]: Bucket capacity / burst limit (int)
-- ARGV[2]: Refill rate in tokens per second (float)
-- ARGV[3]: Current timestamp in milliseconds (int)
-- ARGV[4]: Requested tokens (int)
--
-- Returns: { allowed (1 or 0), remaining_tokens (int), retry_after_ms (int) }

local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

-- Retrieve current state from the Redis hash.
local data = redis.call('HMGET', key, 'tokens', 'last_updated_ms')
local tokens = tonumber(data[1])
local last_updated_ms = tonumber(data[2])

if tokens == nil or last_updated_ms == nil then
    -- Cold start for this client: initialize with maximum burst capacity.
    tokens = capacity
    last_updated_ms = now_ms
else
    -- Why floating point replenishment?
    -- Continuous time token replenishment: (elapsed_ms * refill_rate) calculates fractional tokens
    -- accrued since the last request without requiring periodic background timers.
    -- math.max(0, ...) guards against clock skew across distributed gateway nodes.
    local elapsed_ms = math.max(0, now_ms - last_updated_ms)
    local refilled = (elapsed_ms / 1000.0) * refill_rate
    tokens = math.min(capacity, tokens + refilled)
    last_updated_ms = now_ms
end

local allowed = 0
local retry_after_ms = 0

if tokens >= requested then
    allowed = 1
    tokens = tokens - requested
else
    allowed = 0
    -- Calculate precise wait time until at least the requested tokens are replenished.
    local missing = requested - tokens
    if refill_rate > 0 then
        retry_after_ms = math.ceil((missing / refill_rate) * 1000.0)
    else
        retry_after_ms = -1
    end
end

-- Persist the updated bucket state back into the hash.
-- Even on rejection, storing refilled tokens ensures accrued replenishment is not discarded.
redis.call('HSET', key, 'tokens', tostring(tokens), 'last_updated_ms', tostring(last_updated_ms))

-- Why dynamic TTL?
-- Inactive client keys must be reclaimed to prevent unbounded Redis memory growth.
-- We retain the key for twice the time needed to fully refill the bucket from zero,
-- with a 60-second floor. This ensures state is retained during typical active windows
-- while automatically purging dead keys.
local ttl = math.max(60, math.ceil((capacity / refill_rate) * 2))
redis.call('EXPIRE', key, ttl)

return { allowed, math.floor(tokens), retry_after_ms }
