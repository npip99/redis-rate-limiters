--- Lua scripts are run atomically by default, and since redis
--- is single threaded, there are no race conditions to worry about.
---
--- This script does three things, in order:
--- 1. Retrieves token bucket state, which means the last slot assigned,
---    and how many tokens are left to be assigned for that slot
--- 2. Works out whether we need to move to the next slot, or consume tokens
---    from the current one.
--- 3. Saves the token bucket state and returns the slot (or -1 if timeout exceeded).
---
--- The token bucket implementation is forward looking, so we're really just handing
--- out the next time there would be tokens in the bucket, and letting the client
---
--- returns:
--- * The assigned slot, as a millisecond timestamp, or -1 if request cannot be fulfilled
redis.replicate_commands()

-- Debug flag - set to false to disable all debug output
local DEBUG = false

-- Helper function for debug output
local function debug_echo(message)
    if DEBUG then
        redis.call('ECHO', message)
    end
end

-- Arguments
local capacity = tonumber(ARGV[1])
local refill_amount = tonumber(ARGV[2])
local time_between_slots = tonumber(ARGV[3]) * 1000 -- Convert to milliseconds
local seconds = tonumber(ARGV[4])
local microseconds = tonumber(ARGV[5])
local tokens_requested = tonumber(ARGV[6])
local max_timeout_ms = ARGV[7] and tonumber(ARGV[7]) or nil -- Optional parameter

-- Keys
local data_key = KEYS[1]

-- Get current time in milliseconds
local now = (tonumber(seconds) * 1000) + (tonumber(microseconds) / 1000)

-- Debug prints (using ECHO so they show in MONITOR)
debug_echo("=== TOKEN BUCKET DEBUG ===")
debug_echo("Input params: capacity=" .. capacity .. ", refill_amount=" .. refill_amount .. ", time_between_slots=" .. time_between_slots .. ", tokens_requested=" .. tokens_requested)
debug_echo("Current time: " .. now)

-- Check if request is impossible (more tokens than capacity)
if tokens_requested > capacity then
    debug_echo("Request impossible: tokens_requested > capacity")
    return -1
end

-- Default bucket values (used if no bucket exists yet)
local tokens = capacity
local slot = now

-- Retrieve stored state, if any
local data = redis.call('GET', data_key)
debug_echo("Retrieved data: " .. (data or "nil"))

if data then
    local last_slot, stored_tokens = data:match('(%S+) (%S+)')
    slot = tonumber(last_slot)
    tokens = tonumber(stored_tokens)

    debug_echo("Parsed state: last_slot=" .. slot .. ", stored_tokens=" .. tokens)

    -- Calculate the number of slots that have passed since the last update
    local slots_passed = math.floor((now - slot) / time_between_slots)
    debug_echo("Slots passed: " .. slots_passed)

    if slots_passed > 0 then
        -- Refill the tokens based on the number of slots passed, capped by capacity
        local new_tokens = math.min(tokens + slots_passed * refill_amount, capacity)
        debug_echo("Refilling: " .. tokens .. " + (" .. slots_passed .. " * " .. refill_amount .. ") = " .. new_tokens .. " (capped at " .. capacity .. ")")
        tokens = new_tokens
        -- Update the slot to this run, ensuring a minimum 20ms spacing since the previous slot
        local required_gap = 20
        local since_last = now - slot
        if since_last < required_gap then
            slot = now + (required_gap - since_last)
        else
            slot = now
        end
        debug_echo("Updated slot to: " .. slot)
    end
else
    debug_echo("No existing data, using defaults: tokens=" .. tokens .. ", slot=" .. slot)
end

debug_echo("Before token check: tokens=" .. tokens .. ", tokens_requested=" .. tokens_requested)

-- If we don't have enough tokens available, we need to wait
if tokens < tokens_requested then
    local tokens_needed = tokens_requested - tokens
    local slots_needed = math.ceil(tokens_needed / refill_amount)
    debug_echo("Need to wait: tokens_needed=" .. tokens_needed .. ", slots_needed=" .. slots_needed)

    -- Advance slot and add tokens for the required slots
    slot = slot + (slots_needed * time_between_slots)
    tokens = tokens + (slots_needed * refill_amount)
    debug_echo("After waiting calculation: slot=" .. slot .. ", tokens=" .. tokens)
else
    debug_echo("Sufficient tokens available, no waiting needed")
end

-- Check timeout: if max_timeout_ms is specified and we'd have to wait too long
if max_timeout_ms then
    local wait_time = slot - now
    debug_echo("Timeout check: wait_time=" .. wait_time .. ", max_timeout_ms=" .. max_timeout_ms)
    if wait_time > max_timeout_ms then
        debug_echo("Timeout exceeded: wait_time=" .. wait_time .. " > max_timeout_ms=" .. max_timeout_ms)
        return -1
    end
end

-- Consume the requested tokens
tokens = tokens - tokens_requested
debug_echo("After consuming tokens: " .. tokens)

-- Calculate appropriate expiry based on maximum possible wait time
-- Maximum slots needed would be if we have 0 tokens and need full capacity
local max_slots_needed = math.ceil(capacity / refill_amount)
local max_wait_milliseconds = max_slots_needed * time_between_slots
local max_wait_seconds = math.ceil(max_wait_milliseconds / 1000)
local expiry = math.max(30, max_wait_seconds + 10) -- At least 30 seconds, plus 10 second buffer

debug_echo("Expiry calculated: " .. expiry)

-- Save updated state and set expiry
redis.call('SETEX', data_key, expiry, string.format('%.1f %.1f', slot, tokens))
debug_echo("Saved state: slot=" .. slot .. ", tokens=" .. tokens)

-- Return the slot when the tokens will be available
debug_echo("Returning slot: " .. slot)
return slot
