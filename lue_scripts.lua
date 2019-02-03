-- SEND SCRIPT 
-- KEYS[1] = item id
-- ARGV[1] = item value
-- KEYS[2] = queue
local item_key = KEYS[2] .. ':' .. KEYS[1]
redis.call('set', KEYS[2] .. ':' .. KEYS[1], ARGV[1])
local pend_set = KEYS[2] .. ':pend'
local pend_score = pend_set .. ':score'
redis.call('incr', pend_score)
redis.call('zadd', pend_set, redis.call('get', pend_score), KEYS[1])
return item_key

-- RECV SCRIPT 
-- KEYS[1] = queue set
local pend_set = KEYS[1] .. ':pend'
local item = redis.call('zpopmin', pend_set, 1)
if table.getn(item) == 0 then
    return {0, nil}
end
local raw = redis.call('get', KEYS[1] .. ':' .. item[1])
if raw == nil then
    return redis.error_reply("Item Does Not Exist")
end
local recv_set = KEYS[1] .. ':recv'
local recv_set_score = KEYS[1] .. ':score'
redis.call('incr', recv_set_score)
redis.call('zadd', recv_set, redis.call('get', recv_set_score), item[1])
redis.call('zrem', pend_set, item[1])
return {1, raw}

-- CONSUME SCRIPT
-- KEY[1] = queue set
local pend_set = KEYS[1] .. ':pend'
local item = redis.call('zpopmin', pend_set, 1)
if table.getn(item) == 0 then
    return {0, nil}
end
local item_key = KEYS[1] ..':' .. item[1]
local raw = redis.call('get', item_key)
if raw == nil then
    return redis.error_reply("Item Does Not Exist")
end
redis.call('del', item_key)
return {1, raw}

-- SET RESULT SCRIPT
-- KEYS[1] = task id
-- ARGV[1] = task result value
-- KEYS[2] = queue id
local pend_set = KEYS[2] .. ':pend'
local recv_set = KEYS[2] .. ':recv'
local done_set = KEYS[2] .. ':done'
local task_key = KEYS[2] .. ':' .. KEYS[1]
local task_result_key = task_key .. ':result'
if redis.call('get', task_result_key) != nil then
    return redis.error_reply("Result already set")
else
    redis.call('set', task_result_key, ARV[1])
end
local done_set_score = done_set .. ':score'
redis.call('incr', done_set_score)
redis.call('zadd', done_set, redis.call('get', done_set_score), KEYS[1])
redis.call('zrem', pend_set, KEYS[1])
return redis.call('zrem', recv_set, KEYS[1])

-- CLOSE SCRIPT
-- KEYS[1] = pending queue id
local sets = {KEYS[1] .. ':pend', KEYS[1] .. ':recv', KEYS[1] .. ':done'}
for _, key in ipairs(sets) do
    local items = redis.call('zrange', key, 0, -1)
    if #items > 0 then
        for _, item in ipairs(items) do
            redis.call('del', KEYS[1] .. ':' .. item)
            redis.call('del', KEYS[1] .. ':' .. item .. ':result')
        end
    end
    redis.call('del', key)
    redis.call('del', key .. ':score')
end

-- GET RESULT SCRIPT
-- KEYS[1] = task id
-- KEYS[2] = queue id
local pend_set = KEYS[2] .. ':pend'
local recv_set = KEYS[2] .. ':recv'
local done_set = KEYS[2] .. ':done'
local task_key = KEYS[2] .. ':' .. KEYS[1]
local task_result_key = task_key .. ':result'
res = redis.call('get', task_result_key)
if res == nil then
    return nil
redis.call('zrem', pend_set, KEYS[1])
redis.call('zrem', recv_set, KEYS[1])
redis.call('zrem', done_set, KEYS[1])
redis.call('del', task_key)
redis.call('del', task_result_key)
return res



-- GET STATUS SCRIPT
-- KEYS[1] = item id
-- KEYS[2] = queue id
if redis.call('zscore', KEYS[2] .. ':pend') ~= nil then
    return 'pend'
if redis.call('zscore', KEYS[2] .. ':recv') ~= nil then
    return 'recv'
if redis.call('zscore', KEYS[2] .. ':score') ~= nil then
    return 'recv'
return nil


-- open channel
-- KEYS[1] = channel key
-- ARGV[1] = channel properties
local open_key = KEYS[1] .. ':open'
if redis.call('exists', open_key) == 0 then
    redis.call('set', open_key, ARGV[1])
    return {1, ARGV[1]}
end
return {0, redis.call('get', open_key)}

-- open queue
-- KEYS[1] = queue key
-- ARGV[1] = queue props
local open_key = KEYS[1] .. ':open'
if redis.call('exists', open_key) == 0 then
    redis.call('set', open_key, ARGV[1])
    return {1, ARGV[1]}
end
return {0, redis.call('get', open_key)}