""" Provides RdQ object for creating simple FIFO queues with Redis. """
import json
import pickle
import threading
import time
import uuid
from datetime import timedelta
from hashlib import sha1
from typing import Any, List, Optional, Tuple, Union

import msgpack
import redis_lock
from redis import StrictRedis
from redis.exceptions import NoScriptError, ResponseError

# Shared constants
OPEN_APPEND = ':open'
SCORE_APPEND = ':score'

# Task constants
TASK_PEND = 0
TASK_RECV = 1
TASK_DONE = 2

# Q constants
Q_PEND_SET = ':pend'
Q_RECV_SET = ':recv'
Q_DONE_SET = ':done'
Q_RESULT_APPEND = ':result'


class RedisScriptError(Exception):
    """ Represents an error with a ``RedisScript`` instance. """


class RedisScript:
    """ Represents a LUA Redis Script that can be executed. """
    def __init__(self, script: str, keys_required: int = 0, name: str = None):
        self._script_string = script
        self._script_encoded = script.encode()
        self._script_digest = sha1(self._script_encoded).hexdigest()
        self._name = name or self._script_string[0:6] + "..."
        self._num_keys = keys_required
    
    @property
    def name(self):
        """ Getter for name protected property. """
        return self._name

    def eval(self, redis: StrictRedis, *keys, **kwargs):
        """Tries to call ``EVALSHA`` with the `hash` and then, if it fails, calls
        regular ``EVAL`` with.
        """
        args = kwargs.pop('args', ())
        # make sure number of keys passed matches keys required
        if len(keys) != self._num_keys:
            raise RedisScriptError(f"Key length mismatch. Expected {self._num_keys}. "
                                   f"Got {len(keys)} in {self._name}")

        if kwargs:
            raise TypeError(f"Unexpected keyword arguments {kwargs.keys()} in {self._name}.")
        try:
            return redis.evalsha(self._script_digest, self._num_keys, *keys + args)
        except NoScriptError:
            return redis.eval(self._script_encoded, self._num_keys, *keys + args)

# Q LUA scripts

Q_OPEN = RedisScript(f"""
    -- open queue
    -- KEYS[1] = queue key
    -- ARGV[1] = queue props
    local open_key = KEYS[1] .. '{OPEN_APPEND}'
    if redis.call('exists', open_key) == 0 then
        redis.call('set', open_key, ARGV[1])
        return {{1, ARGV[1]}}
    end
    return {{0, redis.call('get', open_key)}}
""", 1, 'Q_OPEN')

Q_SEND = RedisScript(f"""
    -- KEYS[1] = item id
    -- ARGV[1] = item value
    -- KEYS[2] = queue id
    local item_key = KEYS[2] .. ':' .. KEYS[1]
    redis.call('set', item_key, ARGV[1])
    local pend_set = KEYS[2] .. '{Q_PEND_SET}'
    local pend_score = pend_set .. '{SCORE_APPEND}'
    redis.call('incr', pend_score)
    redis.call('zadd', pend_set, redis.call('get', pend_score), KEYS[1])
    return item_key
""", 2, 'Q_SEND')

Q_RECV = RedisScript(f"""
    -- KEYS[1] = queue set
    local pend_set = KEYS[1] .. '{Q_PEND_SET}'
    local item = redis.call('zpopmin', pend_set, 1)
    if table.getn(item) == 0 then
        return {{0, nil, nil}}
    end
    local raw = redis.call('get', KEYS[1] .. ':' .. item[1])
    if raw == nil then
        return redis.error_reply("Item Does Not Exist")
    end
    local recv_set = KEYS[1] .. '{Q_RECV_SET}'
    local recv_set_score = KEYS[1] .. '{SCORE_APPEND}'
    redis.call('incr', recv_set_score)
    redis.call('zadd', recv_set, redis.call('get', recv_set_score), item[1])
    return {{1, item[1], raw}}
""", 1, 'Q_RECV')

Q_SET = RedisScript(f"""
    -- KEYS[1] = task id
    -- ARGV[1] = task result value
    -- KEYS[2] = queue id
    local recv_set = KEYS[2] .. '{Q_RECV_SET}'
    local done_set = KEYS[2] .. '{Q_DONE_SET}'
    local task_key = KEYS[2] .. ':' .. KEYS[1]
    local task_result_key = task_key .. '{Q_RESULT_APPEND}'
    if redis.call('get', task_result_key) ~= false then
        return redis.error_reply("Result already set")
    else
        redis.call('set', task_result_key, ARGV[1])
    end
    local done_set_score = done_set .. '{SCORE_APPEND}'
    redis.call('incr', done_set_score)
    redis.call('zadd', done_set, redis.call('get', done_set_score), KEYS[1])
    return redis.call('zrem', recv_set, KEYS[1])
""", 2, 'Q_SET')

Q_GET = RedisScript(f"""
    -- KEYS[1] = task id
    -- KEYS[2] = queue id
    local pend_set = KEYS[2] .. '{Q_PEND_SET}'
    local recv_set = KEYS[2] .. '{Q_RECV_SET}'
    local done_set = KEYS[2] .. '{Q_DONE_SET}'
    local task_key = KEYS[2] .. ':' .. KEYS[1]
    local task_result_key = task_key .. '{Q_RESULT_APPEND}'
    if redis.call('exists', task_result_key) == 0 then
        return nil
    end
    local raw = redis.call('get', task_result_key)
    redis.call('zrem', pend_set, KEYS[1])
    redis.call('zrem', recv_set, KEYS[1])
    redis.call('zrem', done_set, KEYS[1])
    redis.call('del', task_key)
    redis.call('del', task_result_key)
    return raw
""", 2, 'Q_GET')

Q_CLOSE = RedisScript(f"""
    -- KEYS[1] = queue id
    local count = 0
    local sets = {{KEYS[1] .. '{Q_PEND_SET}', KEYS[1] .. '{Q_RECV_SET}', KEYS[1] .. '{Q_DONE_SET}'}}
    for _, key in ipairs(sets) do
        local items = redis.call('zrange', key, 0, -1)
        if #items > 0 then
            count = count + #items
            for _, item in ipairs(items) do
                redis.call('del', KEYS[1] .. ':' .. item)
                redis.call('del', KEYS[1] .. ':' .. item .. '{Q_RESULT_APPEND}')
            end
        end
        redis.call('del', key)
        redis.call('del', key .. '{SCORE_APPEND}')
    end
    redis.call('del', KEYS[1] .. '{OPEN_APPEND}')
    return count
""", 1, 'Q_CLOSE')

Q_STATUS = RedisScript(f"""
    -- KEYS[1] = item id
    -- KEYS[2] = queue id
    if redis.call('zscore', KEYS[2] .. '{Q_PEND_SET}', KEYS[1]) ~= false then
        return {TASK_PEND}
    end
    if redis.call('zscore', KEYS[2] .. '{Q_RECV_SET}', KEYS[1]) ~= false then
        return {TASK_RECV}
    end
    if redis.call('zscore', KEYS[2] .. '{Q_DONE_SET}', KEYS[1]) ~= false then
        return {TASK_DONE}
    end
    return nil
""", 2, 'Q_STATUS')


class Qid:
    """ Represents a channel identifier. """
    def __init__(self, key: str, buffer: int = None, serializer: str = None):
        self._key = key
        self._buffer = buffer
        self._serializer = serializer
        self.opened: float = None

    @property
    def key(self):
        """ Getter for the ``Quid`` id key. """
        return self._key

    @property
    def buffer(self):
        """ Getter for the ``Qid`` buffer. """
        return self._buffer

    @property
    def serializer(self):
        """ Getter for the ``Qid`` id serializer. """
        return self._serializer

    def get_q(self, redis: StrictRedis):
        """ Get a queue object with this ID for a given ``redis`` connection. """
        if (self._buffer is None or self._serializer is None):
            res = redis.get(self._key + OPEN_APPEND)
            if res:
                # Q is open, so apply the existing qid props to this obj.
                qid = pickle.loads(res)
                self._buffer = qid.buffer
                self._serializer = qid.serializer
                self.opened = qid.opened
        
        if self._buffer is not None and self._serializer is not None:
            return Q(redis, self._key, buffer=self._buffer, serializer=self._serializer,
                     opened=self.opened)

        return None


class TaskIsReadOnly(Exception):
    """ Raised when ``Task`` in read mode attempts to write. """


class TaskIsWriteOnly(Exception):
    """ Raised when a ``Task`` in write mode attempts to read. """


class TaskResultAlreadySet(Exception):
    """ Raised when a ``Task`` result was already set when attempting to set the value. """


class TaskNotFound(Exception):
    """ Raised when a ``Task`` was not found """


class Task:
    """ ``Task`` represents a task to return a value from an async operator.
    
    A ``Task`` object is returned by ``Q`` whenever an item is sent through the queue.

    to wait for a promsie to be ready either use ``get_result`` with blocking = true
    """

    def __init__(self, queue: 'Q', task_id: str = None, item: Any = None):
        """ Create a new ``Task`` object.

        Args:
            queue: the Q object attached to this task
            item: the item in this task that is being passed to the 'worker'
            task_id: if known, set the task id
        """
        # the id is auto generated as a uuid4 str if one was not passed on init
        self._id = task_id or str(uuid.uuid4())
        self._result = None
        self._item = item
        self._queue = queue
        self._completed = False

    @property
    def id(self):
        """ Accessor for the immutable task id. """
        return self._id

    @property
    def result(self):
        """ Accessor for the immutable result property. """
        return self._result

    @property
    def item(self):
        """ Getter for protected property _item. """
        return self._item

    def get_status(self):
        """ Get the status for this task. """
        # if known to be complete then return complete
        return self._completed or self._queue.get_status(self._id)

    def is_ready(self):
        """ Check if task is ready. """
        return self.get_status() == TASK_DONE
    
    def get_result(self, blocking: bool = False, timeout: int = -1):
        """ Get the result for this task.
        Block if ``blocking`` is ``True`` for ``timeout`` seconds.
        If ``timeout`` is < ``0`` then block forever.

        Raises:
            TaskIsWriteOnly: When writer attempts to get the result

        Returns:
            A ``tuple`` equal to (``status``, ``value``)
        """

        if self._completed:
            # if already completed, return stored result
            return self._result

        # get the result status and value
        value = self._queue.get_result(self._id, blocking, timeout)
        
        # if status is done, then set the result and mark completed
        if value:
            if self.get_status() == TASK_DONE:
                self._result = value
                self._completed = True

        return value

    def set_result(self, value):
        """ Set the result of the task. """
        self._queue.set_result(self._id, value)


class QTimeoutError(Exception):
    """ Raised when ``Q`` times out trying to send or recieve an item from the queue. """


class QNotOpenError(Exception):
    """ Raised when ``Q`` event cannot be completed because queue is not open. """


class QLockTimeout(Exception):
    """ Raised when attempting to acquire a ``Q`` lock times out. """


class QValueCannotBeNone(Exception):
    """ Raised when value is being set to a None/Null value. """


class Q:
    """ Q is a simple Redis Q class for sending and recieving data over redis. """

    def __init__(self, conn: StrictRedis, namespace: str = None,
                 buffer: int = 1, serializer: str = 'pickle', opened: float = None):
        """ Initialize a ``Q`` object.
        
        Args:
            task: the task of this queue, prepended to keysself.
            conn: a StrictRedis connection object
            namespace: the namespace that this queue belongs to
             how many items can be in the buffer at the same time. Default is 1
            serializer: the serializer to use for items in this queue. Can be 'pickele',
             'json', or 'msgpack'. Defaults to 'pickle'.
            opened: timestamp for when channel is opened.
        
        Returns:
            An Q object.
        """
        self._conn: StrictRedis = conn
        # task by name if task string provided, else create a random task with uuidv4
        self._buffer = buffer
        # key for this quueue, create a random one if one doesn't exist already
        self._key = namespace or f"{__name__}:{self.__class__.__name__}:{uuid.uuid4().__str__()}"
        self._serializer: str = None
        # set the serializer
        self._set_serializer(serializer)
        # the queue key
        self._okey = self._key + OPEN_APPEND
        # keep alive thread
        self._keep_alive_thread: threading.Thread = None
        # the threading stop event
        self._stop_thread: threading.Event = None
        # set the ID
        self._id = Qid(self._key, self._buffer, self._serializer)
        self._id.opened = opened

    @property
    def _properties(self):
        """ Getter for properties for this ``Q``. """
        return (self._buffer, self._serializer)

    @property
    def id(self):
        """ Getter for this ``Q`` id object. """
        return self._id

    @property
    def opened(self):
        """ Getter for this ``Q`` id object. """
        return self._id.opened

    def _task_key(self, task_id):
        return self._key + ":" + task_id

    def _result_key(self, task_id):
        return self._key + ":" + task_id + Q_RESULT_APPEND

    def _set_serializer(self, serializer: str):
        """ Set the serializer for this ``Q``. """
        if serializer.lower() == 'msgpack':
            self._serialize = msgpack.dumps
            self._unserialize = msgpack.loads
            self._serializer = 'msgpack'
        elif serializer.lower() == 'json':
            self._serialize = json.dumps
            self._unserialize = json.loads
            self._serializer = 'json'
        else:
            self._serialize = pickle.dumps
            self._unserialize = pickle.loads
            self._serializer = 'pickle'

    def open(self) -> str:
        """ Open this queue. """
        self.__start_keep_alive_daemon()
        return self._key

    def is_open(self):
        """ Test if queue is still open. """
        return self._conn.exists(self._okey)

    def close(self):
        """ Close the queue.
        
        Raises:
            redlock.RedLockError when lock attempt fails
        """
        self._stop_thread.set()
        self._keep_alive_thread.join()

    def send(self, *items, timeout: int = -1) -> Union[List[Task], Task]:
        """ Send ``items`` through the queue.
        If queue is at max length block until ``timeout`` expires,
        or if ``timeout`` is < ``0``, block indefinitely.
        
        Args:
            items: a tuple of one or more items to push onto the fifo queue
            timeout: an optional timeout int
            
        Raises:
            QNotOpenError: if queue isn't open yet
            QTimeoutError: if timeout is hit

        Returns:
            Either a Task object if one item was sent, otherwise a list of Tasks for each item.
        """
        start = time.time()
        tasks = []
        # loop over each item provided and rpush them onto the queue
        for item in items:
            # after each item is inserted we reset the timeout
            while self._conn.zcount(self._key, 0, -1) >= self._buffer and self.is_open():
                if time.time() - start > timeout > 0:
                    raise QTimeoutError("Send time out.")
                time.sleep(0.01)
            if not self.is_open():
                raise QNotOpenError(f"Q '{self._key}' is no longer open")
            # create a new task reader
            task = Task(self)
            # create an id for this item
            tasks.append(task)
            # place the actual item in a tuple with the item id before pushing onto redis list
            Q_SEND.eval(self._conn, task.id, self._key, args=(self._serialize(item),))
        # return a list of item ids if multiple items were provided, else just the one id
        return tasks if len(tasks) > 1 else tasks[0]

    def recv(self, blocking: bool = False, timeout: int = -1) -> 'Task':
        """ Recv an item in the queue.
        Block until ``timeout`` expires, or if ``timeout`` is < ``0``, block indefinitely.

        Args:
            blocking: whether or not to block until an item is available in the queue to recieve
        
        Returns:
            A tuple containing unserialized value from the redis list if a value was found or
            (None, None) if nothing is in the queue currently, but queue is still openself or
            False if the queue is now closed.
        """
        if blocking:
            start = time.time()
            # if we're blocking then probe every 1 microsecd until we have an item
            while self.is_open():
                res = Q_RECV.eval(self._conn, self._key)
                if res[0] == 1:
                    break
                if time.time() - start > timeout > 0:
                    raise QTimeoutError(f"{self.__class__}.recv timed out after {timeout} seconds")
                time.sleep(0.01)
        else:
            # not blocking, just get the result and return None if nothing was found
            if not self.is_open():
                raise QNotOpenError(f"Q '{self._key}' is no longer open")
            # pop the item off the list
            res = tuple(Q_RECV.eval(self._conn, self._key))
            # if nothing to do yet just return None
            if res[0] == 0:
                return None
        return Task(self, res[1].decode(), self._unserialize(res[2]))
    
    def set_result(self, task_id: str, value: Any):
        """ Set the result for a given item and push item to the done list. """
        # remove from the recieved list
        if value is None:
            raise QValueCannotBeNone(f"Value for task '{task_id}' cannot be None")

        if not self.is_open():
            raise QNotOpenError(f"Cannot set result for task '{task_id}'. Q is not open.")

        return Q_SET.eval(self._conn, task_id, self._key, args=(self._serialize(value),))
        
    def get_result(self, task_id: str, blocking: bool = False, timeout: int = -1):
        """ Get a result for a given ``task_id``. If ``blocking`` is True then block until
        ``timeout`` is reached. If ``timeout`` is < 0 then wait forever until queue is closed.
        """
        if blocking:
            start = time.time()
            value = None
            # if we're blocking then probe every 1 microsecd until we have an item
            while value is None:
                
                value = Q_GET.eval(self._conn, task_id, self._key)

                if time.time() - start > timeout > 0:
                    raise QTimeoutError("Get result timed out.")
                time.sleep(0.01)

        else:
            value = Q_GET.eval(self._conn, task_id, self._key)
            if not value:
                # make sure task still exists
                if not self._conn.exists(self._task_key(task_id)):
                    raise TaskNotFound("Cannot get result. Task no longer exist.")
                # if value is None, key doesn't exist yet
                return None
        # if we have a value, delete the item from the queue and the completed set
        return self._unserialize(value)

    def get_status(self, task_id) -> Tuple[int, int]:
        """ Get the status of a given ``Task`` by ``task_id``
        
        Args:
            task_id: the task id
        
        Return:
            a tuple: (status int, score in set int) or None if not found
        """
        return Q_STATUS.eval(self._conn, task_id, self._key)

    def uptime(self):
        """ Get the uptime for this ``Ch`` """
        if self._id.opened:
            return time.time() - self._id.opened
        return None

    def inspect(self) -> dict:
        """ Get the full status of the queue as a dict.
        
        Returns:
            A dict containing details about this queue

        """
        return {
            'Opened': self.opened,
            'UpTime': self.uptime(),
            'Buffer': self._buffer,
            'Namespame': self._key,
            'Pending': self._conn.zrange(self._key + Q_PEND_SET, 0, -1),
            'Recieved': self._conn.zrange(self._key + Q_RECV_SET, 0, -1),
            'Done': self._conn.zrange(self._key + Q_DONE_SET, 0, -1)
        }

    def reset(self):
        """ Close and then open this ``Q`` """
        self.close()
        self.open()

    def __start_keep_alive_daemon(self):
        """ Create the keep alive thread and start it. """
        # first check to see if thread is already running
        new, props = Q_OPEN.eval(self._conn, self._key, args=(pickle.dumps(self.id),))

        print(f"----\n"
              "Start Daemon Q_OPEN eval:\n"
              f"Key = {self._key}\n"
              f"Is new? {new}\n"
              f"Props: {props}\n"
              "----")

        if new:
            self._stop_thread = threading.Event()
            self._keep_alive_thread = threading.Thread(target=self.__queue_daemon, daemon=True)
            self._keep_alive_thread.start()
            self._id.opened = time.time()
        else:
            # set the properties from the set
            self._id = pickle.loads(props)
            self._buffer = self._id.buffer
            self._set_serializer(self._id.serializer)

    def __queue_daemon(self):
        """ The queue daemon that keeps it alive. """
        while not self._stop_thread.is_set():
            # while we're not being flagged to stop, keep updating the expiration times
            time.sleep(0.1)
            self._conn.pexpire(self._okey, 150)
        return Q_CLOSE.eval(self._conn, self._key)


# Channel constants
CH_APPEND = ':channel'

CH_OPEN = RedisScript(f"""
    -- KEYS[1] = channel key
    -- ARGV[1] = channel properties
    local open_key = KEYS[1] .. '{OPEN_APPEND}'
    if redis.call('exists', open_key) == 0 then
        redis.call('set', open_key, ARGV[1])
        return {{1, ARGV[1]}}
    end
    return {{0, redis.call('get', open_key)}}
""", 1, 'CH_OPEN')

CH_SEND = RedisScript(f"""
    -- KEYS[1] = item id
    -- ARGV[1] = item value
    -- KEYS[2] = channel id
    local item_key = KEYS[2] .. ':' .. KEYS[1]
    redis.call('set', item_key, ARGV[1])
    local ch_set = KEYS[2] .. '{CH_APPEND}'
    local ch_score = ch_set .. '{SCORE_APPEND}'
    redis.call('incr', ch_score)
    redis.call('zadd', ch_set, redis.call('get', ch_score), KEYS[1])
    return item_key
""", 2, 'CH_SEND')

CH_RECV = RedisScript(f"""
   -- KEY[1] = channel set
    local ch_set = KEYS[1] .. '{CH_APPEND}'
    local item = redis.call('zpopmin', ch_set, 1)
    if table.getn(item) == 0 then
        return {{0, nil}}
    end
    local item_key = KEYS[1] ..':' .. item[1]
    local raw = redis.call('get', item_key)
    if raw == false then
        return redis.error_reply("Item Does Not Exist")
    end
    redis.call('del', item_key)
    return {{1, raw}}
""", 1, 'CH_RECV')

CH_CLOSE = RedisScript(f"""
   -- KEY[1] = channel set
    local count = 0
    local ch_set = KEYS[1] .. '{CH_APPEND}'
    local items = redis.call('zrange', ch_set, 0, -1)
    if #items > 0 then
        count = count + #items
        for _, item in ipairs(items) do
            redis.call('del', KEYS[1] .. ':' .. item)
        end
    end
    redis.call('del', ch_set)
    redis.call('del', ch_set .. '{OPEN_APPEND}')
    return count
""", 1, 'CH_CLOSE')


class ChId:
    """ Represents a channel identifier. """
    def __init__(self, key: str, buffer: int = None, serializer: str = None):
        self._key = key
        self._buffer = buffer
        self._serializer = serializer
        self.opened: float = None

    @property
    def key(self):
        """ Getter for the channel id key. """
        return self._key

    @property
    def buffer(self):
        """ Getter for the channel id buffer. """
        return self._buffer

    @property
    def serializer(self):
        """ Getter for the channel id serializer. """
        return self._serializer

    def get_q(self, redis: StrictRedis):
        """ Get a queue object with this ID for a given ``redis`` connection. """
        if (self._buffer is None or self._serializer is None):
            res = redis.get(self._key + OPEN_APPEND)
            if res:
                chid = pickle.loads(res)
                self._buffer = chid.buffer
                self._serializer = chid.serializer
                self.opened = chid.opened

        if self._buffer is not None and self._serializer is not None:
            return Ch(redis, self._key, buffer=self._buffer, serializer=self._serializer,
                      opened=self.opened)

        return None


class ChTimeoutError(Exception):
    """ Raised when ``Ch`` times out trying to send or recieve an item from the queue. """


class ChNotOpenError(Exception):
    """ Raised when ``Ch`` event cannot be completed because queue is not open. """


class ChItemNone(Exception):
    """ Raised when ``Ch`` item being sent is None. """


class Ch:
    """ Channel is a simple one way queue with optional buffer. """

    def __init__(self, conn: StrictRedis, namespace: str = None, buffer: int = 1,
                 serializer: str = 'pickle', opened: float = None):

        """ Initialize a ``Channel`` object.
        
        Args:
            conn: a StrictRedis connection object
            namespace: the namespace that this queue belongs to
             how many items can be in the buffer at the same time. Default is 1
            serializer: the serializer to use for items in this queue. Can be 'pickele',
             'json', or 'msgpack'. Defaults to 'pickle'.
        
        Returns:
            An RdQ object.
        """
        self._conn: StrictRedis = conn
        # task by name if task string provided, else create a random task with uuidv4
        self._buffer = buffer
        # namespace for this quueue which prepends the tasck keys.
        self._key = namespace or f"{__name__}:{self.__class__.__name__}:{uuid.uuid4().__str__()}"
        self._serializer: str = None
        # set the serializer
        self._set_serializer(serializer)
        self._okey = self._key + OPEN_APPEND
        # keep alive thread
        self._keep_alive_thread: threading.Thread = None
        # stop threading event
        self._stop_thread: threading.Event = None
        # start the keey alive daemon right away
        self._id = ChId(self._key, buffer=self._buffer, serializer=self._serializer)
        self._id.opened = opened
        self.__open()

    @property
    def key(self):
        """ Getter for channel key. """
        return self._key

    @property
    def id(self):
        """ Get a ChannelId with this channel's key. """
        return self._id

    @property
    def opened(self):
        """ Getter for time this channel was openned. """
        return self._id.opened
    
    def uptime(self):
        """ Get the uptime for this ``Ch`` """
        if self._id.opened:
            return time.time() - self._id.opened
        return None

    def _set_serializer(self, serializer: str):
        """ Set the serializer for this ``Q``. """
        if serializer.lower() == 'msgpack':
            self._serialize = msgpack.dumps
            self._unserialize = msgpack.loads
            self._serializer = 'msgpack'
        elif serializer.lower() == 'json':
            self._serialize = json.dumps
            self._unserialize = json.loads
            self._serializer = 'json'
        else:
            self._serialize = pickle.dumps
            self._unserialize = pickle.loads
            self._serializer = 'pickle'

    def _task_key(self, task_id):
        return self._key + ":" + task_id

    def is_open(self):
        """ Test if queue is still open. """
        return self._conn.exists(self._okey)

    def close(self):
        """ Close the queue.
        
        Raises:
            redlock.RedLockError when lock attempt fails
        """
        self._stop_thread.set()
        self._keep_alive_thread.join()

    def send(self, *items, timeout: int = -1) -> Any:
        """ Send ``items`` through the queue.
        If queue is at max length block until ``timeout`` expires,
        or if ``timeout`` is < ``0``, block indefinitely.
        
        Args:
            items: a tuple of one or more items to push onto the fifo queue
            timeout: an optional timeout int, defaults to -1 which == infinite.
            
        Raises:
            QueueNotOpenError: if queue isn't open yet
            QueueTimeoutError: if timeout is hit

        Returns:
            The redis key for the item
        """
        start = time.time()
        res = []
        # loop over each item provided and rpush them onto the queue
        for item in items:
            if item is None:
                raise ChItemNone("Item cannot be None.")
            # after each item is inserted we reset the timeout
            while self._conn.zcount(self._key, 0, -1) >= self._buffer and self.is_open():
                if time.time() - start > timeout > 0:
                    raise ChTimeoutError("Send time out.")
                time.sleep(0.001)
            if not self.is_open():
                raise ChNotOpenError(f"Queue '{self._key}' is no longer open")
            # place the actual item in a tuple with the item id before pushing onto redis list
            res.append(CH_SEND.eval(self._conn, uuid.uuid4().__str__(), self._key,
                                    args=(self._serialize(item),)))
        return res

    def recv(self, blocking: bool = False, timeout: int = -1) -> 'Task':
        """ Recv an item in the queue.
        Block until ``timeout`` expires, or if ``timeout`` is < ``0``, block indefinitely.

        Args:
            blocking: whether or not to block until an item is available in the queue to recieve
        
        Returns:
            A tuple containing unserialized value from the redis list if a value was found or
            (None, None) if nothing is in the queue currently, but queue is still openself or
            False if the queue is now closed.
        """
        if blocking:
            start = time.time()
            # if we're blocking then probe every 1 microsecd until we have an item
            while self.is_open():
                res = CH_RECV.eval(self._conn, self._key)
                if res[0] == 1:
                    break
                if time.time() - start > timeout > 0:
                    raise ChTimeoutError(f"{self.__class__}.recv timed out after {timeout}s.")
                time.sleep(0.01)
        else:
            # not blocking, just get the result and return None if nothing was found
            if not self.is_open():
                raise ChTimeoutError(f"Queue '{self._key}' is no longer open")
            # pop the item off the list
            res = CH_RECV.eval(self._conn, self._key)
            # if nothing to do yet just return None
            if res[0] == 0:
                return None
                
        return self._unserialize(res[1])

    def inspect(self) -> dict:
        """ Get the full status of the channel.
        
        Returns:
            A dict containing details about this queue

        """
        return {
            'Opened': self._id.opened,
            'UpTime': self.uptime(),
            'Buffer': self._buffer,
            'Id': self._key,
            'items': self._conn.zrange(self._key + ":ch", 0, -1),
        }

    def __open(self):
        """ Create the keep alive thread and start it. """
        new, chid = CH_OPEN.eval(self._conn, self._key, args=(pickle.dumps(self.id),))
        if new == 1:
            # only open a keep alive thread if channel isn't already open
            self._stop_thread = threading.Event()
            self._keep_alive_thread = threading.Thread(target=self.__keep_alive_daemon, daemon=True)
            self._keep_alive_thread.start()
            self._id.opened = time.time()
        else:
            # set the id and props
            self._id = pickle.loads(chid)
            self._buffer = self._id.buffer
            self._set_serializer(self._id.serializer)

    def __keep_alive_daemon(self):
        """ The queue daemon that keeps it alive. """
        self._conn.set(self._okey, 1, px=150)

        while not self._stop_thread.is_set():
            # while we're not being flagged to stop, keep updating the expiration times
            time.sleep(0.1)
            self._conn.pexpire(self._okey, 150)
        return CH_CLOSE.eval(self._conn, self._key)
