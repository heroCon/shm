"""ctypes bindings for libshm_pubsub_c. Loans are memoryviews onto shared blocks."""

import ctypes
import os
from pathlib import Path

OK = 0
WOULD_BLOCK = 8


def _library():
    override = os.environ.get("SHM_PUBSUB_C_LIBRARY")
    if override:
        return override
    here = Path(__file__).resolve()
    candidate = here.parents[3] / "build" / "libshm_pubsub_c.so"
    if candidate.exists():
        return str(candidate)
    return "libshm_pubsub_c.so"


class Loan:
    """View of a shared block. memoryview() is zero-copy.

    A publisher loan is writable. A loan returned by Subscriber.take is not:
    private subscribers share the block, so a write would change what the
    others receive. Assignment is bounded by the loan; a slice uses the same
    rules as a memoryview of this length.
    """

    def __init__(self, address, size, writable=True):
        self._address = address
        self._size = size
        self._writable = writable

    def __len__(self):
        return self._size

    def __setitem__(self, key, value):
        if not self._writable:
            raise TypeError("read-only loan")
        data = bytes(value)
        start, length = self._assign_span(key, len(data))
        if length:
            ctypes.memmove(self._address + start, data, length)

    def _assign_span(self, key, nbytes):
        if isinstance(key, slice):
            if key.step not in (None, 1):
                raise ValueError("loan assignment step must be 1")
            start, stop, _step = key.indices(self._size)
            length = stop - start
            if nbytes != length:
                raise ValueError("loan assignment length mismatch")
            return start, length
        if isinstance(key, int):
            index = key + self._size if key < 0 else key
            if index < 0 or index >= self._size:
                raise IndexError("loan index out of range")
            if nbytes != 1:
                raise ValueError("loan assignment length mismatch")
            return index, 1
        raise TypeError("loan index must be an int or a slice")

    def __bytes__(self):
        return ctypes.string_at(self._address, self._size)

    def memoryview(self):
        raw = (ctypes.c_ubyte * self._size).from_address(self._address)
        view = memoryview(raw)
        if not self._writable:
            return view.toreadonly()
        return view


class Error(RuntimeError):
    def __init__(self, status):
        super().__init__("shm_pubsub status %s" % status)
        self.status = status


def _check(status):
    if status != OK:
        raise Error(status)
    return status


class _Lib:
    def __init__(self):
        self.lib = ctypes.CDLL(_library())
        L = self.lib
        L.shm_pubsub_topic_open.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.c_void_p)]
        L.shm_pubsub_topic_close.argtypes = [ctypes.c_void_p]
        L.shm_pubsub_publisher_create.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p)]
        L.shm_pubsub_publisher_destroy.argtypes = [ctypes.c_void_p]
        L.shm_pubsub_subscriber_create.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.POINTER(ctypes.c_void_p),
        ]
        L.shm_pubsub_subscriber_destroy.argtypes = [ctypes.c_void_p]
        L.shm_pubsub_publish_copy.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t]
        L.shm_pubsub_loan.argtypes = [
            ctypes.c_void_p,
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.POINTER(ctypes.c_uint32),
        ]
        L.shm_pubsub_publish_loan.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
        L.shm_pubsub_release_loan.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
        L.shm_pubsub_receive.argtypes = [
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.c_size_t,
            ctypes.POINTER(ctypes.c_size_t),
        ]
        L.shm_pubsub_take_loan.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.POINTER(ctypes.c_size_t),
            ctypes.POINTER(ctypes.c_uint32),
        ]
        L.shm_pubsub_release_read.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
        L.shm_pubsub_free_blocks.argtypes = [ctypes.c_void_p]
        L.shm_pubsub_free_blocks.restype = ctypes.c_uint
        for name in (
            "shm_pubsub_topic_open",
            "shm_pubsub_publisher_create",
            "shm_pubsub_subscriber_create",
            "shm_pubsub_publish_copy",
            "shm_pubsub_loan",
            "shm_pubsub_publish_loan",
            "shm_pubsub_release_loan",
            "shm_pubsub_receive",
            "shm_pubsub_take_loan",
            "shm_pubsub_release_read",
        ):
            getattr(L, name).restype = ctypes.c_int


_LIB = None


def lib():
    global _LIB
    if _LIB is None:
        _LIB = _Lib()
    return _LIB.lib


class Topic:
    def __init__(self, name):
        out = ctypes.c_void_p()
        _check(lib().shm_pubsub_topic_open(name.encode(), ctypes.byref(out)))
        self._topic = out

    def close(self):
        if self._topic:
            lib().shm_pubsub_topic_close(self._topic)
            self._topic = None

    def __del__(self):
        self.close()

    def publisher(self):
        return Publisher(self)

    def subscriber(self, group=0):
        return Subscriber(self, group)

    def free_blocks(self):
        return lib().shm_pubsub_free_blocks(self._topic)


class Publisher:
    def __init__(self, topic):
        out = ctypes.c_void_p()
        _check(lib().shm_pubsub_publisher_create(topic._topic, ctypes.byref(out)))
        self._owner = topic
        self._pub = out

    def close(self):
        if self._pub:
            lib().shm_pubsub_publisher_destroy(self._pub)
            self._pub = None

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def publish(self, payload):
        data = bytes(payload)
        buf = ctypes.create_string_buffer(data)
        _check(lib().shm_pubsub_publish_copy(self._pub, buf, len(data)))

    def loan(self, size):
        data = ctypes.c_void_p()
        ident = ctypes.c_uint32()
        _check(lib().shm_pubsub_loan(self._pub, size, ctypes.byref(data), ctypes.byref(ident)))
        return Loan(data.value, size, writable=True), ident.value

    def publish_loan(self, ident):
        _check(lib().shm_pubsub_publish_loan(self._pub, ident))


class Subscriber:
    def __init__(self, topic, group=0):
        out = ctypes.c_void_p()
        _check(lib().shm_pubsub_subscriber_create(topic._topic, group, 0, ctypes.byref(out)))
        self._owner = topic
        self._sub = out

    def close(self):
        if self._sub:
            lib().shm_pubsub_subscriber_destroy(self._sub)
            self._sub = None

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def receive(self, capacity=4096):
        buf = ctypes.create_string_buffer(capacity)
        size = ctypes.c_size_t()
        status = lib().shm_pubsub_receive(self._sub, buf, capacity, ctypes.byref(size))
        if status == WOULD_BLOCK:
            return None
        _check(status)
        return buf.raw[: size.value]

    def take(self):
        data = ctypes.c_void_p()
        size = ctypes.c_size_t()
        ident = ctypes.c_uint32()
        status = lib().shm_pubsub_take_loan(
            self._sub, ctypes.byref(data), ctypes.byref(size), ctypes.byref(ident)
        )
        if status == WOULD_BLOCK:
            return None
        _check(status)
        return Loan(data.value, size.value, writable=False), ident.value

    def release(self, ident):
        _check(lib().shm_pubsub_release_read(self._sub, ident))
