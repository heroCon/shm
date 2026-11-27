import multiprocessing as mp
import os

import shm_pubsub


def _child(name, ready):
    topic = shm_pubsub.Topic(name)
    sub = topic.subscriber()
    ready.set()
    while True:
        got = sub.receive()
        if got == b"ping":
            break
    os._exit(0)


def test_loan_and_copy():
    name = "/shm_pubsub_py_%d" % os.getpid()
    topic = shm_pubsub.Topic(name)
    pub = topic.publisher()
    sub = topic.subscriber()
    view, ident = pub.loan(4)
    assert len(view.memoryview()) == 4
    view[:4] = b"abcd"
    pub.publish_loan(ident)
    taken, rid = sub.take()
    assert bytes(taken) == b"abcd"
    assert bytes(taken.memoryview()) == b"abcd"
    sub.release(rid)
    pub.publish(b"xy")
    assert sub.receive() == b"xy"
    assert sub.receive() is None


def test_loan_is_bounded_and_received_loans_are_read_only():
    name = "/shm_pubsub_py_ro_%d" % os.getpid()
    topic = shm_pubsub.Topic(name)
    pub = topic.publisher()
    first = topic.subscriber()
    second = topic.subscriber()
    view, ident = pub.loan(4)
    try:
        view[:4] = b"123456"
        raise AssertionError("oversize assignment wrote past the loan")
    except ValueError:
        pass
    try:
        view[-10] = b"q"
        raise AssertionError("negative index wrote before the loan")
    except IndexError:
        pass
    view[-2:] = b"yz"
    view[:2] = b"ab"
    pub.publish_loan(ident)
    taken_a, id_a = first.take()
    taken_b, id_b = second.take()
    try:
        taken_a[:4] = b"evil"
        raise AssertionError("received loan accepted a write")
    except TypeError:
        pass
    view_b = taken_b.memoryview()
    assert view_b.readonly
    assert bytes(view_b) == b"abyz"
    assert bytes(taken_a) == b"abyz"
    assert bytes(taken_b) == b"abyz"
    first.release(id_a)
    second.release(id_b)


def test_multiprocess():
    name = "/shm_pubsub_py_mp_%d" % os.getpid()
    topic = shm_pubsub.Topic(name)
    ready = mp.Event()
    child = mp.Process(target=_child, args=(name, ready))
    child.start()
    ready.wait(2)
    topic.publisher().publish(b"ping")
    child.join(2)
    assert child.exitcode == 0
