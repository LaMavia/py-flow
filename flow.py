from threading import Thread, RLock, Condition
import functools as fun
from collections.abc import Callable, Iterable
from queue import Queue
from inspect import signature, Signature
from typing import Generic, TypeVar
import itertools as it
from dataclasses import dataclass
from abc import ABC, abstractmethod
from contextlib import contextmanager

T = TypeVar('T')
TFlow = TypeVar('TFlow', bound="_Flow")

DO_LOG = True


@contextmanager
def locked(lock: RLock):
    lock.acquire()
    try:
        yield
    finally:
        lock.release()


class _Flow(ABC, Thread):
    mutex: RLock
    queues_ready_cond: Condition
    n_queues_filled: int
    n_dead_deps: int
    n_deps: int
    can_die: bool
    must_die: bool
    started: bool
    queues: list[Queue]

    def __init__(self, n_queues: int, **kwargs) -> None:
        super().__init__()

        self.mutex = RLock()
        self.queues_ready_cond = Condition(self.mutex)
        self.n_queues_filled = 0
        self.n_dead_deps = 0
        self.n_deps = 0
        self.can_die = False
        self.must_die = False
        self.started = False
        self.queues = [Queue() for _ in range(n_queues)]
        if 'label' in kwargs:
            self.name = f'''{kwargs['label']} ({self.name})'''

    def start(self) -> TFlow:
        with locked(self.mutex):
            self.started = True
            super().start()
            return self

    def check_arg_i(self, i: int):
        if i >= (queue_n := len(self.queues)):
            raise Exception(
                f'''Invalid queue index {i}. Given node has {queue_n} queues.''')

    def push(self, x: ..., i: int) -> TFlow:
        with locked(self.mutex):
            self.check_arg_i(i)

            queue = self.queues[i]
            was_empty = queue.empty()
            queue.put(x)

            if was_empty:
                self.n_queues_filled += 1
            if self.n_queues_filled == len(self.queues):
                self.queues_ready_cond.notify_all()
                if not self.started:
                    self.start()

            return self

    def let_die(self) -> None:
        with locked(self.mutex):
            self.can_die = True
            self.queues_ready_cond.notify_all()

    def kill(self) -> None:
        with locked(self.mutex):
            self.must_die = True
            self.can_die = True
            self.queues_ready_cond.notify_all()

    @abstractmethod
    def pipe(self, flow: TFlow, *indices: int) -> TFlow:
        ...

    @abstractmethod
    def _process(self) -> ...:
        ...

    @abstractmethod
    def _send(self) -> ...:
        ...

    @abstractmethod
    def _is_done(self) -> bool:
        ...

    def _die(self) -> None:
        pass

    def _get_args(self) -> list:
        self.n_queues_filled = 0
        args = []

        for queue in self.queues:
            args.append(queue.get())
            if not queue.empty():
                self.n_queues_filled += 1

        return args

    def run(self) -> None:
        while True:
            self._log("waiting for args...")
            self.__await_args()
            self._log("got args")
            if self.must_die or (self.n_queues_filled < len(self.queues) and self._is_done()):
                self._die()
                self.mutex.release()
                break

            self._log("processing...")
            self._process()
            self._log("sending...")
            self._send()
            self.mutex.release()

    def _can_die(self) -> bool:
        out = self.must_die or (
            self.can_die and self.n_dead_deps == self.n_deps)
        self._log(
            f'can_die: must={self.must_die}, deps={self.n_dead_deps}/{self.n_deps}; result: {out}')
        return out

    def _log(self, msg: str) -> None:
        if DO_LOG:
            print(f'[{self.name}] {msg}')

    def __await_args(self):
        self.mutex.acquire()
        self.queues_ready_cond.wait_for(
            lambda: (self.n_queues_filled >= len(self.queues)) or self._can_die())


class FuncFlow(_Flow):
    f: Callable
    sig: Signature
    forward: list[tuple[TFlow, list[int]]]
    result: ...

    def __init__(self, f: Callable, **kwargs) -> None:
        self.f = f
        self.sig = signature(f)
        self.forward = []
        super().__init__(len(self.sig.parameters), **kwargs)

    def pipe(self, flow: TFlow, *indices: int) -> "FuncFlow":
        with locked(self.mutex):
            for i in indices:
                flow.check_arg_i(i)

            for other, js in self.forward:
                if other == flow:
                    js.extend(indices)
                    return self

            self.forward.append((flow, list(indices)))
            with locked(flow.mutex):
                flow.n_deps += 1
            return self

    def _process(self) -> ...:
        args = self._get_args()
        self.result = self.f(*args)

    def _send(self) -> ...:
        for flow, indices in self.forward:
            for i in indices:
                flow.push(self.result, i)

    def _is_done(self) -> bool:
        return self.n_dead_deps == len(self.queues)

    def _die(self) -> None:
        self._log("dying")
        for flow, _ in self.forward:
            flow.mutex.acquire()
            flow.n_dead_deps += 1
            flow.queues_ready_cond.notify_all()
            flow.mutex.release()


class NonNullFuncFlow(FuncFlow):
    def __init__(self, f: Callable, **kwargs) -> None:
        super().__init__(f, **kwargs)

    def _send(self) -> ...:
        if self.result == None:
            return
        return super()._send()

    def _process(self) -> ...:
        out = super()._process()
        self._log(f'produced {out}')
        if self.result == None:
            self.must_die = True
        return out

    def _is_done(self) -> bool:
        return super()._is_done() or self.result == None


if __name__ == '__main__':
    inp = NonNullFuncFlow(lambda: (y := input()) and y or None, label='input')
    conv = FuncFlow(lambda x: int(x), label='converted')
    f = FuncFlow(lambda x: x*x + 1, label='func')
    pr = FuncFlow(lambda y, x: print(
        f'result: {y} for x = {x}'), label='printer')

    inp.pipe(conv, 0)
    conv.pipe(f, 0)
    conv.pipe(pr, 1)
    f.pipe(pr, 0)

    inp.start()
    conv.start()
    f.start()
    pr.start()

    inp.let_die()
    conv.let_die()
    f.let_die()
    pr.let_die()
