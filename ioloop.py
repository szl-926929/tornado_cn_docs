#coding=utf-8
#!/usr/bin/env python
#
# Copyright 2009 Facebook
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""An I/O event loop for non-blocking sockets.

Torando 在Linux和FreeBSD上使用高效的异步I/O模型 epoll 和kqueue来实现高效的web服务器,


Typical applications will use a single `IOLoop` object, in the
`IOLoop.instance` singleton(单例).  The `IOLoop.start` method should usually
be called at the end of the ``main()`` function.

Atypical(非典型) applications may
use more than one `IOLoop`, such as one `IOLoop` per thread, or per `unittest(单元测试)`
case.

In addition to(除了) I/O events, the `IOLoop` can also schedule time-based events(基于事件的事件).
`IOLoop.add_timeout` is a non-blocking alternative(替代) to `time.sleep`.
"""

from __future__ import absolute_import, division, print_function, with_statement

import datetime
import errno
import functools
import heapq
import itertools
import logging
import numbers
import os
import select
import sys
import threading
import time
import traceback

from tornado.concurrent import TracebackFuture, is_future
from tornado.log import app_log, gen_log
from tornado import stack_context
from tornado.util import Configurable, errno_from_exception, timedelta_to_seconds

try:
    import signal
except ImportError:
    signal = None

try:
    import thread  # py2
except ImportError:
    import _thread as thread  # py3

from tornado.platform.auto import set_close_exec, Waker


_POLL_TIMEOUT = 3600.0


class TimeoutError(Exception):
    pass


class IOLoop(Configurable):
    """A level-triggered I/O loop.

    We use ``epoll`` (Linux) or ``kqueue`` (BSD and Mac OS X) if they
    are available, or else we fall back on select().
    高并发时你应该使用`epoll` or `kqueue`
    例如使用一个简单的TCP服务器::

        import errno
        import functools
        import ioloop
        import socket

        def connection_ready(sock, fd, events):
            while True:
                try:
                    connection, address = sock.accept()
                except socket.error, e:
                    if e.args[0] not in (errno.EWOULDBLOCK, errno.EAGAIN):
                        raise
                    return
                connection.setblocking(0)
                handle_connection(connection, address)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setblocking(0)
        sock.bind(("", port))
        sock.listen(128)

        io_loop = ioloop.IOLoop.instance()

        # 用一些默认参数包装一个可调用对象,返回结果是可调用对象，并且可以像原始对象一样对待，
        # 冻结 connection_ready 的第一个参数为 sock ,既 socket 的返回值
        callback = functools.partial(connection_ready, sock)

        # 注册函数，  第一个参数是将 sock 转换为标准的描述符，第二个为回调函数，第三个是事件类型       
        io_loop.add_handler(sock.fileno(), callback, io_loop.READ)

        io_loop.start()

    """
    # 从epoll的模块常量
    _EPOLLIN = 0x001
    _EPOLLPRI = 0x002
    _EPOLLOUT = 0x004
    _EPOLLERR = 0x008
    _EPOLLHUP = 0x010
    _EPOLLRDHUP = 0x2000
    _EPOLLONESHOT = (1 << 30)
    _EPOLLET = (1 << 31)

    # 我们的活动正好映射到epoll的事件
    NONE = 0
    READ = _EPOLLIN
    WRITE = _EPOLLOUT
    ERROR = _EPOLLERR | _EPOLLHUP

    # 创建IOLoop实例全局锁
    _instance_lock = threading.Lock()

    _current = threading.local()

    @staticmethod
    def instance():
        """返回一个全局`IOLoop`实例.

        大多数应用程序都是单一的，全局`IOLoop`运行在(main thread)主线程上。
        使用该方法从另一个线程中获取这个实例
        获取当前线程的`IOLoop`, 用 `current()`方法

        单例模式：一个进程只有一个

        [注解]: 在Mac上运行 `tornado.ioloop.IOLoop.current()`
         则输出 <tornado.platform.kqueue.KQueueIOLoop object at 0x103aeca50>

        """
        if not hasattr(IOLoop, "_instance"):
            with IOLoop._instance_lock:
                if not hasattr(IOLoop, "_instance"):
                    # New instance after double check
                    IOLoop._instance = IOLoop()
        return IOLoop._instance

    @staticmethod
    def initialized():
        """如果单例已创建返回true"""
        return hasattr(IOLoop, "_instance")

    def install(self):
        """安装此`IOLoop`对象作为一个单例。

        这通常是没有必要的，因为`instance()` 将创建一个`IOLoop`需求
        但是你可能想调用`install` 去使用一个自定义的`IOLoop`子类
        """
        assert not IOLoop.initialized()
        IOLoop._instance = self

    @staticmethod
    def clear_instance():
        """清除全局`IOLoop` instance.

        .. versionadded:: 4.0
        """
        if hasattr(IOLoop, "_instance"):
            del IOLoop._instance

    @staticmethod
    def current():
        """Returns the current thread's `IOLoop`.

        If an `IOLoop` is currently running or has been marked as current
        by `make_current`, returns that instance.  Otherwise returns
        `IOLoop.instance()`, i.e. the main thread's `IOLoop`.

        A common pattern for classes that depend on ``IOLoops`` is to use
        a default argument to enable programs with multiple ``IOLoops``
        but not require the argument for simpler applications::

            class MyClass(object):
                def __init__(self, io_loop=None):
                    self.io_loop = io_loop or IOLoop.current()

        In general you should use `IOLoop.current` as the default when
        constructing an asynchronous object, and use `IOLoop.instance`
        when you mean to communicate to the main thread from a different
        one.
        """
        current = getattr(IOLoop._current, "instance", None)
        if current is None:
            return IOLoop.instance()
        return current

    def make_current(self):
        """Makes this the `IOLoop` for the current thread.

        An `IOLoop` automatically becomes current for its thread
        when it is started, but it is sometimes useful to call
        `make_current` explictly before starting the `IOLoop`,
        so that code run at startup time can find the right
        instance.
        """
        IOLoop._current.instance = self

    @staticmethod
    def clear_current():
        IOLoop._current.instance = None

    @classmethod
    def configurable_base(cls):
        return IOLoop

    @classmethod
    def configurable_default(cls):
        if hasattr(select, "epoll"):
            from tornado.platform.epoll import EPollIOLoop
            return EPollIOLoop
        if hasattr(select, "kqueue"):
            # Python 2.6+ on BSD or Mac
            from tornado.platform.kqueue import KQueueIOLoop
            return KQueueIOLoop
        from tornado.platform.select import SelectIOLoop
        return SelectIOLoop

    def initialize(self):
        pass

    def close(self, all_fds=False):
        """Closes the `IOLoop`, 释放所使用的任何资源.

        If ``all_fds`` is true, 注册在该`IOLoop`的所有文件描述符将被关闭(不只是IOLoop本身所创建的)
        许多应用程序将只使用一个单一的'IOLoop`运行该方法的整个生命周期
        在这种情况下关闭`IOLoop`是没有必要的，因为一旦进程退出，一切将被清理.
        `IOLoop.close` 主要提供如下方案：
            unit tests(单元测试), which create and destroy a large number of
        ``IOLoops``.

        一个`IOLoop`在它被关闭前必须完全停止，这意味着`IOLoop.stop()`必须调用，
        `IOLoop.start()`必须在调用`IOLoop.close()`前有返回。

        .. versionchanged:: 3.1
           If the `IOLoop` implementation(实现) supports non-integer objects
           for "file descriptors", those objects will have their
           ``close`` method when ``all_fds`` is true.
        """
        raise NotImplementedError()

    def add_handler(self, fd, handler, events):
        """
        [比较重要:将文件描述符发生相应的事件时的回调函数对应]
        add_handler用于添加socket到主循环中, 
        接受三个参数: 
            fd 是socket的文件描述符 
            handler 是处理此socket的callback函数 
            events 是此socket注册的事件

        Registers the given handler to receive the given events for ``fd``.

        The ``fd`` argument may either be an integer file descriptor or
        a file-like object with a ``fileno()`` method (and optionally a
        ``close()`` method, which may be called when the `IOLoop` is shut
        down).

        The ``events`` argument is a bitwise(按位) 或者
        ``IOLoop.READ``, ``IOLoop.WRITE``, and ``IOLoop.ERROR``的常量.

        当一个事件发生, ``handler(fd, events)`` will be run.

        .. versionchanged:: 4.0
           Added the ability to pass file-like objects in addition to
           raw file descriptors.
        """
        raise NotImplementedError()

    def update_handler(self, fd, events):
        """Changes the events we listen for ``fd``.

        .. versionchanged:: 4.0
           Added the ability to pass file-like objects in addition to
           raw file descriptors.
        """
        raise NotImplementedError()

    def remove_handler(self, fd):
        """Stop listening for events on ``fd``.

        .. versionchanged:: 4.0
           Added the ability to pass file-like objects in addition to
           raw file descriptors.
        """
        raise NotImplementedError()

    def set_blocking_signal_threshold(self, seconds, action):
        """发送一个信号如果`IOLoop`被阻塞超过``s`` seconds.

        Pass ``seconds=None`` to disable.  Requires Python 2.6 on a unixy
        platform.

        action: 是一个 Python signal handler.
        `action`为 None, 被阻塞太久的进程将会被干掉
        """
        raise NotImplementedError()

    def set_blocking_log_threshold(self, seconds):
        """记录一个堆栈跟踪(stack trace)，如果`IOLoop`被阻塞超过``s``秒。

        相当于 ``set_blocking_signal_threshold(seconds, self.log_stack)``
        """
        self.set_blocking_signal_threshold(seconds, self.log_stack)

    def log_stack(self, signal, frame):
        """信号处理程序来记录当前线程的堆栈跟踪。

        For use with `set_blocking_signal_threshold`.
        """
        gen_log.warning('IOLoop blocked for %f seconds in\n%s',
                        self._blocking_signal_threshold,
                        ''.join(traceback.format_stack(frame)))

    def start(self):
        """Starts the I/O loop.

        The loop will run until one of the callbacks calls `stop()`, which
        will make the loop stop after the current event iteration completes.
        """
        raise NotImplementedError()

    def _setup_logging(self):
        """The IOLoop catches and logs exceptions, so it's
        important that log output be visible.  However, python's
        default behavior for non-root loggers (prior to python
        3.2) is to print an unhelpful "no handlers could be
        found" message rather than the actual log entry, so we
        must explicitly configure logging if we've made it this
        far without anything.

        This method should be called from start() in subclasses.
        """
        if not any([logging.getLogger().handlers,
                    logging.getLogger('tornado').handlers,
                    logging.getLogger('tornado.application').handlers]):
            logging.basicConfig()

    def stop(self):
        """Stop the I/O loop.

        To use asynchronous methods from otherwise-synchronous code (such as
        unit tests), you can start and stop the event loop like this::

          ioloop = IOLoop()
          async_method(ioloop=ioloop, callback=ioloop.stop)
          ioloop.start()

        ``ioloop.start()`` will return after ``async_method`` has run
        its callback, whether that callback was invoked before or
        after ``ioloop.start``.

        Note that even after `stop` has been called, the `IOLoop` is not
        completely stopped until `IOLoop.start` has also returned.
        Some work that was scheduled before the call to `stop` may still
        be run before the `IOLoop` shuts down.
        """
        raise NotImplementedError()

    def run_sync(self, func, timeout=None):
        """Starts the `IOLoop`, runs the given function, and stops the loop.

        If the function returns a `.Future`, the `IOLoop` will run
        until the future is resolved(解析).  If it raises an exception, the
        `IOLoop` will stop and the exception will be re-raised to the
        caller.

        The keyword-only argument ``timeout`` may be used to set
        a maximum(最大) duration(长短) for the function.  If the timeout expires,
        a `TimeoutError` is raised.

        This method is useful in conjunction(关联) with `tornado.gen.coroutine`
        to allow asynchronous calls in a ``main()`` function::

            @gen.coroutine
            def main():
                # do stuff...

            if __name__ == '__main__':
                IOLoop.instance().run_sync(main)
        """
        future_cell = [None]

        def run():
            try:
                result = func()             # 无参
            except Exception:
                future_cell[0] = TracebackFuture()
                future_cell[0].set_exc_info(sys.exc_info())
            else:       # if no exception,get here(try-except-else-finally写法)
                if is_future(result):       # 判断函数返回值是否是`Future`对象
                    future_cell[0] = result
                else:
                    future_cell[0] = TracebackFuture()
                    future_cell[0].set_result(result)
            # 将`Future`对象和IOLoop的`stop()`作为参数传入`add_future()`函数中
            self.add_future(future_cell[0], lambda future: self.stop())


        self.add_callback(run)      # 回调函数
        if timeout is not None:     # 超时设置,`stop()`
            timeout_handle = self.add_timeout(self.time() + timeout, self.stop)

        # 开始ioloop
        self.start()

        if timeout is not None:
            self.remove_timeout(timeout_handle)

        if not future_cell[0].done():       # `done()`方法
            raise TimeoutError('Operation timed out after %s seconds' % timeout)
        return future_cell[0].result()      # 返回`result()`

    def time(self):
        """根据`IOLoop`的时钟返回当前时间。

        The return value is a floating-point number(浮点数) relative to(相对于) an
        unspecified(不确定的) time in the past.

        By default, the `IOLoop`'s time function is `time.time`.  However,
        it may be configured to use e.g. `time.monotonic` instead.
        Calls to `add_timeout` that pass a number instead of a
        `datetime.timedelta` should use this function to compute the
        appropriate time, so they can work no matter what time function
        is chosen.
        """
        return time.time()

    def add_timeout(self, deadline, callback, *args, **kwargs):
        """Runs the ``callback`` at the time ``deadline`` from the I/O loop.

        Returns an opaque(不透明的) handle 可能被传递到`remove_timeout`取消。

        ``deadline``(最后期限) may be a number denoting(表示) a time (在同等规模的`IOLoop.time`，通常`time.time`),
        或者是相对于当前时间的`datetime.timedelta`对象期限.

        Since Tornado 4.0, `call_later`是因为它不要求一个timedelta对象的相对情况下，作为更方便的选择。

        Note that it is not safe to call `add_timeout` from other threads.(其他线程调用`add_timeout`的不安全性)
        
        相反，你必须使用`add_callback`来控制转移到`IOLoop`的线程，然后从那里调用`add_timeout`。

        `IOLoop`子类实现规则：

            必须包含 `add_timeout` or `call_at`; the default implementations of each will call
        the other.  

        `call_at` is usually easier to implement, but
        subclasses that wish to maintain(维持) compatibility(兼容性) with Tornado
        versions prior to(之前) 4.0 must use `add_timeout` instead.

        .. versionchanged:: 4.0
           Now passes through ``*args`` and ``**kwargs`` to the callback.
        """
        if isinstance(deadline, numbers.Real):
            return self.call_at(deadline, callback, *args, **kwargs)
        elif isinstance(deadline, datetime.timedelta):
            return self.call_at(self.time() + timedelta_to_seconds(deadline),
                                callback, *args, **kwargs)
        else:
            raise TypeError("Unsupported deadline %r" % deadline)

    def call_later(self, delay, callback, *args, **kwargs):
        """
        有点像定时计划, 这个函数同`add_timeout`
        delay: N 秒后,
        Runs the ``callback`` after ``delay`` seconds have passed.

        Returns an opaque handle that may be passed to `remove_timeout`
        to cancel.  Note that unlike the `asyncio` method of the same
        name, the returned object does not have a ``cancel()`` method.

        See `add_timeout` for comments on thread-safety and subclassing.

        .. versionadded:: 4.0
        """
        return self.call_at(self.time() + delay, callback, *args, **kwargs)

    def call_at(self, when, callback, *args, **kwargs):
        """
        运行``callback``在由``when``指定的绝对时间。

        Runs the ``callback`` at the absolute time designated by ``when``.

        ``when`` must be a number using the same reference point as
        `IOLoop.time`.

        Returns an opaque handle that may be passed to `remove_timeout`
        to cancel.  Note that unlike the `asyncio` method of the same
        name, the returned object does not have a ``cancel()`` method.

        See `add_timeout` for comments on thread-safety and subclassing.

        .. versionadded:: 4.0
        """
        return self.add_timeout(when, callback, *args, **kwargs)

    def remove_timeout(self, timeout):
        """Cancels a pending timeout.

        The argument is a handle as returned by `add_timeout`.  It is
        safe to call `remove_timeout` even if the callback has already
        been run.
        """
        raise NotImplementedError()

    def add_callback(self, callback, *args, **kwargs):
        """Calls the given callback on the next I/O loop iteration.

        It is safe to call this method from any thread at any time,
        except from a signal handler.  Note that this is the **only**
        method in `IOLoop` that makes this thread-safety guarantee; all
        other interaction with the `IOLoop` must be done from that
        `IOLoop`'s thread.  `add_callback()` may be used to transfer
        control from other threads to the `IOLoop`'s thread.

        To add a callback from a signal handler, see
        `add_callback_from_signal`.
        """
        raise NotImplementedError()

    def add_callback_from_signal(self, callback, *args, **kwargs):
        """Calls the given callback on the next I/O loop iteration.

        Safe for use from a Python signal handler; should not be used
        otherwise.

        Callbacks added with this method will be run without any
        `.stack_context`, to avoid picking up the context of the function
        that was interrupted by the signal.
        """
        raise NotImplementedError()

    def spawn_callback(self, callback, *args, **kwargs):
        """Calls the given callback on the next IOLoop iteration.

        Unlike all other callback-related methods on IOLoop,
        ``spawn_callback`` does not associate the callback with its caller's
        ``stack_context``, so it is suitable for fire-and-forget callbacks
        that should not interfere with the caller.

        .. versionadded:: 4.0
        """
        with stack_context.NullContext():
            self.add_callback(callback, *args, **kwargs)

    def add_future(self, future, callback):
        """Schedules a callback on the ``IOLoop`` when the given
        `.Future` is finished.

        The callback is invoked with one argument, the
        `.Future`.
        """
        assert is_future(future)
        callback = stack_context.wrap(callback)
        future.add_done_callback(
            lambda future: self.add_callback(callback, future))

    def _run_callback(self, callback):
        """Runs a callback with error handling.

        For use in subclasses.
        """
        try:
            ret = callback()
            if ret is not None and is_future(ret):
                # Functions that return Futures typically swallow all
                # exceptions and store them in the Future.  If a Future
                # makes it out to the IOLoop, ensure its exception (if any)
                # gets logged too.
                self.add_future(ret, lambda f: f.result())
        except Exception:
            self.handle_callback_exception(callback)

    def handle_callback_exception(self, callback):
        """This method is called whenever a callback run by the `IOLoop`
        throws an exception.

        By default simply logs the exception as an error.  Subclasses
        may override this method to customize reporting of exceptions.

        The exception itself is not passed explicitly, but is available
        in `sys.exc_info`.
        """
        app_log.error("Exception in callback %r", callback, exc_info=True)

    def split_fd(self, fd):
        """Returns an (fd, obj) pair from an ``fd`` parameter.

        We accept both raw file descriptors and file-like objects as
        input to `add_handler` and related methods.  When a file-like
        object is passed, we must retain the object itself so we can
        close it correctly when the `IOLoop` shuts down, but the
        poller interfaces favor file descriptors (they will accept
        file-like objects and call ``fileno()`` for you, but they
        always return the descriptor itself).

        This method is provided for use by `IOLoop` subclasses and should
        not generally be used by application code.

        .. versionadded:: 4.0
        """
        try:
            return fd.fileno(), fd
        except AttributeError:
            return fd, fd

    def close_fd(self, fd):
        """Utility method to close an ``fd``.

        If ``fd`` is a file-like object, we close it directly; otherwise
        we use `os.close`.

        This method is provided for use by `IOLoop` subclasses (in
        implementations of ``IOLoop.close(all_fds=True)`` and should
        not generally be used by application code.

        .. versionadded:: 4.0
        """
        try:
            try:
                fd.close()
            except AttributeError:
                os.close(fd)
        except OSError:
            pass


class PollIOLoop(IOLoop):
    """Base class for IOLoops built around a select-like function.

    For concrete implementations, see `tornado.platform.epoll.EPollIOLoop`
    (Linux), `tornado.platform.kqueue.KQueueIOLoop` (BSD and Mac), or
    `tornado.platform.select.SelectIOLoop` (all platforms).
    """
    def initialize(self, impl, time_func=None):
        super(PollIOLoop, self).initialize()
        self._impl = impl
        if hasattr(self._impl, 'fileno'):
            set_close_exec(self._impl.fileno())
        self.time_func = time_func or time.time
        self._handlers = {}
        self._events = {}
        self._callbacks = []
        self._callback_lock = threading.Lock()
        self._timeouts = []
        self._cancellations = 0
        self._running = False
        self._stopped = False
        self._closing = False
        self._thread_ident = None
        self._blocking_signal_threshold = None
        self._timeout_counter = itertools.count()

        # Create a pipe that we send bogus data to when we want to wake
        # the I/O loop when it is idle
        self._waker = Waker()
        self.add_handler(self._waker.fileno(),
                         lambda fd, events: self._waker.consume(),
                         self.READ)

    def close(self, all_fds=False):
        with self._callback_lock:
            self._closing = True
        self.remove_handler(self._waker.fileno())
        if all_fds:
            for fd, handler in self._handlers.values():
                self.close_fd(fd)
        self._waker.close()
        self._impl.close()
        self._callbacks = None
        self._timeouts = None

    def add_handler(self, fd, handler, events):
        fd, obj = self.split_fd(fd)
        self._handlers[fd] = (obj, stack_context.wrap(handler))
        self._impl.register(fd, events | self.ERROR)

    def update_handler(self, fd, events):
        """
        update_handler用于更新主循环中已存在的socket响应事件, 
        接受两个参数: 
            fd 是socket对应的文件描述符 
            events 是注册的新事件
        """
        fd, obj = self.split_fd(fd)
        self._impl.modify(fd, events | self.ERROR)

    def remove_handler(self, fd):
        """
        移除主循环中已存在的socket
        """
        fd, obj = self.split_fd(fd)
        self._handlers.pop(fd, None)
        self._events.pop(fd, None)
        try:
            self._impl.unregister(fd)
        except Exception:
            gen_log.debug("Error deleting fd from IOLoop", exc_info=True)

    def set_blocking_signal_threshold(self, seconds, action):
        if not hasattr(signal, "setitimer"):
            gen_log.error("set_blocking_signal_threshold requires a signal module "
                          "with the setitimer method")
            return
        self._blocking_signal_threshold = seconds
        if seconds is not None:
            signal.signal(signal.SIGALRM,
                          action if action is not None else signal.SIG_DFL)

    def start(self):
        if self._running:
            raise RuntimeError("IOLoop is already running")
        self._setup_logging()
        if self._stopped:
            self._stopped = False
            return
        old_current = getattr(IOLoop._current, "instance", None)
        IOLoop._current.instance = self
        self._thread_ident = thread.get_ident()
        self._running = True

        # signal.set_wakeup_fd closes a race condition in event loops:
        # a signal may arrive at the beginning of select/poll/etc
        # before it goes into its interruptible sleep, so the signal
        # will be consumed without waking the select.  The solution is
        # for the (C, synchronous) signal handler to write to a pipe,
        # which will then be seen by select.
        #
        # In python's signal handling semantics, this only matters on the
        # main thread (fortunately, set_wakeup_fd only works on the main
        # thread and will raise a ValueError otherwise).
        #
        # If someone has already set a wakeup fd, we don't want to
        # disturb it.  This is an issue for twisted, which does its
        # SIGCHILD processing in response to its own wakeup fd being
        # written to.  As long as the wakeup fd is registered on the IOLoop,
        # the loop will still wake up and everything should work.
        old_wakeup_fd = None
        if hasattr(signal, 'set_wakeup_fd') and os.name == 'posix':
            # requires python 2.6+, unix.  set_wakeup_fd exists but crashes
            # the python process on windows.
            try:
                old_wakeup_fd = signal.set_wakeup_fd(self._waker.write_fileno())
                if old_wakeup_fd != -1:
                    # Already set, restore previous value.  This is a little racy,
                    # but there's no clean get_wakeup_fd and in real use the
                    # IOLoop is just started once at the beginning.
                    signal.set_wakeup_fd(old_wakeup_fd)
                    old_wakeup_fd = None
            except ValueError:  # non-main thread
                pass

        try:
            while True:
                # Prevent IO event starvation by delaying new callbacks
                # to the next iteration of the event loop.
                with self._callback_lock:
                    callbacks = self._callbacks
                    self._callbacks = []

                # Add any timeouts that have come due to the callback list.
                # Do not run anything until we have determined which ones
                # are ready, so timeouts that call add_timeout cannot
                # schedule anything in this iteration.
                due_timeouts = []
                if self._timeouts:
                    now = self.time()
                    while self._timeouts:
                        if self._timeouts[0].callback is None:
                            # The timeout was cancelled.  Note that the
                            # cancellation check is repeated below for timeouts
                            # that are cancelled by another timeout or callback.
                            heapq.heappop(self._timeouts)
                            self._cancellations -= 1
                        elif self._timeouts[0].deadline <= now:
                            due_timeouts.append(heapq.heappop(self._timeouts))
                        else:
                            break
                    if (self._cancellations > 512
                            and self._cancellations > (len(self._timeouts) >> 1)):
                        # Clean up the timeout queue when it gets large and it's
                        # more than half cancellations.
                        self._cancellations = 0
                        self._timeouts = [x for x in self._timeouts
                                          if x.callback is not None]
                        heapq.heapify(self._timeouts)

                for callback in callbacks:
                    self._run_callback(callback)
                for timeout in due_timeouts:
                    if timeout.callback is not None:
                        self._run_callback(timeout.callback)
                # Closures may be holding on to a lot of memory, so allow
                # them to be freed before we go into our poll wait.
                callbacks = callback = due_timeouts = timeout = None

                if self._callbacks:
                    # If any callbacks or timeouts called add_callback,
                    # we don't want to wait in poll() before we run them.
                    poll_timeout = 0.0
                elif self._timeouts:
                    # If there are any timeouts, schedule the first one.
                    # Use self.time() instead of 'now' to account for time
                    # spent running callbacks.
                    poll_timeout = self._timeouts[0].deadline - self.time()
                    poll_timeout = max(0, min(poll_timeout, _POLL_TIMEOUT))
                else:
                    # No timeouts and no callbacks, so use the default.
                    poll_timeout = _POLL_TIMEOUT

                if not self._running:
                    break

                if self._blocking_signal_threshold is not None:
                    # clear alarm so it doesn't fire while poll is waiting for
                    # events.
                    signal.setitimer(signal.ITIMER_REAL, 0, 0)

                try:
                    event_pairs = self._impl.poll(poll_timeout)
                except Exception as e:
                    # Depending on python version and IOLoop implementation,
                    # different exception types may be thrown and there are
                    # two ways EINTR might be signaled:
                    # * e.errno == errno.EINTR
                    # * e.args is like (errno.EINTR, 'Interrupted system call')
                    if errno_from_exception(e) == errno.EINTR:
                        continue
                    else:
                        raise

                if self._blocking_signal_threshold is not None:
                    signal.setitimer(signal.ITIMER_REAL,
                                     self._blocking_signal_threshold, 0)

                # Pop one fd at a time from the set of pending fds and run
                # its handler. Since that handler may perform actions on
                # other file descriptors, there may be reentrant calls to
                # this IOLoop that update self._events
                self._events.update(event_pairs)
                while self._events:
                    fd, events = self._events.popitem()
                    try:
                        fd_obj, handler_func = self._handlers[fd]
                        handler_func(fd_obj, events)
                    except (OSError, IOError) as e:
                        if errno_from_exception(e) == errno.EPIPE:
                            # Happens when the client closes the connection
                            pass
                        else:
                            self.handle_callback_exception(self._handlers.get(fd))
                    except Exception:
                        self.handle_callback_exception(self._handlers.get(fd))
                fd_obj = handler_func = None

        finally:
            # reset the stopped flag so another start/stop pair can be issued
            self._stopped = False
            if self._blocking_signal_threshold is not None:
                signal.setitimer(signal.ITIMER_REAL, 0, 0)
            IOLoop._current.instance = old_current
            if old_wakeup_fd is not None:
                signal.set_wakeup_fd(old_wakeup_fd)

    def stop(self):
        self._running = False
        self._stopped = True
        self._waker.wake()

    def time(self):
        return self.time_func()

    def call_at(self, deadline, callback, *args, **kwargs):
        timeout = _Timeout(
            deadline,
            functools.partial(stack_context.wrap(callback), *args, **kwargs),
            self)
        heapq.heappush(self._timeouts, timeout)
        return timeout

    def remove_timeout(self, timeout):
        # Removing from a heap is complicated, so just leave the defunct
        # timeout object in the queue (see discussion in
        # http://docs.python.org/library/heapq.html).
        # If this turns out to be a problem, we could add a garbage
        # collection pass whenever there are too many dead timeouts.
        timeout.callback = None
        self._cancellations += 1

    def add_callback(self, callback, *args, **kwargs):
        with self._callback_lock:
            if self._closing:
                raise RuntimeError("IOLoop is closing")
            list_empty = not self._callbacks
            self._callbacks.append(functools.partial(
                stack_context.wrap(callback), *args, **kwargs))
            if list_empty and thread.get_ident() != self._thread_ident:
                # If we're in the IOLoop's thread, we know it's not currently
                # polling.  If we're not, and we added the first callback to an
                # empty list, we may need to wake it up (it may wake up on its
                # own, but an occasional extra wake is harmless).  Waking
                # up a polling IOLoop is relatively expensive, so we try to
                # avoid it when we can.
                self._waker.wake()

    def add_callback_from_signal(self, callback, *args, **kwargs):
        with stack_context.NullContext():
            if thread.get_ident() != self._thread_ident:
                # if the signal is handled on another thread, we can add
                # it normally (modulo the NullContext)
                self.add_callback(callback, *args, **kwargs)
            else:
                # If we're on the IOLoop's thread, we cannot use
                # the regular add_callback because it may deadlock on
                # _callback_lock.  Blindly insert into self._callbacks.
                # This is safe because the GIL makes list.append atomic.
                # One subtlety is that if the signal interrupted the
                # _callback_lock block in IOLoop.start, we may modify
                # either the old or new version of self._callbacks,
                # but either way will work.
                self._callbacks.append(functools.partial(
                    stack_context.wrap(callback), *args, **kwargs))


class _Timeout(object):
    """An IOLoop timeout, a UNIX timestamp and a callback"""

    # Reduce memory overhead when there are lots of pending callbacks
    __slots__ = ['deadline', 'callback', 'tiebreaker']

    def __init__(self, deadline, callback, io_loop):
        if not isinstance(deadline, numbers.Real):
            raise TypeError("Unsupported deadline %r" % deadline)
        self.deadline = deadline
        self.callback = callback
        self.tiebreaker = next(io_loop._timeout_counter)

    # Comparison methods to sort by deadline, with object id as a tiebreaker
    # to guarantee a consistent ordering.  The heapq module uses __le__
    # in python2.5, and __lt__ in 2.6+ (sort() and most other comparisons
    # use __lt__).
    def __lt__(self, other):
        return ((self.deadline, self.tiebreaker) <
                (other.deadline, other.tiebreaker))

    def __le__(self, other):
        return ((self.deadline, self.tiebreaker) <=
                (other.deadline, other.tiebreaker))


class PeriodicCallback(object):
    """Schedules the given callback to be called periodically.

    The callback is called every ``callback_time`` milliseconds.

    `start` must be called after the `PeriodicCallback` is created.
    """
    def __init__(self, callback, callback_time, io_loop=None):
        self.callback = callback
        if callback_time <= 0:
            raise ValueError("Periodic callback must have a positive callback_time")
        self.callback_time = callback_time
        self.io_loop = io_loop or IOLoop.current()
        self._running = False
        self._timeout = None

    def start(self):
        """Starts the timer."""
        self._running = True
        self._next_timeout = self.io_loop.time()
        self._schedule_next()

    def stop(self):
        """Stops the timer."""
        self._running = False
        if self._timeout is not None:
            self.io_loop.remove_timeout(self._timeout)
            self._timeout = None

    def _run(self):
        if not self._running:
            return
        try:
            return self.callback()
        except Exception:
            self.io_loop.handle_callback_exception(self.callback)
        finally:
            self._schedule_next()

    def _schedule_next(self):
        if self._running:
            current_time = self.io_loop.time()
            while self._next_timeout <= current_time:
                self._next_timeout += self.callback_time / 1000.0
            self._timeout = self.io_loop.add_timeout(self._next_timeout, self._run)
