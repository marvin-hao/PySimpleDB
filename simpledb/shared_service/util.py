from __future__ import print_function
__author__ = 'Marvin'

import _thread
import threading
import types


def synchronized_with_attr(lock_name):
    def decorator(method):
        def synced_method(self, *args, **kws):
            lock = getattr(self, lock_name)
            with lock:
                return method(self, *args, **kws)

        return synced_method

    return decorator


def synchronized_with(lock):
    def synchronized_obj(obj):

        if isinstance(obj, types.FunctionType):

            obj.__lock__ = lock

            def func(*args, **kws):
                with lock:
                    return obj(*args, **kws)

            return func

        elif isinstance(obj, type):

            orig_init = obj.__init__

            def __init__(self, *args, **kws):
                self.__lock__ = lock
                orig_init(self, *args, **kws)

            obj.__init__ = __init__

            for key in obj.__dict__:
                val = obj.__dict__[key]
                if isinstance(val, types.FunctionType):
                    decorator = synchronized_with(lock)
                    obj.__dict__[key] = decorator(val)

            return obj

    return synchronized_obj


def synchronized(item):
    if isinstance(item, str):
        decorator = synchronized_with_attr(item)
        return decorator(item)

    if isinstance(item, _thread.LockType):
        decorator = synchronized_with(item)
        return decorator(item)

    else:
        new_lock = threading.Lock()
        decorator = synchronized_with(new_lock)
        return decorator(item)


def java_string_hashcode(s):
    """
    reimplemented hashCode() function in Java
    :return: hash code of string s
    """
    h = 0
    for c in s:
        h = (31*h+ord(c)) & 0xFFFFFFFF
    return ((h+0x80000000) & 0xFFFFFFFF) - 0x80000000


