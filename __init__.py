#
# 用于并行化处理文本数据的库。
# 其中并行部分使用的是multiprocessing库
#
# ParallelLineProcess/__init__.py
#
# 这个包的目的是加快文本数据的处理速度。通过并行化的方式，由预加载模块，数据分发器，处理线程池，数据收集器组成。
# 通过并行化加快文本数据的处理效率。尽可能多地利用现代处理器多核心的特性。
#
# 早期的版本期望借助CPU的并行能力。后期版本可能添加基于CUDA的硬件加速方法，更快地提高文件的处理速度
#
# Copyright (c) 2020-2030 张景旭
# Licensed to GPL under a Contributor Agreement.
#

import sys

#
# Copy stuff from default context
#

__all__ = [x for x in dir(context._default_context) if not x.startswith('_')]
globals().update((name, getattr(context._default_context, name)) for name in __all__)

#
# XXX These should not really be documented or public.
#

SUBDEBUG = 5
SUBWARNING = 25

#
# Alias for main module -- will be reset by bootstrapping child processes
#

if '__main__' in sys.modules:
    sys.modules['__parallel_line_process__'] = sys.modules['__main__']
