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

