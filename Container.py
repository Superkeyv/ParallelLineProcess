"""
这个文件实现的是超大数据量下的容器，在有限内存情况下，如果需要访问100G以上的序列数据，通过加载内存的方法无法实现数据的存储。

这里的类通过数据缓冲至磁盘的方式，将部分数据存放在RAM中，剩余的数据以文件形式存放在磁盘中，由此降低内存占用量。

同时通过工作线程预加载的方式，在顺序读写情况下，提升数据的访问效率。

todo 提升随即访问的效率

"""

from multiprocessing import Queue
import time
import os


class PQueue:
    """
    PQueue的作用是面对特大数据时，能够在固定内存使用上限的情况下实现对数据的顺序访问。

    建议使用的类型为：str、int、float这种

    不建议在Queue中嵌入不同类型的数据，同时用list、dict,tuple包装的数据也不能放入本队列
    """

    def __init__(self, mem_limit=10 * 1024 * 1024, cache_dir='tmp', sync=True) -> None:
        """
        超大List的预加载和处理
        :param mem_limit: 该List占用mem的上限，单位为bytes
        :param cache_dir: 缓存文件存放的位置，是目录。一旦数据量超过mem_limit的时候，则会创建文件清空内存
        :param sync: 这个是磁盘加载的同步版本，会启动另一个进程处理该任务。如果内存中的数据已经被用尽了，则会重新到磁盘中加载数据
        """
        super().__init__()
        self.mem_limit_b = mem_limit * 1024 * 1024  # 这里转化为字节单位
        self.mem_used = 0  # 已经使用过的内存大小

        # 私有变量
        self.__c_index = 0  # 当前读取的元素位置
        self.__queue = Queue()  # 这个队列不限容量，需要其他程序配合处理
        self.__queue_mem_used = 0  # 队列使用的容量
        self.use_disk_cache = False  # 是否使用了文件缓存数据
        self.__disk_cache = None  # 本对象使用的磁盘cache对象
        self.__co_worker = None  # 协助任务处理对象，用完即销毁
        self.cache_dir = cache_dir  # 缓存目录
        self.cache_file_name = cache_dir + '/' + 'plist_' + str(time.time())  # 缓存文件路径

    def __del__(self):
        """
        析构函数，这里处理一下缓存文件的问题
        :return:
        """
        # 删除缓存文件
        os.remove(self.cache_file_name)

        # 尝试删除缓存目录
        if len(os.listdir(self.cache_dir)) is 0:
            try:
                os.removedirs(self.cache_dir)
            except:
                return

    def __load_buffer(self):
        """
        用于加载RAM缓冲区
        :return:
        """

    def ___unload_buffer(self):
        """
        用于卸载缓冲区，减少内存的占用
        :return:
        """

    def __cowork_disk2ram(self):

        return self

    def put(self, data):
        """
        向队列压入数据
        :param data:
        :return:
        """

        # 检测内存数据是否存放满了
        if self.mem_used + len(data) > self.mem_limit_b:
            print("内存已经占满，准备迁移数据")
            if not os.path.exists(self.cache_file_name):
                if not os.path.exists(self.cache_dir):
                    os.mkdir(self.cache_dir)
                self.cache_file = open(self.cache_file_name, 'w')

            # 将queue中一半的数据写入磁盘
            for i in range(int(self.__queue.qsize() / 2)):
                item = self.__queue.get()
                self.mem_used -= len(item)
                self.cache_file.write('{}/n'.format(item))

        # 压入数据，并更新内存使用量
        self.__queue.put(data)
        self.mem_used += len(data)

    def put_done(self):
        """
        是一个信号，代表数据已经全部压入，下一步是将RAM中的数据全部清空，写入到磁盘中。
        :return:
        """
        self.__queue.put(None)  # 这里代表数据压入结束
        self.mem_used = 0

        while True:
            item = self.__queue.get()
            if item is None:
                break
            self.cache_file.write('{}/n'.format(item))
        self.cache_file.close()

    def get(self):
        """
        获取队头的数据
        :return:
        """
        ret = None  # 要返回的数据

        if self.use_disk_cache:
            # 使用了磁盘缓存
            print("从磁盘中重新加载数据")

            if self.cache_file.closed:
                self.cache_file = open(self.cache_file_name, 'r')




        else:
            ret = self.__queue.get()

        return ret

    def __iter__(self):
        """
        先设置一些初始化参数，然后返回迭代器对象，也就是拥有__next__方法的自己
        :return:
        """
        self.__c_index = 0
        if self.use_disk_cache:
            print("从新把磁盘中的数据加载到ram，然后开始迭代")

        return self

    def __next__(self):
        """
        迭代器部分，用于遍历全部的数据
        :return:
        """
        return self.get()


class PList:
    """
    容纳超过100G数据的List
    """

    def __init__(self) -> None:
        super().__init__()

    def __iter__(self):
        return self

    def __next__(self):
        return
