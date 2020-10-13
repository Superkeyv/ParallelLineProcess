from multiprocessing import Manager, Process, Pool, Value, Queue, Lock
import sys
import time


class ChunkLoader:
    """
    这个是数据预加载器。按照要求进行数据的加载
    这个程序会维持一个加载队列，当主进程处理其他数据时，会使用另外一个进行继续数据的加载
    """

    def __init__(self, infile, chunk_size=1000, use_async=True, with_line_num=False) -> None:
        """
        数据加载器初始化
        :param infile: 需要读取的文件
        :param chunk_size: 一次加载的行数量
        :param use_async: 是否使用额外线程进行数据的异步加载
        :param with_line_num: 加载的数据List是否包括行号信息，如果True，则返回的List结构为 [(1,"xxx"),(2,"xxx"),(3,"xxx"),...]
        """

        # assert (infile.readable(), "文件无法读取")

        self.infile = infile
        self.chunk_size = chunk_size
        self.use_async = use_async
        self.EOF = False  # 代表文件已经读取完毕

        # if with_line_num:
        #     self.with_line_num = Value('b', 1)
        # else:
        #     self.with_line_num = Value('b', 0)
        self.with_line_num = with_line_num

        # 用于进程间数据共享
        self.ram_cache = Queue(maxsize=1)  # 最大容量为1的队列
        self.LineNum = Value('l', 0)  # 记录当前读取的行号

    def __read_a_chunk(self):
        """
        读取一个chunk行的文本
        :return:
        """
        tmp = []
        for i in range(self.chunk_size):
            line = self.infile.readline()
            if line == '':
                self.EOF = True
                break
            self.LineNum.value += 1

            if self.with_line_num:
                tmp.append((self.LineNum.value, line))
            else:
                tmp.append(line)
        return tmp

    def __read_async(self):
        """
        读取一个
        :return:
        """
        while True:
            # 文件是否读取完毕
            if self.EOF:
                # 最后发送一个空文件作为信号，代表数据已经读完
                self.ram_cache.put(None, block=True)  # 阻塞式等待最后一次被读取
                break

            # 开始进行缓存
            tmp = self.__read_a_chunk()
            self.ram_cache.put(tmp, block=True)  # 阻塞式的数据入队

    def read_async(self):
        """
        异步的数据读取方法。通过self.process执行
        :return:
        """
        self.process = Process(target=self.__read_async, name='co_thread@chunkloader')
        self.process.start()

    def read_sync(self):
        """
        同步的数据读取方法
        :return:
        """
        return self.__read_a_chunk()

    def is_eof(self):
        """
        文件是否读取完毕
        :return:
        """

        # if self.EOF.value is 0:
        # bugfix，当用户在async模式下，同样使用is_eof来判断文件是否读取完毕时，由于self.EOF是另一个进程完成的，会导致ram_cache还没有排空，但提前终止的现象
        # 此时，另一个进程的self.ram_cache.put()方法是阻塞式的，数据没有读出，因而阻塞。导致数据读出缺失
        return self.EOF

    def get(self):
        """返回已经加载好的数据
        :return: 获取行的List形式。如果读取完毕，则会返回[]
        """
        ret = []

        # 进行读取
        if self.use_async:
            # print('read_async')
            if not hasattr(self, 'process'):
                self.read_async()

            ret = self.ram_cache.get(block=True)

            if ret is None:
                self.EOF = True
                ret = []

        else:
            # print('read_sync')
            ret = self.read_sync()

        return ret

    def close(self):
        if hasattr(self, 'process'):
            # 等待process的后续任务做完
            self.process.join()
            self.process.close()


def line_proc(data):
    """
    这是一个默认方法，作为ParallelLine的默认行处理方法

    行处理器， 是需要被复写的行处理方法。

    :param line: 输入的行
    :return: 输出的结果，默认是将输入重新传出去
    """

    return data


class ParallelLine:
    """
    这是文本文件的并行化处理器。
    一般面对的是500M以上的文件。文件的行可能不大，但是每行的长度很大。
    这里直接采用file.read()方法，在处理这种规模的文件时顺序读写具有一定的优势

    文件处理过程中，内存占用是核心问题之一。为了降低内存占用，会考虑使用磁盘缓存的方式进行。缓存文件的使用由线程决定

    """

    def __init__(self, infile, outfile=None, line_func=line_proc, order=False, n_jobs=4,
                 with_line_num=False, chunk_size=100) -> None:
        """
        按照行的方式，并行化处理数据的类

        :param infile: 待处理的文件
        :param outfile: 处理完毕需要输出的文件。默认为None，代表文件将会输出到内存中。如果为None，那么数据将会以list的方式，逐行存放，并最后返回
        :param line_func: 用于行处理的方法。方法定义为 def func(data): -> ProcessedLine。其中data部分包含行号信息
        :param order: 数据的行顺序是否保持不变。True代表行顺序不变，False代表行顺序无所谓
        :param n_jobs: 并行数
        :param with_line_num: 传递给line_func的数据是否包括行号。如果包括行号，那么传递给line_func的数据为 (line_num, line_data)
        :param chunk_size: 用于指定一次性处理的块大小。建议设置为n_jobs的整数倍
        """

        self.line_func = line_func
        self.infile = infile
        self.outfile = outfile
        self.order = order
        self.n_jobs = n_jobs
        self.__file_cache = {}  # 文件缓存。每个线程都可以创建自己的文件缓存。字典类型。通过进程号对应

        if self.outfile is None:
            self._cache_mode = 'Mem'  # 如果没有打开的输出文件，将使用内存作为缓存区
        else:
            self._cache_mode = 'File'

        # 初始化线程池，包括1个预加载器、n_jobs个数据处理器、主进程负责数据的分发、收集和写入
        self.chunk_loader = ChunkLoader(infile=self.infile, chunk_size=chunk_size, use_async=True,
                                        with_line_num=with_line_num)
        self.pool = Pool(self.n_jobs)

    def run_row(self):
        """
        对文件进行并行化处理，并最终返回
        :return: 返回经过处理的结果。如果outfile=None，这意味着会返回处理List，其中包括经过处理后的所有行
        """

        # 用于缓存已经处理过的所有行
        ret = []
        epoch = 1
        while True:
            if self.chunk_loader.is_eof():
                break
            # 获取一份数据
            data = self.chunk_loader.get()

            # 处理
            print("并行化处理")
            if self.order:
                data1 = self.pool.imap(self.line_func, data, chunksize=self.n_jobs)
            else:
                data1 = self.pool.imap_unordered(self.line_func, data, chunksize=self.n_jobs)

            print('epoch:[{}]'.format(epoch))
            epoch += 1

            # 返回或写入
            print("返回的数据进行处理")
            if self._cache_mode == 'Mem':
                ret += data1
            else:
                for line in data1:
                    self.outfile.write('{}'.format(line))

            print("处理下一批次数据\n\n")

        if self._cache_mode == 'Mem':
            return ret

    def close(self):
        """
        执行关闭操作
        :return:
        """
        self.chunk_loader.close()

        # 等待pool任务执行完毕
        self.pool.close()
        self.pool.join()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print("hi")
