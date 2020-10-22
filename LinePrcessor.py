from multiprocessing import Process, Pool, Queue
import os
import time
import progressbar as pb
import re
import gzip


def assume_gzip_origin_size(filename, test_bytes=20 * 1024 * 1024):
    """
    该函数用于估计gzip文件的原始大小，通过尝试解压20M数据，比对gz文件读取量和解压出数据的字节数。得到近似压缩比
    通过估算，最后得到gzip文件的近似原始大小
    :param filename: 需要估计的gz文件路径
    :param test_bytes: 需要测试解压出字节数，如果输入-1，代表整个文件解压
    :return:
    """
    assert filename.endswith('.gz'), "输入的文件:{}，不是gzip文件".format(filename)
    file = open(filename, 'rb')
    gz_file = gzip.GzipFile(fileobj=file)

    gz_file.read(test_bytes)

    s1 = gz_file.tell()
    if test_bytes == -1:
        # 这个代表整个压缩文件都解压了
        gz_file.close()
        file.close()
        return s1

    s2 = file.tell()
    gz_file_size = os.path.getsize(filename)
    gz_file.close()
    file.close()
    return gz_file_size * s1 / s2


class ChunkLoader:
    """
    这个是数据预加载器。按照要求进行数据的加载
    这个程序会维持一个加载队列，当主进程处理其他数据时，会使用另外一个进行继续数据的加载

    todo 1 是否可以增加一个迭代器方式的读取
    """

    def __init__(self, input_file_name, chunk_size=1000, use_async=True, with_line_num=False) -> None:
        """
        数据加载器初始化
        :param input_file_name: 需要读取的文件名
        :param chunk_size: 一次加载的行数量
        :param use_async: 是否使用额外线程进行数据的异步加载
        :param with_line_num: 加载的数据List是否包括行号信息，如果True，则返回的List结构为 [(1,"xxx"),(2,"xxx"),(3,"xxx"),...]
        """

        # assert (infile.readable(), "文件无法读取")
        if input_file_name.endswith('.vcf'):
            self.infile = open(input_file_name, 'r')
        elif input_file_name.endswith('.gz'):
            self.infile = gzip.open(input_file_name, 'rt')
        else:
            self.infile = open(input_file_name, 'r')

        self.chunk_size = chunk_size
        self.use_async = use_async
        self.EOF = False  # 代表文件已经读取完毕。该项由读取函数处理，从读取完毕得到[]作为标志触发
        self.with_line_num = with_line_num

        # 用于进程间数据共享
        self.ram_cache = Queue(maxsize=1)  # 最大容量为1的队列
        self.LineNum = 0  # 记录已经传出数据的行号

    def __read_a_chunk(self):
        """
        读取一个chunk行的文本。如果已经读取到文件的末尾，那么再次调用本方法将会返回[]
        读取的行将会清除掉'\n'符号

        :return: 注意，这里的数据不能进行二级包装。这是由于Queue数据的引用方式造成的
        """
        line_datas = []
        # 对行数据进一步处理
        for i in range(self.chunk_size):
            line = self.infile.readline()
            if line == '':
                break
            # 清除掉换行符
            line = line.strip('\r\n')
            line = line.strip('\n')
            line_datas.append(line)

        return line_datas

    def __read_async(self):
        """
        异步读取的附加线程主循环
        :return:
        """
        while True:
            # 开始进行缓存，如果文件已经到了EOF，那么之后的读取都会是tmp=[]
            tmp = self.__read_a_chunk()
            self.ram_cache.put(tmp, block=True)  # 阻塞式的数据入队

            # 文件是否读取完毕，当读取完毕后，最后一批数据(包括[]值)发送完毕，这里获取EOF状态，并结束进程的运行
            # 这里不能以EOF状态为结束标志，存在情况，最后一次数据没有读满，但EOF=true，这会造成主进程不清楚辅助进程已经结束。因此，这里多读取一次，保证发送一个[]给队列读取者
            if len(tmp) == 0:
                break

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
        # bugfix 2020.10.13，当用户在async模式下，同样使用is_eof来判断文件是否读取完毕时，由于self.EOF是另一个进程完成的，会导致ram_cache还没有排空，但提前终止的现象
        # 此时，另一个进程的self.ram_cache.put()方法是阻塞式的，数据没有读出，因而阻塞。导致数据读出缺失

        # bugfix 2020.10.14，self.EOF由get函数的使用者处理。以__read_a_chunk的返回结果作为判断标志。如果返回[]，代表读取完毕。否则为还需读取
        return self.EOF

    def get(self):
        """返回已经加载好的数据
        :return: 获取行的List形式。如果返回[]，代表数据已经读取完毕
        """
        ret = []

        # 保证文件读取完毕，不会再次进入循环
        if self.EOF:
            return ret

        # 进行读取
        if self.use_async:
            # print('read_async')
            if not hasattr(self, 'process'):
                self.read_async()

            ret = self.ram_cache.get(block=True)
        else:
            # print('read_sync')
            ret = self.read_sync()

        # 判断文件是否读取结束
        if len(ret) == 0:
            self.EOF = True

        # 包装行号
        tmp = []
        if self.with_line_num:
            tmp.append(range(self.LineNum, self.LineNum + len(ret)))
            tmp.append(ret)
            self.LineNum += len(ret)
        else:
            tmp = ret

        return tmp

    def close(self):
        self.infile.close()
        if hasattr(self, 'process'):
            # 等待process的后续任务做完
            self.process.join()
            self.process.close()


def line_proc(data):
    """
    这是一个默认方法，作为ParallelLine的默认行处理方法
    我们将这个方法定义为行处理方法， 是需要被复写的行处理方法。

    几个可以参考的文件格式。
        csv，数值间通过',' ';' 隔开，作为csv文件后缀，可以轻松被支持csv的表格处理程序时别。注意，由于换行方式的不同，数据正确，但是会出现和pandas输出的csv格式不同的情况

    :param line: 输入的行
    :return: 输出的结果，默认是将输入重新传出去。如果返回的是None，则数据不会被写出目标位置
    """

    return data


def chunk2col(data):
    """
    这是一个默认方法，用于将chunk块中多行数据分解为对应的列数据。默认的分隔符为',' ':' ';' '|'，这些分隔符都会被应用
    :return:
    """

    row_list = []  # 存放经过切分的行列表
    col_list = []  # 用于存储处理得到的列数据
    max_col = 0

    for line in data:
        # 去掉换行符
        line.replace('\n', '')
        # 进行字符的切分
        sp = re.split('[:;, |\n]', line)

        # 记录最大行长度
        if len(sp) > max_col:
            max_col = len(sp)

        row_list.append(sp)

    # 准备各列的存储空间
    for i in range(max_col):
        col_list.append([])

    # 将行数据存放给列来使用。
    for j in range(max_col):
        for row in row_list:
            if len(row) < j:
                # 空值处理方法
                col_list[j].append('')
            col_list[j].append(row[j])

    return col_list


def col_proc(data):
    """
    这是一个默认方法，作为ParallelLine的默认列处理方法
    我们将这个方法定义为列处理方法， 是需要被复写的列处理方法。
    :param data:
    :return:
    """

    return data


def file_line_count(fname):
    '''
    用于确认文件有多少行
    :param f: 所打开的文件名
    :return: 该文件存在的行数
    '''
    line_num = 0
    with open(fname, 'r') as f:
        while (True):
            buf = f.read(4 * 1024 * 1024)
            if (buf == ''):
                break

            line_num += buf.count('\n')

        f.close()
    return line_num


class ParallelLine:
    """
    这是文本文件的并行化处理器。
    一般面对的是500M以上的文件。文件的行可能不大，但是每行的长度很大。
    这里直接采用file.read()方法，在处理这种规模的文件时顺序读写具有一定的优势

    文件处理过程中，内存占用是核心问题之一。为了降低内存占用，会考虑使用磁盘缓存的方式进行。缓存文件的使用由线程决定

    """

    def __init__(self, n_jobs=4, chunk_size=100, show_process_status=True) -> None:
        """
        按照行的方式，并行化处理数据的类

        :param n_jobs: 并行数
        :param chunk_size: 用于指定一次性处理的块大小。建议设置为n_jobs的整数倍
        :param show_process_status: 是否展示处理进度
        """

        self.n_jobs = n_jobs
        self.chunk_size = chunk_size
        self.__pool_chunk_size = chunk_size // n_jobs  # 将chunksize的数据均匀地划分给n_jobs个进程。
        self.__show_process_status = show_process_status
        self.__file_cache = {}  # 文件缓存。每个线程都可以创建自己的文件缓存。字典类型。通过进程号对应

    def run_row(self, input_file_name, output_file_name=None, row_func=line_proc, with_line_num=False, order=True,
                use_CRLF=False):
        """
        对文件的行并行化处理，并最终返回

        :param in_file: 待处理的文件名。使用文件名的形式，可以提供多次读取的优势
        :param output_file: 处理完毕需要输出的文件。默认为None，代表文件将会输出到内存中。如果为None，那么数据将会以list的方式，逐行存放，并最后返回
        :param row_func: 用于行处理的方法。方法定义为 def func(data): -> ProcessedLine。其中data部分包含行号信息
        :param with_line_num: 传递给line_func的数据是否包括行号。如果包括行号，那么传递给line_func的数据为 (line_num, line_data)
        :param order: 是否按照有序的方式处理数据。True保证处理的顺序，False允许乱序处理
        :param use_CRLF: 换行模式，由于默认在Linux上运行，换行模式LF为'\n'。在win上，所采用的换行模式CRLF为'\r\n'，即回车换行
        :return: 返回经过处理的结果。如果outfile!=None，那么处理的结果将会直接写入到文件中; 如果outfile=None，这意味着会返回处理List，其中包括经过处理后的所有行
        """

        # 在文件内部打开
        output_file = open(output_file_name, 'w')

        #### 公共展示信息补充 ####
        __cache_mode = 'File'
        if output_file == None:
            __cache_mode = 'Mem'  # 如果没有打开的输出文件，将使用内存作为缓存区

        # 获取输入文夹的大小。
        __in_file_size = 0
        if input_file_name.endswith('.vcf'):
            __in_file_size = os.path.getsize(input_file_name)
        elif input_file_name.endswith('.gz'):
            __in_file_size = assume_gzip_origin_size(input_file_name)

        # 初始化线程池，包括1个预加载器、n_jobs个数据处理器、主进程负责数据的分发、收集和写入
        chunk_loader = ChunkLoader(input_file_name, chunk_size=self.chunk_size, use_async=True,
                                   with_line_num=with_line_num)
        pool = Pool(self.n_jobs)

        #### 参数初始化 ####
        line_breaker = '\n'
        if use_CRLF:
            line_breaker = '\r\n'

        # 列出运行配置
        print("ParallelLine使用配置:")
        prefix = "@run_row:\t"
        print(prefix + "顺序处理={}".format(order))
        print(prefix + "n_jobs={}".format(self.n_jobs))
        print(prefix + "pool_chunksize={}".format(self.__pool_chunk_size))
        print(prefix + "缓存模式={}".format(__cache_mode))
        print(prefix + "展示处理进度={}".format(self.__show_process_status))
        print(prefix + "输入文件大小={} bytes".format(__in_file_size))

        if self.__show_process_status:
            self.progressbar = pb.ProgressBar(maxval=__in_file_size)
            self.progressbar.start()
            self.load_file_size = 0

        # 用于缓存已经处理过的所有行
        ret = []
        while True:

            # 获取一份数据
            data = chunk_loader.get()

            # 展示文件的处理进度
            if self.__show_process_status:
                for line in data:
                    if with_line_num:
                        self.load_file_size += len(line[1])
                    else:
                        self.load_file_size += len(line)
                if self.__show_process_status:
                    if self.load_file_size > __in_file_size:
                        self.load_file_size = __in_file_size
                    self.progressbar.update(self.load_file_size)

            # 加快获取文件末尾的效率
            if len(data) == 0:
                print("处理完毕")
                if self.__show_process_status:
                    self.progressbar.finish()
                break

            # 处理
            if order:
                data1 = pool.imap(row_func, data, chunksize=self.n_jobs)
            else:
                data1 = pool.imap_unordered(row_func, data, chunksize=self.n_jobs)

            # 数据重整，主要是清除返回的数据中存在的None值
            data2 = []
            for res in data1:
                if res == None:
                    continue
                data2.append(res)

            # 返回或写入
            if __cache_mode == 'Mem':
                ret += data2
            else:
                for line in data2:
                    output_file.write('{}{}'.format(line, line_breaker))

        # 处理完毕，这里清除一下信息
        chunk_loader.close()
        pool.close()
        pool.join()

        # 关闭打开的文件
        output_file.close()
        if __cache_mode == 'Mem':
            return ret

    def __run_col(self, input_file_name, output_file_name=None, with_cache_file=True, chunk2col_func=chunk2col,
                  col_func=col_proc,
                  with_column_num=True, use_CRLF=False):
        """
        对文件的列进行处理。（有列意味着必须有列的分割符号，这里需要写一个行构成的块如何转化为列的处理方法)

        数据按照块的方式进行加载。通过chunk2col进行行数据的切分，同时从行的存储模式转变为列的存储模式
        col_func用于处理chunk2col方法得到的chunk块的列形式。

        这个方法下，如果不设置输出文件

        todo 该方法还未完善，不建议使用
        todo 1 使用外部行拆分程序，进行行切割
        todo 2 并行方式集中在多列的并行化处理方面，借助chunkloader多次加载文件，在一个文件读取周期内处理多行。将多行的处理结果批次写入系统
        todo 3 存在模式2,一次读取文件，完成所有列的处理。可以参考各列写缓存的思路，最终将缓存合并，构成最终文件
        todo 4 需要准备3个方法，行的分割方法，chunk行块的处理方法，chunk行块返回结果的合并方法。（预处理、处理、后处理）

        :param input_file_name: 待处理的文件名
        :param output_file_name: 处理完毕需要输出的文件名。默认为None，代表文件将会输出到内存中。如果为None，那么数据将会以list的方式，逐行存放，并最后返回
        :param with_cache_file: 由于文件体积极大的时候，无法直接将列数据连续存放。因此需要考虑使用临时文件存放的方式
        :param chunk2col_func: 将一个块中的行数据转换切分为列的方式存放
        :param col_func: 用于列数据处理的方法。每次一个列给该方法，列带有对应的列号。得到的参数结构为(col_num, col_value)。返回数据时要求结构为(col_num,pd_col_value)
        :param with_column_num: 传递给line_func的数据是否包括列的编号。如果True，那么传递给col_func的数据为 (col_num, col_data)。注意col_num从1开始
        :param use_CRLF: 换行模式，由于默认在Linux上运行，换行模式LF为'\n'。在win上，所采用的换行模式CRLF为'\r\n'，即回车换行
        :return: 返回经过处理的结果。如果outfile!=None，那么处理的结果将会直接写入到文件中; 如果outfile=None，这意味着会返回处理List，其中包括经过处理后的所有行
        :return:
        """
        input_file = open(input_file_name, 'r')
        output_file = None
        #### 公共展示信息补充 ####
        __cache_mode = 'File'
        if output_file_name == None:
            __cache_mode = 'Mem'  # 如果没有打开的输出文件，将使用内存作为缓存区
        else:
            output_file = open(output_file_name, 'w')

        if __cache_mode == 'File':
            # 创建缓存文件夹
            if not os.path.exists('tmp'):
                os.mkdir('tmp')

        # 获取输入文夹的大小
        __in_file_size = os.path.getsize(input_file.name)

        # 初始化线程池，包括1个预加载器、n_jobs个数据处理器、主进程负责数据的分发、收集和写入
        chunk_loader = ChunkLoader(input_file_name, chunk_size=self.chunk_size, use_async=True,
                                   with_line_num=with_column_num)
        pool = Pool(self.n_jobs)

        #### 参数初始化 ####
        line_breaker = '\n'
        if use_CRLF:
            line_breaker = '\r\n'

        # 列出运行配置
        print("ParallelLine使用配置:")
        prefix = "@run_col:\t"
        print(prefix + "n_jobs={}".format(self.n_jobs))
        print(prefix + "pool_chunksize={}".format(self.__pool_chunk_size))
        print(prefix + "缓存模式={}".format(__cache_mode))
        print(prefix + "缓存文件器用={} bytes".format(with_cache_file))
        print(prefix + "展示处理进度={}".format(self.__show_process_status))
        print(prefix + "输入文件大小={} bytes".format(__in_file_size))

        ret = []
        # 处理到最后，ret中最多i几行，最多几列
        ret_rows = 0
        ret_cols = 0

        while True:
            data = chunk_loader.get()

            # 加快获取文件末尾的效率
            if len(data) == 0:
                print("处理完毕")
                break

            # 行数据转换为切分过的列数据
            data1 = chunk2col_func(data)
            # 为每列添加列编号
            data2 = []
            for i, col in data1:
                data2.append((i + 1, col))

            # 按照col_func方法并行处理列数据。这里有可能是数据写入。需要外部提供方法指定
            data2 = pool.imap(col_func, data2, chunksize=self.n_jobs)

            # data2转存，同时更新ret_cols和ret_rows
            data3 = []
            for col_c in data2:
                assert len(col_c) == 2, "col_func方法的返回结果长度应该为2,收到错误的返回长度"
                col_num = col_c[0]
                col = col_c[1]
                if ret_rows < len(col):
                    ret_rows = len(col)
                data3.append(col)

            if ret_cols < len(data3):
                ret_cols = len(data3)

            # 进行数据的合并
            if __cache_mode == 'Mem':
                # 如果缓冲模式是内存缓冲，那么将数据写给ret。ret保存有每个chunk的数据，经过其他步骤整合得到最终的结果
                ret.append(data3)
            else:
                # 行数据写入到缓存文件中，其中文件名就是行号
                for (col_num, col_value) in data2:
                    with open('tmp/{}'.format(col_num), 'a+') as f:
                        f.write(col_value)

        # 处理完毕，这里清除一下信息
        chunk_loader.close()
        pool.close()
        pool.join()
        if __cache_mode == 'Mem':
            # 将ret中的数据进行整合，最终符合列关系
            tmp = []
            # 构造所有的列
            for i in range(ret_cols):
                tmp.append([])

            for chunk in ret:
                c_chunk_max_row = 0

                # 获取当前chunk的最大行数，用于后续数据的补齐工作
                for col in chunk:
                    if c_chunk_max_row < len(col):
                        c_chunk_max_row = len(col)

                for col_num in range(ret_cols):
                    # 如果列号超过了数据范围，那么主动补充空值
                    if col_num > len(chunk):
                        for i in range(c_chunk_max_row):
                            tmp[col_num].append('')
                        continue

                    for row_num in range(c_chunk_max_row):
                        if row_num > len(chunk[col_num]):
                            tmp[col_num].append('')
                        tmp[col_num].append(chunk[col_num][row_num])

            return tmp
        else:
            # 对应处理__cache_mode == 'file'
            for i in range(1, ret_cols + 1):
                with open('tmp/{}'.format(i), 'r'):
                    output_file.write('\n')

        input_file.close()
        if output_file != None:
            output_file.close()

    def __run_block(self, input_file_name, output_file_name, block_size=(0, 0), split_func=chunk2col,
                    block_func=col_proc, use_CRLF=False):
        """
        这个是新的并行处理思路。内容包括数据的读取，任务分发，结果聚合写入。
        核心思想是突出数据的块化处理 ，将data_loader的chunk块进一步按照列进行切分。得到许多小block，每个小block结构为([row_nums],[col_nums],[data])

        :param input_file_name: 待处理的文件名称
        :param output_file_name: 结果输出的文件名
        :param block_size: 将表格拆分成的block大小
        :param split_func: 每一行数据的具体拆分方法
        :param block_func: 每一个block的处理方法
        :param use_CRLF: 写出数据是否采用CRLF方式进行换行
        :return:
        """

        #### 参数初始化 ####
        line_breaker = '\n'
        if use_CRLF:
            line_breaker = '\r\n'

    # Press the green button in the gutter to run the script.
    if __name__ == '__main__':
        print("hi")
