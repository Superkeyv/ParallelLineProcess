from unittest import TestCase

from LinePrcessor import ChunkLoader, ParallelLine

import time

f = open('sample.vcf', 'r')
out = open('sample_test.vcf', 'w')


class Testchunkloader(TestCase):
    loader = ChunkLoader(infile=f, chunk_size=71, use_async=True, with_line_num=True)

    def test_get(self):
        num_line = 0
        epoch = 0

        while True:
            if self.loader.is_eof():
                break
            data = self.loader.get()
            num_line += len(data)

            epoch += 1
            print("epoch:[{}]".format(epoch))

            for line in data:
                out.write("{}".format(line))

        print("共计读取{}行".format(num_line))

        self.loader.close()
        out.close()
        f.close()


class TestLineProcessor(TestCase):
    def test_process(self):
        lineProcessor = ParallelLine(in_file=f, out_file=out, order=True, n_jobs=4, chunk_size=5, with_line_num=False)

        lineProcessor.run_row()

        print("数据处理完毕")
        lineProcessor.__close()
        out.close()
        f.close()

    def test_2(self):
        f2 = open('/home/karma/Sync_folder/code/python/ParallelLineProcess/a.txt', 'r')

        print(f2.name)

        f2.close()
