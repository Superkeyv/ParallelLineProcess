from unittest import TestCase

from LinePrcessor import ChunkLoader, ParallelLine

import time


class Testchunkloader(TestCase):
    loader = ChunkLoader(input_file_name='sample.vcf', chunk_size=71, use_async=True,
                         with_line_num=True)

    def test_get(self):
        out = open('sample.vcf.test1', 'w')
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
                out.write("{}\n".format(line))

        print("共计读取{}行".format(num_line))

        self.loader.close()
        out.close()


class TestLineProcessor(TestCase):
    def test_process(self):
        lineProcessor = ParallelLine(n_jobs=4, chunk_size=5)

        lineProcessor.run_row(input_file_name='sample.vcf', output_file_name='sample.vcf.test1')

        print("数据处理完毕")

    def test_run_row_gz(self):
        lineProcessor = ParallelLine(n_jobs=4, chunk_size=5)

        lineProcessor.run_row(input_file_name='sample.vcf.gz', output_file_name='sample.vcf.test1')

        print("数据处理完毕")


class TestPQueue(TestCase):
    def test_put(self):
        self.fail()

    def test_get(self):
        self.fail()
