import LinePrcessor

# 碱基配对字典
BasePair = {'A': 'T',
            'T': 'A',
            'C': 'G',
            'G': 'C'}

ignore_ALTs = 0
loss_replace = '-'


def line_process(line):
    # 处理文件头
    if (line[0:2] == '##'):
        return

    if (line[0:2] == '#C'):
        # 这里是第一行，可以作为头部
        head = line.split()
        title = 'CHROM,POS'

        s_index = 0
        for i, col_name in enumerate(head):
            if (col_name[0] == 's'):
                s_index = i
                break

        for col in head[s_index:]:
            title += ',{}'.format(col)
        return title

    # 解析处理单行数据
    s_data = line.split()

    # 处理ALT特殊情况，当变异类型超过opt.ignore_ALTs时，忽略当前行的记录
    if (ignore_ALTs > 0 and ignore_ALTs < len(s_data[4].split(','))):
        return

    # 记录染色体，SNP位点位置
    row = s_data[0] + ',' + s_data[1]

    # 准备参考碱基与对应碱基
    REF = s_data[3]
    RREF = BasePair[REF]

    # 从9列到最后，进行遍历处理
    for i in range(9, len(s_data)):
        s = s_data[i][0:3]

        GT = ''  # 当前样本该位置的基因型
        if (s == './.'):
            GT = loss_replace * 2
        else:
            # 在没有缺失的情况下，解析基因型
            if (s[0] == '0'):
                GT += REF
            else:
                GT += RREF

            if (s[2] == '0'):
                GT += REF
            else:
                GT += RREF

        row += ',' + GT

    return row

src = open('a.txt', 'r')
out = open('a.txt.csv', 'w')

pline = LinePrcessor.ParallelLine(n_jobs=4, chunk_size=100, show_process_status=True)

pline.run_row(input_file=src, output_file=out, row_func=line_process, use_CRLF=True)
