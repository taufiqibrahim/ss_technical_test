import os
import argparse


def clear_bins(sort_dir):
    for f in os.listdir(sort_dir):
        if f.startswith('bin_crazy_sort_'):
            os.remove(os.path.join(sort_dir, f))


def writer(sort_dir, line):
    # pass
    bin_file = os.path.join(sort_dir, 'bin_crazy_sort_%s.txt' % line)
    # bin_file = os.path.join(sort_dir, 'bin_crazy_sort_%s.txt' % line)
    # with open(bin_file, 'a') as b: v[]
    #     b.write("%s/n" % line)


def update_bins(sort_dir, ages):
    pass
    [writer(sort_dir, age.strip()) for age in ages]


def get_chunks(file_size, chunk_size):
    chunk_start = 0
    while chunk_start + chunk_size < file_size:
        yield(chunk_start, chunk_size)
        chunk_start += chunk_size

    final_chunk_size = file_size - chunk_start
    yield(chunk_start, final_chunk_size)


def parse_mem(memsize):
    if ('k' in memsize.lower()):
        memsizeint = int(memsize[:-1]) * 1024
    elif ('m' in memsize.lower()):
        memsizeint = int(memsize[:-1]) * 1024 * 1024
    elif ('g' in memsize.lower()):
        memsizeint = int(memsize[:-1]) * 1024 * 1024 * 1024

    return int(memsizeint)


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('filename',
                        help='filepath for to be sorted')
    parser.add_argument('memsize',
                        default='64k',
                        help='memory size')
    args = parser.parse_args()
    sort_dir = os.path.dirname(args.filename)

    # Get file size
    file_size = os.path.getsize(args.filename)
    print('Incoming file size %s bytes' % file_size)
    print('Sort using %s of memory...' % args.memsize)

    clear_bins(sort_dir)
    for chunk_start, chunk_size in get_chunks(file_size, parse_mem(args.memsize)):
        with open(args.filename, 'r') as f:
            ages = f.readlines(chunk_size)
            update_bins(sort_dir, ages)
            del ages


if __name__ == '__main__':
    run()
