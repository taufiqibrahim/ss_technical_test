import random

scale = 7646026188
# scale = 76460
max_age = 117
chunk_size = 100000000
out_file = './data/age.txt'


def gen_random_age():
    ages = [int(random.random() * max_age) + 1 for o in range(chunk_size)]
    with open(out_file, 'a') as f:
        for age in ages:
            f.write("%s\n" % age)


def main():
    loop = int(scale / chunk_size) + 1
    for l in range(loop):
        print('Loop %s' % l)
        gen_random_age()


if __name__ == '__main__':
    main()
