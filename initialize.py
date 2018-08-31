import os
import time
import datetime
# import sys

# USER_PREF = sys.argv[1]
# PRODUCT = sys.argv[2]

UID = 1999
MEM_SIZE = '10M'
# USER_PREF = '/home/produk/user_preference.txt'
USER_PREF = '/home/produk/lite.txt'
PRODUCT_SCORE = '/home/produk/production/jupyter/synth/results/product_score.txt'


class InitDB(object):
    """
    Initialize the database and perform indexing
    """

    def __init__(self, user_preference, product_score, mem_size):
        super(InitDB, self).__init__()
        self.mem_size = mem_size
        self.multiplier_const = 0.95
        self.user_preference = user_preference if user_preference else '/home/produk/user_preference.txt'
        self.user_preference_idx = [[0, 0, 0, ]]
        self.user_preference_idx_dic = {0: [0, 0, ]}
        self.idx_user_preference = '/home/produk/index_user_preference.txt'
        self.product_score = product_score if product_score else '/home/produk/production/jupyter/synth/results/product_score.txt'

    def parse_mem(self, mem_size='10M'):
        if ('k' in self.mem_size.lower()):
            self.mem_size_int = int(self.mem_size[:-1]) * 1024
        elif ('m' in self.mem_size.lower()):
            self.mem_size_int = int(self.mem_size[:-1]) * 1024 * 1024
        elif ('g' in self.mem_size.lower()):
            self.mem_size_int = int(self.mem_size[:-1]) * 1024 * 1024 * 1024

        return self.mem_size_int

    def get_chunks(self, file_size, chunk_size):
        chunk_start = 0
        while chunk_start + chunk_size < file_size:
            yield(chunk_start, chunk_size)
            chunk_start += chunk_size

        final_chunk_size = file_size - chunk_start
        yield(chunk_start, final_chunk_size)

    def load_product_score(self):
        # Load product score table into memory (since it's small)
        with open(self.product_score) as f:
            # generate product score as tuple of tuples
            as_tup = tuple(self.format_col(line, 'product_score')
                           for line in f.readlines()[1:])

            # convert product score into dictionary, it's hashed so it's faster
            # https://stackoverflow.com/a/11241481
            self.t_product_score = {p[0]: p[1] for p in as_tup}

            # debug
            # print(self.t_product_score)

    def format_col(self, line, table):
        if table == 'product_score':
            return tuple(map(int, line.strip().split('\t')))
        elif table == 'user_preference':
            stripped = tuple(line.strip().split('\t'))
            return (int(stripped[0]),
                    int(stripped[1]),
                    float(stripped[2]),
                    int(stripped[3]),
                    )

    def create_index(self, chunk_start, lines):
        index_tmp = list()
        for line in enumerate(lines):
            index_key = line[1][0]
            last_index = self.user_preference_idx[-1]
            rn = chunk_start + line[0]
            try:
                if index_key == int(last_index[0]):
                    index_tmp = [last_index[0],
                                 last_index[1], last_index[2] + 1, ]
                    self.user_preference_idx[-1] = index_tmp
                    self.user_preference_idx_dic[index_key] = index_tmp[1:]
                elif index_key > int(last_index[0]):
                    index_tmp = [index_key, rn, rn, ]
                    self.user_preference_idx.append(index_tmp)
                    self.user_preference_idx_dic[index_key] = [rn, rn, ]

            except Exception as e:
                print(line[1])
                raise e

        # print(self.user_preference_idx_dic[55])
    def write_index(self):
        print('Writing index...')
        with open(self.idx_user_preference, 'a') as f:
            for k, v in self.user_preference_idx_dic.items():
                line = "{}\t{}\t{}\n".format(k, v[0], v[1])
                f.write(line)

    def indexing_user_preference(self):
        # Read user preference table
        with open(self.user_preference, 'r') as f:

            file_size = os.path.getsize(self.user_preference)
            print('Incoming file size %s bytes' % file_size)

            mem_size_int = self.parse_mem(self.mem_size)
            chunks = self.get_chunks(file_size, mem_size_int)
            counter = 1
            for chunk_start, chunk_size in chunks:
                if counter == 1:
                    lines = tuple(self.format_col(line, 'user_preference')
                                  for line in f.readlines(chunk_size)[1:])
                else:
                    lines = tuple(self.format_col(line, 'user_preference')
                                  for line in f.readlines(chunk_size))
                self.create_index(chunk_start, lines)
                counter += 1

    def load_user_preference(self, uid, startrow, endrow):
        the_range = list(range(startrow + 1, endrow + 2))
        with open(self.user_preference, 'r') as f:

            as_tup = tuple(self.calc_user_preference(line)
                           for rn, line in enumerate(f)
                           if rn in the_range)
            as_dict = {p[1]: (p[0], p[4], ) for p in as_tup}

    def load_index(self):
        with open(self.idx_user_preference) as f:
            # generate index as tuple of tuples
            as_tup = tuple(self.format_col(line, 'product_score')
                           for line in f.readlines())

            # convert product score into dictionary, it's hashed so it's faster
            # https://stackoverflow.com/a/11241481
            self.t_idx = {p[0]: (p[1], p[2], ) for p in as_tup}
        # print(self.t_idx.get(138))

    def get_recommendation(self):
        pass

    def recommendation_engine(self):
        """
        Based on example, it is clear that the pattern was
        to calculate all [product_score] available LEFT JOIN to [user_preference].
        Because there are some resulting recommended product
        which are not available in [user_preference] table.

        Prove:
            P3 -> 500 * 0.214 + 500 = 607.000
            P1 -> 750 * -0.199 + 750 = 600.750
            P4 -> 0 + 600 = 600.000  << 0 means no user preference to this product
            P5 -> 650 * -0.200 + 650 = 520.000
            P7 -> 0 + 500 = 500.000  << 0 means no user preference to this product
        """
        for k in self.t_idx.items():
            self.load_user_preference(k[0], k[1][0], k[1][1])
            print('done', k[0])

    def calc_user_preference_eff_score(self, line):
        """
        Calculate effective score
        Each uid-pid relationship score is calculated in different time,
        we can't highly trust on old uid-pid score calculation.
        So each relationship is given a score which is equal to calculated score
        times the 0.95 to the power of the number of days between
        when that score is calculated and the current time.
        So if a relationship has a score of 0.8 and was calculated 2 days ago,
        then the final score for this relationship is equal to 0.8 * 0.95^2 = 0.722.
        Note that the very old calculation will be effectively 0.
        """
        delta = datetime.datetime.now() - \
            datetime.datetime.fromtimestamp(line[3])
        eff_score = line[2] * self.multiplier_const ** delta.days

        return line + (eff_score,)

    def calc_user_preference(self, line):
        """
        This is somekind of calculation pipeline. For each user preference data:
        - Strip the column
        - Calculate effective score
        """
        stripped_cols = self.format_col(line, 'user_preference')
        add_effective_score = self.calc_user_preference_eff_score(
            stripped_cols)

        return add_effective_score

    def map_product_to_user_pref(self):
        for p in self.product_score[0:5]:
            pass

    def load_user_preferencebak(self):
        # Read user preference table
        with open(self.user_preference, 'r') as f:

            file_size = os.path.getsize(self.user_preference)
            print('Incoming file size %s bytes' % file_size)

            mem_size_int = self.parse_mem(self.mem_size)
            chunks = self.get_chunks(file_size, mem_size_int)
            counter = 1
            for chunk_start, chunk_size in chunks:
                if counter == 1:
                    lines = tuple(self.calc_user_preference(line)
                                  for line in f.readlines(chunk_size)[1:])
                else:
                    lines = tuple(self.calc_user_preference(line)
                                  for line in f.readlines(chunk_size))
                # lines = f.readlines(chunk_size)
                # self.create_index(chunk_start, lines)
                print(lines[:2])

                # Lookup product score
                for line in lines[:2]:
                    p_score = (self.t_product_score.get(line[1]),)
                    if p_score:
                        line_scored = line + p_score
                    else:
                        line_scored
                counter += 1

def main():
    print('\nInitializing database...')
    i = InitDB(USER_PREF, PRODUCT_SCORE, '10M')

    print('\nIndexing database table [user_preference]...')
    i.indexing_user_preference()
    i.write_index()

    print('\nInitializing database table [product_score]...')
    i.load_product_score()

    print('\nLoad user preference index table...')
    i.load_index()

    print('\nPerforming recommendation calculation...')
    #i.load_user_preference(0, 8969, 9035)
    i.recommendation_engine()


#     print('Initializing database table [user_preference]...')
#     i.load_user_preference()
