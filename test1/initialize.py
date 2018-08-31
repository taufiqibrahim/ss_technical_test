import os
import time
import datetime
from itertools import islice
import pprint as pp
import sys

# USER_PREF = sys.argv[1]
# PRODUCT = sys.argv[2]

UID = 1999
MEM_SIZE = '10M'
CHUNK_SIZE = 100
USER_PREF = './data/user_preference.txt'
USER_PREF_INDEX = './data/index_user_preference.txt'
PRODUCT_SCORE = './data/product_score.txt'


class InitDB(object):
    """
    Initialize the database and perform indexing
    """

    def __init__(self, user_preference, product_score, mem_size):
        super(InitDB, self).__init__()
        self.mem_size = mem_size
        self.multiplier_const = 0.95
        self.user_preference = user_preference if user_preference else USER_PREF
        self.user_preference_idx = [[0, 0, 0, ]]
        self.user_preference_idx_dic = {0: [0, 0, ]}
        self.idx_user_preference = USER_PREF_INDEX
        self.product_score = product_score if product_score else PRODUCT_SCORE
        self.user_preference_table = open(USER_PREF, 'r')

    def timer(self, start=True, operation=''):
        if start:
            self.time_start = time.time()
        else:
            self.time_end = time.time()
            seconds = self.time_end - self.time_start
            print('Done %s in %s seconds.\n---------' % (operation, seconds))

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
            # Generate product score as tuple of tuples
            as_tup = tuple(self.format_col(line, 'product_score')
                           for line in f.readlines()[1:])

            # Convert product score into dictionary, it's hashed so it's faster
            # https://stackoverflow.com/a/11241481
            # self.t_product_score = {p[0]: p[1] for p in as_tup}

            # Let's use tuple for now
            self.t_product_score = as_tup

            # Clean up
            del as_tup

            # DEBUG
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

    def indexing_user_preferencenew(self):
        # Read user preference table
        with open(self.user_preference, 'r') as f:
            for lineno, line in enumerate(f):

                if lineno % CHUNK_SIZE == 0:

                    lines = tuple(self.format_col(line, 'user_preference')
                                  for line in f.readlines())
                    #self.create_index(chunk_start, lines)
                    print(len(lines))

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
            except:
                print(line[1])
                raise

        # DEBUG
        # print(self.user_preference_idx_dic[55])

    def write_index(self):
        print('Writing index...')
        with open(self.idx_user_preference, 'a') as f:
            for k, v in self.user_preference_idx_dic.items():
                line = "{}\t{}\t{}\n".format(k, v[0], v[1])
                f.write(line)
        del self.user_preference_idx
        del self.user_preference_idx_dic
        print('Deleted tmp index')

    def load_index(self):
        with open(self.idx_user_preference) as f:
            # Generate index as tuple of tuples
            self.t_idx_as_tup = tuple(self.format_col(line, 'product_score')
                                      for line in f.readlines())

            # Convert product score into dictionary, it's hashed so it's faster
            # https://stackoverflow.com/a/11241481
            self.t_idx_as_dic = {p[0]: (p[1], p[2], )
                                 for p in self.t_idx_as_tup}

    def load_user_preference(self, startrow, endrow):
        print('Working on %s\nStarting at row: %s\nEnd at row %s.' %
              (self.user_preference, startrow, endrow))
        with open(self.user_preference, 'r') as f:

            # V2 This one is faster
            self.t_user_preference = [self.calc_user_preference(
                line) for line in f.readlines()[startrow:endrow + 1]]

            self.t_user_preference_as_dict = {
                p[0]: {p[1]: p[4]} for p in self.t_user_preference}
            pp.pprint(self.t_user_preference[:2])

            print('The size: ', len(self.t_user_preference),
                  len(self.t_user_preference_as_dict))
            #del as_list, as_dict

            # V1 Suspect this is a slow operations
            # as_tup = tuple(self.calc_user_preference(line) for rn, line in enumerate(f) if rn in the_range)
            #as_dict = {p[1]: (p[0], p[4], ) for p in as_tup}

    def product_leftjoin_user_pref(self):
        print('\nThis is product score:')
        pp.pprint(self.t_product_score[:5])

        print('\nThis is user preference:')
        pp.pprint(self.t_user_preference[:5])

        print('\nThis is user preference as dict:')
        pp.pprint(self.t_user_preference_as_dict.get(7779))

        # Now to the left join
        # The left join shall be performed per UID
        # UID iteration based on Index
        print('\nThis is UID index:')
        for uid in self.t_idx_as_tup[:1]:
            print(uid)

            # First convert user preference into dict.. oh we already did!
            output = list()
            for product in self.t_product_score:
                # print(self.t_user_preference_as_dict.get(product[0], (0, 0))[1])
                output_item = product + \
                    (self.t_user_preference_as_dict.get(
                        product[0], (0, 0))[1], )
                output.append(output_item)

            print('Output size: \n', len(output))
            # pp.pprint(output)

    def get_recommendation(self):
        pass

    def recommendation_engine_by_user(self):
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

        # Iterate through UID from index
        print('Index size: \n', len(self.t_idx_as_tup))
        for uidx in self.t_idx_as_tup[:1]:
            uid = uidx[0]
            startrow = uidx[1]
            endrow = uidx[2]

            operation = 'Load user preference'
            self.timer(start=True, operation=operation)
            self.load_user_preference(startrow, endrow)
            self.product_leftjoin_user_pref()
            self.timer(start=False, operation=operation)

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
        step = 100
        the_len = len(self.t_idx_as_tup)
        the_range = list(range(0, the_len, step))
        print('Size of index: \n', the_len)

        for r in the_range[:1]:
            print('Current range: \n', r)
            pos_min = self.t_idx_as_tup[r][1]

            try:
                pos_max = self.t_idx_as_tup[r + step - 1][2]
            except:
                pos_max = self.t_idx_as_tup[-1][2]

            operation = 'Load user preference'
            self.timer(start=True, operation=operation)
            self.load_user_preference(pos_min, pos_max)
            self.product_leftjoin_user_pref()
            self.timer(start=False, operation=operation)


def main():
    arg_1 = sys.argv[1]
    arg_2 = sys.argv[2]
    print('\nInitializing database...')
    i = InitDB(arg_1, arg_2, '10M')

    operation = 'Indexing database table [user_preference]'
    print('\n%s...' % operation)
    i.timer(start=True, operation=operation)
    i.indexing_user_preference()
    i.write_index()
    i.timer(start=False, operation=operation)

    operation = 'Initializing database table [product_score]'
    print('\n%s...' % operation)
    i.timer(start=True, operation=operation)
    i.load_product_score()
    i.timer(start=False, operation=operation)

    operation = 'Load user preference index table'
    print('\n%s...' % operation)
    i.timer(start=True, operation=operation)
    i.load_index()
    i.timer(start=False, operation=operation)

    print('\nPerforming recommendation calculation...')
    i.recommendation_engine()
    i.recommendation_engine_by_user()


if __name__ == '__main__':
    st = time.time()
    main()
    et = (time.time() - st)

    print('\nDone in %s minutes' % str(et / 60))
    print('Done in %s seconds' % str(et))
