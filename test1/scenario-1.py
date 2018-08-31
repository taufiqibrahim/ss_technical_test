from trumania.core import circus
import trumania.core.random_generators as gen
import trumania.core.operations as ops
import trumania.components.time_patterns.profilers as profilers
import trumania.core.util_functions as util_functions

import pandas as pd
import numpy as np
import time

st_x = time.time()

print('Required packages imported. Good to go')


def seq2int(seq, prefix):
    return int(seq.strip(prefix))


def build_circus():
    return circus.Circus(
        name="salestock",
        master_seed=12345,
        start=pd.Timestamp("1 Jun 2018 00:00"),
        step_duration=pd.Timedelta("1h"))


def add_user_population(the_circus, size=100):
    """
    Creates a user population and attach it to the circus
    """
    customer = the_circus.create_population(
        name="users", size=size,
        ids_gen=gen.SequencialGenerator(prefix="UID_"))

    customer.create_attribute(
        name="FIRST_NAME",
        init_gen=gen.FakerGenerator(method="first_name",
                                    seed=next(the_circus.seeder)))
    customer.create_attribute(
        name="LAST_NAME",
        init_gen=gen.FakerGenerator(method="last_name",
                                    seed=next(the_circus.seeder)))

    customer.create_relationship(name="my_items")

    return customer


def add_products_populations(the_circus, size=100):
    """
    Creates a products population and attach it to the circus
    """
    products = the_circus.create_population(
        name="products", size=size,
        ids_gen=gen.SequencialGenerator(prefix="PID_"))

    product_name_gen = gen.FakerGenerator(
        method="sentence", nb_words=2, seed=next(the_circus.seeder))
    products.create_attribute(name="ProductName", init_gen=product_name_gen)

    score_gen = gen.FakerGenerator(
        method="random_int", min=0, max=1000, seed=next(the_circus.seeder))
    products.create_attribute(name="ProductScore", init_gen=score_gen)

    products.create_relationship(name="my_products")

    """
    Write products to product_score.txt
    
    Example product_score.txt:
    ============================
    2939     600     
    2123     300     
    ============================
    """
    products_copy = products.to_dataframe().copy()
    products_copy['PID'] = products_copy.index
    products_copy['PID'] = products_copy['PID'].apply(
        lambda x: seq2int(x, 'PID_'))
    products_copy.sort_values(by=['PID'], inplace=True)
    products_copy.to_csv('results/product_score.txt',
                         sep='\t',
                         index=False, quoting=2,
                         columns=['PID', 'ProductScore']
                         )
    print('Written to results/product_score.txt')
    del products_copy

    return products


def add_user_preference_story(the_circus):

    users = the_circus.populations["users"]

    timer_gen = gen.ConstantDependentGenerator(
        value=the_circus.clock.n_iterations(duration=pd.Timedelta("24h")) - 1)


#     # using this timer means users only listen to songs during work hours
#     timer_gen = profilers.WorkHoursTimerGenerator(
#         clock=the_circus.clock, seed=next(the_circus.seeder))

#     # this generate activity level distributed as a "truncated normal
#     # distribution", i.e. very high and low activities are prevented.
#     bounded_gaussian_activity_gen = gen.NumpyRandomGenerator(
#         method="normal",
#         seed=next(the_circus.seeder),
#         loc=timer_gen.activity(n=1,
#         per=pd.Timedelta("1 day")),
#         scale=1
#     ).map(ops.bound_value(lb=10, ub=20))

    prefer = the_circus.create_story(
        name="prefer_events",
        initiating_population=users,
        member_id_field="UID",
        timer_gen=timer_gen,
        #              activity_gen=bounded_gaussian_activity_gen
    )

    repo = the_circus.populations["products"]

    prefer.set_operations(

        users.ops.lookup(
            id_field="UID",
            select={
                "FIRST_NAME": "USER_FIRST_NAME",
                "LAST_NAME": "USER_LAST_NAME",
            }
        ),

        # Add user preference value
        gen.NumpyRandomGenerator(method="uniform",
                                 low=-1,
                                 high=1,
                                 seed=next(the_circus.seeder)).ops.generate(named_as="PREFERENCE"),

        # Picks a product at random
        repo.ops.select_one(named_as="PRODUCT_ID"),

        # Add timestamp column
        the_circus.clock.ops.timestamp(named_as="DATETIME"),

        ops.FieldLogger("prefer_events")
    )


def run(the_circus):
    the_circus.run(
        duration=pd.Timedelta("65 days"),
        log_output_folder="output/preferences",
        delete_existing_logs=True
    )


def run_story(usernum, productnum):
    st = time.time()
    circus = build_circus()
    add_user_population(circus, usernum)
    add_products_populations(circus, productnum)
    print('Products generated in %s minutes' % str((time.time() - st) / 60))

    st = time.time()
    add_user_preference_story(circus)
    print('Users generated in %s minutes' % str((time.time() - st) / 60))

    st = time.time()
    run(circus)
    print('Circus run in %s minutes' % str((time.time() - st) / 60))


st = time.time()
run_story(usernum=1538470, productnum=12835)

result = pd.read_csv('output/preferences/prefer_events.csv',
                     sep=',',
                     usecols=['UID', 'PREFERENCE', 'PRODUCT_ID', 'DATETIME'],
                     parse_dates=['DATETIME']
                     )
print('Have rows: %s' % result.shape[0])

result['UID'] = result['UID'].apply(lambda x: seq2int(x, 'UID_'))
result['PID'] = result['PRODUCT_ID'].apply(lambda x: seq2int(x, 'PID_'))
result['TIMESTAMP'] = pd.to_datetime(
    result['DATETIME'], format='%Y-%m-%d %H:%M:%S').values.astype(np.int64) // 10 ** 9
result.drop(columns=['DATETIME'], inplace=True)
result.sort_values(by=['UID', 'PID', 'TIMESTAMP'], inplace=True)

"""
Example user_preference.txt:
============================
12341     2123     0.832     1508733366
12341     2939     0.200     1457261934
21235     2329     -0.800    1448432918
============================
"""
cols = ['UID', 'PID', 'PREFERENCE', 'TIMESTAMP',
        #         'DATETIME'
        ]
result[cols].to_csv('results/user_preference.txt',
                    sep='\t', index=False, quoting=2)
print('Written to results/user_preference.txt')

del result

print('User preferences generated in %s minutes' %
      str((time.time() - st) / 60))

el = time.time() - st_x
print('Done in %s minutes.' % str(el / 60))
