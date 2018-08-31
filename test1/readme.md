# Recommendation API

## Generate Data

The whole story was simple and defined as a function named as `run_story`. Here's the snippet:

```py
def run_story():
    circus = build_circus()
    add_user_population(circus, 10)
    add_products_populations(circus, 100)
    add_user_preference_story(circus)
    run(circus)
```

### Generating The Circus

The term `circus` were made by `trumanian` developer, which is quite contextual. The `circus` is built using `build_circus` function.

```py
    circus = build_circus()
```

### Generating User Data

The `users` is built by calling `create_user_population` function. The number of users is set up using the second argument. Here we defined 10 users.

```py
    add_user_population(circus, 10)
```

### Generating Product Data

The specification as below:
- Product ID int : uniquely identifies the product
- Product Score int : has range 0 to 1000

Salestock requires at least ten thousand of products data. So, let's us define the number as __12835__

The `products` is built by calling `add_products_populations` function. The number of products is set up using the second argument. Here we defined 12835 products.

```py
    add_products_populations(circus, 12835)
```

### Generate User Preference

Model:

- User ID (uid) int : uniquely identifies the user
- Product ID (pid) int : uniquely identifies the product
- Score double : score of the likelihood of user interest to a product, and has range -1 to 1.
- Timestamp int : the Unix timestamp of last score calculation

This is a transaction-like data. We will generate this data using story.

The `user_preference` is built by calling `add_user_preference_story` function.

```py
    add_user_preference_story(circus)
```

## Requirements

- Your solution must be scalable so that it can run in a timely fashion dealing with the large data files.
- Just say, you have to deal with at least a hundred million of user_preferences and
- Ten thousand of products data.

We have to generate around 100 million of user preferences.

### initiate.py

The plan was:
- [x] Generate data using __trumania__ as decribed above.
- [x] Read `user_preference.txt` by chunks
- [x] Read `product_score.txt`
- [x] Create new file called `index_user_preference.txt` which contains `UID`, `start_row`, `end_row`. The `UID` refer to unique User ID generated on `user_preference.txt`. While `start_row` and `end_row represent` start and end row number for specific UID.
- [x] Create function `calc_user_preference` to calculate effective_score
- [] (NOT FINISHED) Create function for performing LEFT JOIN between `product_score` and `user_preference` to get the product score
- [] (NOT FINISHED) Create function for calculate final score

### recommend
- [] (NOT FINISHED) Create program `recommend-products.py` to act as the API
