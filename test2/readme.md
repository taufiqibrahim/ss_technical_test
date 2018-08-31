# Log

## Creating 7 Billion Age Data

I use Python to generate the data. The code is in `data/gen_age.py`.

```py
import csv
import random as r
import time

st = time.time()
output = 'age.tsv'
sampleSize = 7000
sampleSize = 7000000
sampleSize = 7000000000

with open(output, mode='w', newline='') as tsvfile:
    writer = csv.writer(tsvfile, delimiter='\t')
    for i in range(sampleSize):
        writer.writerow([r.randint(1, 88)])

et = time.time() - st

print('Generate %s done in %s minutes' % (output, str(et / 60)))
```

| Number of data | Time Elapsed |
| -------------- | ------------ |
| 7,000 | 0.0002225041389465332 minutes |
| 7,000,000 | 0.21738688945770263 minutes |
| 7,000,000,000 | 260.9811469475428 minutes |

This process generate 26 GB of data.

```
(venv) tibrahim@tibrahim:/mnt/c/tole/tech_interviews/salestock/salestock_dataengineer/data$ wc -l age.tsv
7000000000 age.tsv
(venv) tibrahim@tibrahim:/mnt/c/tole/tech_interviews/salestock/salestock_dataengineer/data$ ls -lh
total 26G
-rwxrwxrwx 1 tibrahim tibrahim 26G Aug 25 15:32 age.tsv
```

### Faster Method

Above method is somewhat slooooow.

Let's us try googling for another shared ideas.

