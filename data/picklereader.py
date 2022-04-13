import pandas as pd

object = pd.read_pickle('bundle_clo.pkl')
for i in object:
    print(str(i) + '\n')