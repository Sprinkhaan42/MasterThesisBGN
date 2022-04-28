import pandas as pd
save_path6 = '../data/BGGN_format2/bundle_item.txt'
df = pd.read_csv(save_path6, sep='\t', header=None)
print(df)
