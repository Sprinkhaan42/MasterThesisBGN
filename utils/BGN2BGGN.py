import numpy as np
import pandas as pd

from tqdm import tqdm


def to_df(file_path):
    with open(file_path, 'r') as f:
        df = {}
        i = 0
        for line in tqdm(f.readlines()):
            df[i] = eval(line)
            i += 1
        df = pd.DataFrame.from_dict(df, orient='index')
        return df


def build_map(df, col_name):
    key = sorted(df[col_name].unique().tolist())
    m = dict(zip(key, range(len(key))))
    df[col_name] = df[col_name].map(lambda x: m[x])
    return m, key


# def main():
reviews_path = '../data/Sports_and_Outdoors_5.json'
meta_path = '../data/meta_Sports_and_Outdoors.json'
save_path1 = '../data/user_bundle_original.txt'
save_path2 = '../data/bundle_item_original.txt'
save_path3 = '../data/user_bundle_train.txt'
save_path4 = '../data/user_bundle_test.txt'
save_path5 = '../data/bundle_item.txt'

reviews_df = to_df(reviews_path)
reviews_df = reviews_df[['reviewerID', 'asin', 'unixReviewTime']]
reviews_df

meta_df = to_df(meta_path)
meta_df = meta_df[meta_df['asin'].isin(reviews_df['asin'].unique())]
meta_df = meta_df.reset_index(drop=True)
meta_df['categories'] = meta_df['categories'].map(lambda x: x[-1][-1])
meta_df

# %%
# main()
# renumber asin(item id) starting at 0 and put in map
asin_map, asin_key = build_map(meta_df, 'asin')
# renumber category starting at 0 and put in map
cate_map, cate_key = build_map(meta_df, 'categories')
# renumber reviewer id starting at 0 and put in map
revi_map, revi_key = build_map(reviews_df, 'reviewerID')

# %%
print('Changing the numbers...')
meta_df = meta_df.sort_values('asin')
meta_df = meta_df.reset_index(drop=True)
reviews_df['asin'] = reviews_df['asin'].map(lambda x: asin_map[x])
reviews_df = reviews_df.sort_values(['reviewerID', 'unixReviewTime'])
reviews_df = reviews_df.reset_index(drop=True)
reviews_df = reviews_df[['reviewerID', 'asin', 'unixReviewTime']]
for i, m in meta_df.iterrows():
    m['asin'] = asin_key[i]

# %%
print('Building bundle_map...')
bundle_all = [(-1,)]
for _, hist in tqdm(reviews_df.groupby('reviewerID')):
    hist_group = hist.groupby('unixReviewTime')
    if len(hist_group) <= 1: continue
    for _, group in hist_group:
        bundle = group['asin'].tolist()
        bundle_all.append(tuple(bundle))
bundle_all.remove((-1,))
bundle_map = dict(zip(bundle_all, range(len(bundle_all))))
bundle_count = len(bundle_all) - 1
bundle_max_len = len(bundle_all[0])

# %%
# creates user bundle
with open(save_path1, 'w') as f:
    write_to_file_1 = ""
    for uid, hist in tqdm(reviews_df.groupby('reviewerID')):
        hist_group = hist.groupby('unixReviewTime')
        if len(hist_group) <= 1: continue
        for _, group in hist_group:
            bundle = group['asin'].tolist()
            bundle_number = bundle_map[tuple(bundle)]
            user_number = group['reviewerID'].tolist()[0]
            to_add_line_1 = str(user_number) + '\t' + str(bundle_number) + '\n'
            write_to_file_1 += to_add_line_1
    f.write(write_to_file_1)

# %%
# creates bundle item
with open(save_path2, 'w') as f:
    write_to_file_2 = ""
    for key in bundle_map:
        for i in key:
            to_add_line_2 = str(i) + '\t' + str(bundle_map[key]) + '\n'
            write_to_file_2 += to_add_line_2
    f.write(write_to_file_2)

# %%
bundle_list = []
# creates user bundle
with open(save_path3, 'w') as f:
    with open(save_path4, 'w') as g:
        write_to_file_3 = ""
        write_to_file_4 = ""
        i = 0
        for uid, hist in tqdm(reviews_df.groupby('reviewerID')):
            hist_group = hist.groupby('unixReviewTime')
            if len(hist_group) <= 1: continue
            for _, group in hist_group:
                bundle = group['asin'].tolist()
                bundle_number = bundle_map[tuple(bundle)]
                user_number = group['reviewerID'].tolist()[0]
                to_add_line_3 = str(user_number) + '\t' + str(bundle_number) + '\n'
                if i < 100:
                    write_to_file_3 += to_add_line_3
                elif 100 <= i < 200:
                    write_to_file_4 += to_add_line_3
                else:
                    break
                i += 1
                bundle_list.append(bundle_number)
        f.write(write_to_file_3)
        g.write(write_to_file_4)

# creates bundle item
with open(save_path5, 'w') as f:
    write_to_file_5 = ""
    for key in bundle_map:
        if bundle_map[key] in bundle_list:
            for i in key:
                to_add_line_5 = str(i) + '\t' + str(bundle_map[key]) + '\n'
                write_to_file_5 += to_add_line_5
    f.write(write_to_file_5)

print("DONE")
