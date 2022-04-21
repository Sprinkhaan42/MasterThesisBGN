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
save_path1 = '../data/BGGN_format/user_bundle_original.txt'
save_path2 = '../data/BGGN_format/bundle_item_original.txt'
save_path3 = '../data/BGGN_format/user_bundle_train.txt'
save_path4 = '../data/BGGN_format/user_bundle_test.txt'
save_path5 = '../data/BGGN_format/user_bundle.txt'
save_path6 = '../data/BGGN_format/bundle_item.txt'
save_path7 = '../data/BGGN_format/sports_data_size.txt'

reviews_df = to_df(reviews_path)
reviews_df = reviews_df[['reviewerID', 'asin', 'unixReviewTime']]

meta_df = to_df(meta_path)
meta_df = meta_df[meta_df['asin'].isin(reviews_df['asin'].unique())]
meta_df = meta_df.reset_index(drop=True)
meta_df['categories'] = meta_df['categories'].map(lambda x: x[-1][-1])

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
bundle_map = {}
i = 0
for bundle in bundle_all:
    bundle_map[bundle] = i
    i = i + 1
bundle_count = len(bundle_all)

# %%
print(len(bundle_map))
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
print(len(bundle_map))
# %%
# creates bundle item
with open(save_path2, 'w') as f:
    write_to_file_2 = ""
    # a key is a bundle
    for key in bundle_map:
        # every i is an item
        for i in key:
            # TODO: check if this is switched correctly
            to_add_line_2 = str(bundle_map[key]) + '\t' + str(i) + '\n'
            write_to_file_2 += to_add_line_2
    f.write(write_to_file_2)
# %%
bundle_list = []
# creates user bundle
with open(save_path3, 'w') as f:
    with open(save_path4, 'w') as g:
        with open(save_path5, 'w') as h:
            write_to_file_3 = ""
            write_to_file_4 = ""
            write_to_file_34 = ""
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
                        write_to_file_34 += to_add_line_3
                    elif 100 <= i < 200:
                        write_to_file_4 += to_add_line_3
                        write_to_file_34 += to_add_line_3
                    else:
                        break
                    i += 1
                    bundle_list.append(bundle_number)
            f.write(write_to_file_3)
            g.write(write_to_file_4)
            h.write((write_to_file_34))
# %%
# creates bundle item
with open(save_path6, 'w') as f:
    write_to_file_5 = ""
    for key in bundle_map:
        if bundle_map[key] in bundle_list:
            for i in key:
                to_add_line_5 = str(bundle_map[key]) + '\t' + str(i) + '\n'
                write_to_file_5 += to_add_line_5
    f.write(write_to_file_5)
# %%
with open(save_path7, 'w') as f:
    data1 = pd.read_csv('../data/BGGN_format/bundle_item.txt', sep='\t', header=None)
    data1.columns = ["bundle", "item"]
    data2 = pd.read_csv('../data/BGGN_format/user_bundle.txt', sep='\t', header=None)
    data2.columns = ["user", "bundle"]
    data3 = pd.read_csv('../data/BGGN_format/user_bundle_train.txt', sep='\t', header=None)
    data3.columns = ["user", "bundle"]
    vertical_stack = pd.concat([data2, data3], axis=0)
    item_list = data1['item'].tolist()
    item_set = set(item_list)
    user_list = data2['user'].tolist()
    user_list = vertical_stack['user'].tolist()
    user_set = set(user_list)
    bundle_list = vertical_stack['bundle'].tolist()
    bundle_list = data2['bundle'].tolist()
    bundle_set = set(bundle_list)
    bundle_count2 = len(set(data1['bundle'].tolist()))

    user_count = len(user_set)
    item_count = len(item_set)
    bundle_count = len(bundle_set)

    write_to_file_7 = str(user_count) + '\t' + str(bundle_count) + '\t' + str(item_count)
    print(write_to_file_7)
    f.write(write_to_file_7)
print("DONE")
