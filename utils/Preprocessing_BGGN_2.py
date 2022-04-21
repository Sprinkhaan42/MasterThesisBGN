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


reviews_path = '../data/Sports_and_Outdoors_5.json'
meta_path = '../data/meta_Sports_and_Outdoors.json'
save_path1 = '../data/BGGN_format2/user_bundle_original.txt'
save_path2 = '../data/BGGN_format2/bundle_item_original.txt'
save_path3 = '../data/BGGN_format2/user_bundle_train.txt'
save_path4 = '../data/BGGN_format2/user_bundle_test.txt'
save_path5 = '../data/BGGN_format2/user_bundle.txt'
save_path6 = '../data/BGGN_format2/bundle_item.txt'
save_path7 = '../data/BGGN_format2/sports_data_size.txt'

# %%
reviews_df = to_df(reviews_path)
reviews_df = reviews_df[['reviewerID', 'asin', 'unixReviewTime']]

# %%
asin_list = reviews_df['asin'].tolist()
asin_set = set(asin_list)
item_map = {}
number = 0

for i in tqdm(asin_set):
    item_map[i] = number
    number = number + 1
item_count = len(item_map)

# %%
reviews_df['asin'] = reviews_df['asin'].map(lambda x: item_map[x])

# %%
user_list = reviews_df['reviewerID'].tolist()
user_set = set(user_list)
user_map = {}
number = 0

for i in tqdm(user_set):
    user_map[i] = number
    number = number + 1
user_count = len(user_map)

# %%
reviews_df['reviewerID'] = reviews_df['reviewerID'].map(lambda x: user_map[x])

# %%
reviews_df.rename(columns={'reviewerID': 'user', 'asin': 'item'}, inplace=True)

# %%
print("Creating bundle-item dictionaries... \n")
usergroups = reviews_df.groupby(by='user')
bundle_list = []
for _, i in tqdm(usergroups):
    user_time_groups = i.groupby(by='unixReviewTime')
    for _, j in user_time_groups:
        if not (len(j) <= 1):
            bundle = j['item'].tolist()
            bundle_list.append(tuple(bundle))

bundle_tuple = tuple(bundle_list)
bundle_count = len(bundle_tuple)

# %%
bundle_dict = {}
bundle_dict_numbered = {}
counter = 0
for bundle in bundle_tuple:
    bundle_dict[bundle] = counter
    bundle_dict_numbered[counter] = bundle
    counter += 1


# %%
print("Writing user-bundle original... \n")
with open(save_path1, 'w') as f:
    write_to_file_1 = ""
    usergroups = reviews_df.groupby(by='user')
    for _, i in tqdm(usergroups):
        user_time_groups = i.groupby(by='unixReviewTime')
        for _, j in user_time_groups:
            if not (len(j) <= 1):
                bundle = bundle_dict[tuple(j['item'].tolist())]
                user = j['user'].tolist()[0]
                to_add_line_1 = str(user) + '\t' + str(bundle) + '\n'
                write_to_file_1 += to_add_line_1

    f.write(write_to_file_1)




# %%
print("Writing bundle-item original... \n")
with open(save_path2, 'w') as f:
    write_to_file_2 = ""
    # a key is a bundle number
    for key in tqdm(bundle_dict_numbered):
        # every i is an item
        for item in bundle_dict_numbered[key]:
            to_add_line_2 = str(key) + '\t' + str(item) + '\n'
            write_to_file_2 += to_add_line_2
    f.write(write_to_file_2)

