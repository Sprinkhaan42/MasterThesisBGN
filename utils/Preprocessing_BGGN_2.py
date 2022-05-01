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


reviews_path = '../data/reviews_Clothing_Shoes_and_Jewelry_5.json'
save_path1 = '../data/Clothes/user_bundle_original.txt'
save_path2 = '../data/Clothes/bundle_item_original.txt'
save_path3 = '../data/Clothes/user_bundle_train.txt'
save_path4 = '../data/Clothes/user_bundle_test.txt'
save_path5 = '../data/Clothes/user_bundle.txt'
save_path6 = '../data/Clothes/bundle_item.txt'
save_path7 = '../data/Clothes/Toys_data_size.txt'
save_pathx = '../data/Clothes/renumber_item.txt'

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

# %%
bundle_dict = {}
counter = 0
for bundle in bundle_list:
    bundle_dict[bundle] = counter
    counter += 1

bundle_dict_numbered = {}
recounter = 0
for key in bundle_dict:
    bundle_dict[key] = recounter
    bundle_dict_numbered[recounter] = key
    recounter += 1

bundle_count = len(bundle_dict)
bundle_count2 = max(bundle_dict.values()) + 1

# what happens is that bundles that come a second time get rewritten.
# therefore lenght does not equal the max number and a renumbering is needed.

# %%
print("Writing user-bundle original... ")
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
print("Writing bundle-item original... ")
with open(save_path2, 'w') as f:
    write_to_file_2 = ""
    # a key is a bundle number
    for key in tqdm(bundle_dict_numbered):
        # every i is an item
        for item in bundle_dict_numbered[key]:
            to_add_line_2 = str(key) + '\t' + str(item) + '\n'
            write_to_file_2 += to_add_line_2
    f.write(write_to_file_2)

# %%
print("Writing user bundle test and train...")
with open(save_path3, 'w') as f:
    with open(save_path4, 'w') as g:
        with open(save_path5, 'w') as h:
            write_to_file_3 = ""
            write_to_file_4 = ""
            write_to_file_5 = ""
            with open(save_path1, 'r') as r:
                counter = 0
                for line in tqdm(r):
                    if counter < 42077:
                        write_to_file_3 += line
                        write_to_file_5 += line
                    else:
                        write_to_file_4 += line
                        write_to_file_5 += line
                    counter += 1
            f.write(write_to_file_3)
            g.write(write_to_file_4)
            h.write(write_to_file_5)

# %%
print("Writing bundle_item...")
with open(save_path6, 'w') as f:
    write_to_file_6 = ""
    with open(save_path2, 'r') as r:
        for line in tqdm(r):
            write_to_file_6 += line
    '''
    intermediate_file = ""
    df = pd.read_csv(save_path5, sep='\t', header=None)
    df.columns = ["user", "bundle"]
    df.columns = ["user", "bundle"]
    sampled_bundle_list = df["bundle"].tolist()
    for bundle in sampled_bundle_list:
        for item in bundle_dict_numbered[bundle]:
            to_add_line_intermediate = str(bundle) + '\t' + str(item) + '\n'
            intermediate_file += to_add_line_intermediate
    with open(save_pathx, 'w') as x:
        x.write(intermediate_file)
    # renumber items to make them start from 0
    df2 = pd.read_csv(save_pathx, sep='\t', header=None)
    df2.columns = ["bundle", "item"]
    renumber_list = df2["item"].tolist()
    renumber_dict = {}
    itemcounter = 0
    for item in renumber_list:
        renumber_dict[item] = itemcounter
        itemcounter += 1

    # renumber items that are in multiple bundles
    renumber_dict_2 = {}
    itemrecounter2 = 0
    for key in renumber_dict:
        renumber_dict_2[key] = itemrecounter2
        itemrecounter2 += 1

    for bundle in sampled_bundle_list:
        for item in bundle_dict_numbered[bundle]:
            to_add_line_6 = str(bundle) + '\t' + str(renumber_dict_2[item]) + '\n'
            write_to_file_6 += to_add_line_6
    '''
    f.write(write_to_file_6)

# %%
print("Writing sports data size")
with open(save_path7, 'w') as f:
    data1 = pd.read_csv('../data/Clothes/bundle_item.txt', sep='\t', header=None)
    data1.columns = ["bundle", "item"]
    data2 = pd.read_csv('../data/Clothes/user_bundle_test.txt', sep='\t', header=None)
    data2.columns = ["user", "bundle"]
    data3 = pd.read_csv('../data/Clothes/user_bundle_train.txt', sep='\t', header=None)
    data3.columns = ["user", "bundle"]
    vertical_stack = pd.concat([data2, data3], axis=0)
    item_list = data1['item'].tolist()
    user_list = vertical_stack['user'].tolist()
    bundle_list = vertical_stack['bundle'].tolist()

    user_count = max(user_list) + 1
    item_count = max(item_list) + 1
    bundle_count1 = max(bundle_list) + 1
    bundle_count2 = max(data1['bundle'].tolist()) + 1
    bundle_count = max(bundle_count1, bundle_count2)
    write_to_file_7 = str(user_count) + '\t' + str(bundle_count) + '\t' + str(item_count)
    print(write_to_file_7)
    f.write(write_to_file_7)
