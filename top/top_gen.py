import sys

sys.path.append('../utils')
import pickle
import numpy as np
from tqdm import tqdm
from collections import Counter
from utils import metrics



def subsets(nums, mi=2, ma=5):
    res = [[]]
    for num in sorted(nums):
        res += [item + [num] for item in res if len(item) < ma]
    res = [tuple(t) for t in res if len(t) >= mi]
    return res


def main(flag, k):
    if flag == 'clo':
        source_path = '../data/bundle_clo.pkl'
    elif flag == 'ele':
        source_path = '../data/bundle_ele.pkl'
    elif flag == 'spo':
        source_path = '../data/bundle_spo.pkl'
    elif flag == 'toy':
        source_path = '../data/bundle_toy.pkl'
    else:
        assert False

    with open(source_path, 'rb') as f:
        train_set = pickle.load(f)
        test_set = pickle.load(f)
        cate_list = pickle.load(f)
        bundle_map = pickle.load(f)
        (user_count, item_count, cate_count, bundle_count, bundle_rank, _) = pickle.load(f)
        gen_groundtruth_data = pickle.load(f)

    freq = Counter()
    for t in train_set:
        if len(bundle_map[t[2]]) >= 2:
            t = bundle_map[t[2]]
            freq.update(subsets(t))
            # for i in range(len(t)):
            #   for j in range(i+1, len(t)):
            #     freq.update([tuple([t[i], t[j]])])

    preds = freq.most_common(k)
    preds = [[i for i in t[0]] for t in preds]

    total, jacc, prec, recall, ndcg, mrr = 0, metrics.jaccard(preds), 0.0, 0, 0, 0
    for uid, hist, pos in gen_groundtruth_data:
        groundtruth = list(bundle_map[pos])
        prec += metrics.precision(groundtruth, preds)
        recall += metrics.Recall(groundtruth, preds)
        ndcg += metrics.NDCG(groundtruth, preds)
        mrr += metrics.MRR(groundtruth, preds)
        if len(preds) == 1:  # topk=1, no diversity
            jacc += 0
        else:
            jacc += metrics.jaccard(preds)
        total += 1

    print('%s\tPre@%d: %.4f%%\tRecall@%d: %.4f%%\tNDCG@%d: %.4f%%\tMRR@%d: %.4f%%\tDiv: %.4f'
          % (flag, k, prec * 100 / total, k, recall * 100 / total, k, ndcg * 100 / total, k, mrr * 100 / total,
             -jacc / total))


if __name__ == '__main__':
    main('spo', k=10)
    main('clo', k=10)
    main('toy', k=10)
