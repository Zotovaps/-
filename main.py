from itertools import chain, combinations
from collections import defaultdict


def subsets(arr):
    return chain(*[combinations(arr, i + 1) for i, a in enumerate(arr)])


def get_items_satisfies_min_support(item_set, transaction_list, min_support, freq_set):
    # Возвращает набор элементов удовлетворяющих min_support
    _itemSet = set()
    local_set = defaultdict(int)

    for item in item_set:
        for transaction in transaction_list:
            if item.issubset(transaction):
                freq_set[item] += 1
                local_set[item] += 1

    for item, count in local_set.items():
        support = float(count) / len(transaction_list)

        if support >= min_support:
            _itemSet.add(item)

    return _itemSet


def join_set(item_set, length):
    return set([i.union(j) for i in item_set for j in item_set if len(i.union(j)) == length])


def get_items_and_transactions(data_iterator):
    transaction_list = list()
    item_set = set()
    for record in data_iterator:
        transaction = frozenset(record)
        transaction_list.append(transaction)
        for item in transaction:
            item_set.add(frozenset([item]))  # Generate 1-itemSets
    return item_set, transaction_list


def run_apriori_algorithm(data_iter, min_sup):
    item_set, transaction_list = get_items_and_transactions(data_iter)

    # Набор частот
    frequent_set = defaultdict(int)
    large_set = dict()

    currentLSet = get_items_satisfies_min_support(item_set, transaction_list, min_sup, frequent_set)

    k = 2
    while (currentLSet != set([])):
        large_set[k - 1] = currentLSet
        currentLSet = join_set(currentLSet, k)
        currentCSet = get_items_satisfies_min_support(currentLSet, transaction_list, min_sup, frequent_set)
        currentLSet = currentCSet
        k = k + 1

    def getSupport(item):
        return float(frequent_set[item]) / len(transaction_list)

    toRetItems = []
    for key, value in large_set.items():
        toRetItems.extend([(tuple(item), getSupport(item)) for item in value])

    return toRetItems


def run_apriori_algorithm_with_rules(data_iter, min_sup, min_conf):
    item_set, transaction_list = get_items_and_transactions(data_iter)

    print("1")
    # Набор частот
    frequent_set = defaultdict(int)
    large_set = dict()

    currentLSet = get_items_satisfies_min_support(item_set, transaction_list, min_sup, frequent_set)
    print("2")
    k = 2
    while (currentLSet != set([])):
        large_set[k - 1] = currentLSet
        currentLSet = join_set(currentLSet, k)
        currentCSet = get_items_satisfies_min_support(currentLSet, transaction_list, min_sup, frequent_set)
        currentLSet = currentCSet
        k = k + 1

    print("3")
    def getSupport(item):
        return float(frequent_set[item]) / len(transaction_list)

    toRetItems = []
    for key, value in large_set.items():
        toRetItems.extend([(tuple(item), getSupport(item)) for item in value])

    toRetRules = []
    for key, value in list(large_set.items())[1:]:
        for item in value:
            _subsets = map(frozenset, [x for x in subsets(item)])
            for element in _subsets:
                remain = item.difference(element)
                if len(remain) > 0:
                    confidence = getSupport(item) / getSupport(element)
                    if confidence >= min_conf:
                        toRetRules.append(((tuple(element), tuple(remain)), confidence))

    return toRetItems, toRetRules


def print_results(items, rules=None):
    print("\n------------------------ ITEMS:")
    for item, support in sorted(items, key=lambda support: support):
        print("item: %s , %.3f" % (str(item), support))

    if rules is not None:
        print("\n------------------------ RULES:")
        for rule, confidence in sorted(rules, key=lambda confidence: confidence):
            pre, post = rule
            print("Rule: %s ==> %s , %.3f" % (str(pre), str(post), confidence))


def data_from_file():
    file_iter = open('baskets.csv', 'r')
    for line in file_iter:
        record = frozenset(line.strip().split(','))
        yield record


if __name__ == '__main__':
    minSup = 0.5
    minConf = 0.1

    # LAB1
    # csvFile = data_from_file()
    # i = run_apriori_algorithm(csvFile, minSup)
    # print_results(i)

    # LAB2
    datFile = data_from_file()
    item, rule = run_apriori_algorithm_with_rules(datFile, minSup, minConf)
    print_results(item, rule)
