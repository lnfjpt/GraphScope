import sys

if __name__ == '__main__':
    args = sys.argv
    filepath = args[1]
    fin = open(filepath, "r")
    query_count = 0
    query_time = []
    while True:
        line = fin.readline()
        if not line:
            break
        if "'is' finished," not in line:
            continue
        query_time.append(int(line.split(' ')[-1]))
        query_count += 1
    fin.close()
    query_time.sort()
    mean_time = sum(query_time) / len(query_time)
    index_50 = len(query_time) / 2
    index_90 = len(query_time) / 10
    index_95 = len(query_time) / 20
    index_99 = len(query_time) / 100
    print("query count: %d, min time: %d, max time %d, mean time %d, P50 %d, P90 %d, P95 %d, P99 %d" % (query_count, query_time[0], query_time[-1], mean_time, query_time[-index_50], query_time[-index_90], query_time[-index_95], query_time[-index_99]))
