import sys

if __name__ == '__main__':
    args = sys.argv
    filepath = args[1]
    print(filepath)
    worker_num = int(args[2])
    fin = open(filepath, "r")
    query_count = 0
    query_time_dict = {}
    query_time_list = []
    while True:
        line = fin.readline()
        if not line:
            break
        if " finished," not in line:
            continue
        job_id = line.split(' ')[-5]
        query_time = int(line.split(' ')[-1])
        latest = query_time_dict.get(job_id, 0)
        if query_time > latest:
            query_time_dict[job_id] = query_time
    fin.close()
    for i in query_time_dict.values():
        query_time_list.append(i)
    query_time_list.sort()
    mean_time = sum(query_time_list) / len(query_time_list)
    index_50 = len(query_time_list) / 2
    index_90 = len(query_time_list) / 10
    index_95 = len(query_time_list) / 20
    index_99 = len(query_time_list) / 100
    print("query count: %d, min time: %d, max time %d, mean time %d, P50 %d, P90 %d, P95 %d, P99 %d" % (len(query_time_list), query_time_list[0], query_time_list[-1], mean_time, query_time_list[-index_50], query_time_list[-index_90], query_time_list[-index_95], query_time_list[-index_99]))