
queries = {
    "Count of enrollment events for course": [],
    "Count of total enrollment events for org": [],
    # "for this learner" for Clickhouse, "for this actor" for some others...
    "Count of enrollments for this learner": [],
    "Count of enrollments for this course - count of unenrollments, last 30 days": [],
    "Count of enrollments for this course - count of unenrollments, all time": [],
    "Count of enrollments for all courses - count of unenrollments, last 5 minutes": []
}

# Just used for testing output
log = """2022-12-09 16:59:47.500233
8890 of 10000
2022-12-09 17:01:11.173771
8900 of 10000
2022-12-09 17:02:37.400290
Count of enrollment events for course http://localhost:18000/course/course-v1:salsaX+DemoX+81fcb08f-218c-454f-ad33-fe7c6e784ecb
28
Completed in: 0.014149
=================================
Count of total enrollment events for org chipX
190824
Completed in: 122.453695
=================================
Count of enrollments for this actor c29df261-45b5-4f32-85cc-56c54297f315
0
Completed in: 114.205501
=================================
2022-12-09 17:06:44.088138
Collection count:
89010000
8910 of 10000
2022-12-09 17:10:25.374361
8920 of 10000
2022-12-09 17:12:23.670774
""".splitlines()

def go():
    # fname = "citus_100M_columnar_cluster_no_partition.txt"
    # fname = "clickhouse_100M.txt"
    # fname = "mongo_100M_4indexes.txt"
    # fname = "ralph_mongo_100M.txt"
    fname = "ralph_100M_json_obj_no_buffer.txt"
    with open(fname, "r") as logf:
        log = logf.readlines()
        x = -1
        for line in log:
            print(line)
            x += 1
            for start in queries:
                # print(start)
                if line.startswith(start):
                    queries[start].append(x)

        output = {}
        for start in queries:
            output[start] = []
            print(f"{start}: {len(queries[start])}")

            # 3 lines after the start phrase is found is the time output line
            for base_line in queries[start]:
                # 'Completed in: 0.006565\n'

                offset = 3
                time_str = log[base_line + offset].strip()
                time = time_str[13:]

                output[start].append(time)
                # print(f"Base line: {log[base_line]}")
                # print(f"Base + 2: {log[base_line + 2]}")
                # print("-------")
                # break

        for start in output:
            print(start)
            print("\n".join(output[start]))


go()
