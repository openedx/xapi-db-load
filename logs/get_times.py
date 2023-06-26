with open("ralph_clickhouse_100M_2/ralph_clickhouse_100m_10kbatch.txt", "r") as f:
    print(
        """# Automatically generated from get_times.py
    
    
import datetime

a = (
    """
    )

    # count = 0

    # First few timestamps are startup related
    for line in f.readlines()[4:]:
        if line.startswith("[('"):
            # count += 1

            # Every 10th and 11th timestamp are query timing related
            # if count % 11 == 0 or count % 12 == 0:
            #    continue
            print(line.strip() + ",")

print(
    """
)

times = [x[0][1] for x in a]

prev_time = times[1]

durations = []
count = 1
for t in times[2:]:
    count += 1
    
    if count % 11 == 0 or count % 12 == 0:
        prev_time = t
        continue
        
    durations.append(str((t - prev_time).seconds))
    prev_time = t

print("\\n".join(durations))
"""
)
