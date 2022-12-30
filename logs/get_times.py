import datetime

timestamps = []

with open("x.log", "r") as f:
    for line in f.readlines():
        z = exec(line.strip())
        print(z)
        line = line.strip()
        print(exec("line"))
print(timestamps)

