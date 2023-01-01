import numpy as np
import random

L = []
RANGE = 10**7

def randomstr(stringLength):
    """Generate a random string of fixed length """
    letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
    return ''.join(random.choice(letters) for i in range(stringLength))


class MyClass:
    def __init__(self, package_name, filepath):
        self.package_name = package_name
        self.filepath = filepath

# for i in range(RANGE):
    # L.append({"package_name":"A"*48, "filepath":"B"*48})
    # L.append(MyClass("A"*48, "B"*48))

# L = [{"package_name":"A"*48, "filepath":"B"*48} for i in range(RANGE)] # 2200Mib - 3.0 secs
# L = [MyClass("A"*48, "B"*48) for i in range(RANGE)] # 1600Mib - 11.6 secs

# L = np.array([{"package_name":"A"*48, "filepath":"B"*48} for i in range(RANGE)]) # 2400Mib - 4.1 secs
L = np.array((MyClass("A"*48, "B"*48) for i in range(RANGE))) # 1625Mib - 2O.5 secs

input("Done. Any key")