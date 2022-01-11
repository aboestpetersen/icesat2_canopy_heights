import os
import sys

loc = os.getcwd()

phoREAL_loc = loc + '/PhoREAL/source_code/'

sys.path.insert(1, phoREAL_loc)

print(phoREAL_loc)