__author__ = 'blu2'
__author__ = 'jtanigawa'

import sys
import os


basename = sys.argv[1]
output_filename = sys.argv[2]
data_dir = sys.argv[3]
if data_dir[len(data_dir)-1] != '/':
    data_dir += '/'


import os


d = {}
for file in os.listdir(data_dir):
    if file.startswith(basename):
        pairs = open(data_dir + file, 'r').read().split()
        for pair in pairs:
            elems = pair.split('=')
            key = elems[0]
            value = int(elems[1])

            if key in d.keys():
                old_value = int(d[key])
                d[key] = old_value + value
            else:
                d[key] = value

output = open(data_dir + output_filename, 'w')
keys = d.keys()
keys.sort()
for key in keys:
    output.write(str(key) + '=' + str(d[key]) + '\n')

output.close()

