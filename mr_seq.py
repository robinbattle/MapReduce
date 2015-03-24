__author__ = 'blu2'
__author__ = 'jtanigawa'

import sys
import re

from datetime import datetime

def read_from_file(data_dir, filename):
    input = open(data_dir + filename, 'r').read()
    return input

def write_to_file(data_dir, filename, reduce_dict):
    output = open(data_dir + filename, 'w')
    output.write(str(reduce_dict))
    output.close()

    output = open(data_dir + filename, 'w')

    keylist = reduce_dict.keys()
    keylist.sort()


    for key in keylist:
        output.write(str(key) + '=' + str(reduce_dict[key]) + '\n')

    output.close()


def map(input):
    s_list = []
    words = input.split()
    for word in words:
        #word = word.replace('\n', '')
        word = re.sub('[^0-9a-zA-Z]+', '', word)
        s_list.append(word + "=1")

    return s_list

def reduce(words):
    reduce_dict = {}
    for word in words:
        elems = word.split('=')
        if len(elems) != 2:
            continue
        key = elems[0]
        try:
            value = int(elems[1])
        except:
            # this may need to be handle properly later
            value = 0
            print elems[1]

        if key in reduce_dict:
            old_value = int(reduce_dict[key])
            reduce_dict[key] = old_value + value
        else:
            reduce_dict[key] = value
    return reduce_dict


if __name__ == '__main__':

    mr_type = sys.argv[1]
    data_dir = sys.argv[2]
    if data_dir[len(data_dir)-1] != '/':
        data_dir += '/'
    input_filename = sys.argv[3]
    output_filename = sys.argv[4]

    start = datetime.now()

    if mr_type == 'wordcount':
        input = read_from_file(data_dir, input_filename)
        map_list = map(input)
        reduce_list = reduce(map_list)
        write_to_file(data_dir, output_filename, reduce_list)


    end = datetime.now()

    print "It takes " + str((end - start).total_seconds()) + " seconds"


