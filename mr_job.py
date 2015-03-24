__author__ = 'blu2'
__author__ = 'jtanigawa'

import sys

import zerorpc

from datetime import datetime

if __name__ == '__main__':

    master_addr = 'tcp://' + sys.argv[1]

    c = zerorpc.Client()
    c.connect(master_addr)

    mr_type = sys.argv[2]

    split_size = sys.argv[3]

    num_reducers = sys.argv[4]

    filename = sys.argv[5]

    base_filename = sys.argv[6]


    start = datetime.now()

    if mr_type == 'wordcount':
        c.do_word_count(filename, split_size, num_reducers, base_filename)
    else:
        print "other mr type"

    end = datetime.now()

    print "It takes " + str((end - start).total_seconds()) + " seconds"


    #c.split_file(filename, 60)
    #c.do_job()




