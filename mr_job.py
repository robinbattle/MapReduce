__author__ = 'blu2'
__author__ = 'jtanigawa'

import sys

import zerorpc

if __name__ == '__main__':

    master_addr = 'tcp://' + sys.argv[1]

    c = zerorpc.Client()
    c.connect(master_addr)

    filename = sys.argv[2]

    c.split_file(filename, 60)
    c.do_job()




