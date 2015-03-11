__author__ = 'blu2'
__author__ = 'jtanigawa'

import sys

import zerorpc
import gevent



class Worker(object):


    def __init__(self):
        gevent.spawn(self.controller)
        pass

    def controller(self):
        while True:
            print('[Worker]')
            gevent.sleep(1)

    def ping(self):
        print '[Worker] Ping from Master'

    def do_work(self, data_dir, filenames, work_type):
        sys.exit(0)



if __name__ == '__main__':

    master_addr = "tcp://" + sys.argv[1]

    s = zerorpc.Server(Worker())
    ip = '0.0.0.0'
    port = sys.argv[2]
    s.bind('tcp://' + ip + ':' + port)
    c = zerorpc.Client()
    c.connect(master_addr)
    c.register(ip, port)
    s.run()





