__author__ = 'blu2'
__author__ = 'jtanigawa'

import sys

import zerorpc
import gevent
from threading import Lock


class Master(object):

    def __init__(self):
        gevent.spawn(self.controller)
        self.state = 'READY'
        self.workers = {}
        self.works = []
        self.all_works = []
        self.lock = Lock()

    def controller(self):
        while True:

            print '[Master:%s] ' % (self.state),
            for w in self.workers:
                print '(%s,%s,%s)' % (w[0], w[1], self.workers[w][0]),
            print
            for w in self.workers:
                try:
                    if self.workers[w][0] != 'Die':
                        self.workers[w][1].ping()
                    else:
                        print self.workers[w][0]
                except:
                    self.lock.acquire()
                    self.workers[w] = ('Die', self.workers[w][1])
                    self.lock.release()
                    #self.workers[w] = ('Die', self.workers[w][1])
                    #print self.workers[w][0]
            gevent.sleep(1)


    def register_async(self, ip, port):
        print '[Master:%s] ' % self.state,
        print 'Registered worker (%s,%s)' % (ip, port)
        c = zerorpc.Client(timeout=0.5)
        c.connect("tcp://" + ip + ':' + port)
        self.workers[(ip,port)] = ('READY', c)
        c.ping()

    def register(self, ip, port):
        gevent.spawn(self.register_async, ip, port)


    def update_work(self, work_done):
        self.works.remove(work_done)
        print self.works

    def alived_worker(self):
        self.lock.acquire()
        count = 0
        for w in self.workers:
            if self.workers[w][0] != 'Die':
                count += 1
        self.lock.release()
        return count


    def do_job(self):

        while True:

            # init
            n = self.alived_worker()
            chunk = len(self.works) / n
            i = 0
            offset = 0
            procs = []

            # break condition
            if len(self.works) <= 0:
                gevent.sleep(1)
                break

            # map
            self.lock.acquire()
            print "workers: " + str(n)
            for w in self.workers:
                if self.workers[w][0] == 'Die':
                    continue
                if i == (n - 1):
                    filenames = self.works[offset:]
                else:
                    filenames = self.works[offset:offset+chunk]
                proc = gevent.spawn(self.workers[w][1].do_work, data_dir, filenames, 'map')
                print proc
                procs.append(proc)

                i += 1
                offset += chunk
            self.lock.release()


            gevent.joinall(procs)

            gevent.sleep(1)

        print "##### end of mapping, start reducing "

        # restore work list
        self.works = self.all_works[:]

        while True:

            # init
            n = self.alived_worker()
            chunk = len(self.works) / n
            i = 0
            offset = 0
            procs = []

            # break condition
            if len(self.works) <= 0:
                gevent.sleep(1)
                break

            # map
            self.lock.acquire()
            print "workers: " + str(n)
            for w in self.workers:
                if self.workers[w][0] == 'Die':
                    continue

                if i == (n - 1):
                    filenames = self.works[offset:]
                else:
                    filenames = self.works[offset:offset+chunk]
                proc = gevent.spawn(self.workers[w][1].do_work, data_dir, filenames, 'reduce')
                procs.append(proc)

                i += 1
                offset += chunk

            self.lock.release()
            gevent.joinall(procs)


            print "############### done"
            gevent.sleep(1)


        return

    def split_file(self, filename):
        splitLen = 150         # 20 lines per file
        outputBase = 'output'  # output.1.txt, output.2.txt, etc.

        input = open(filename, 'r').read().split('\n')

        at = 1
        for lines in range(0, len(input), splitLen):
            # First, get the list slice
            outputData = input[lines:lines+splitLen]

            # Now open the output file, join the new slice with newlines
            # and write it out. Then close the file.
            output = open(data_dir + outputBase + str(at) + '.txt', 'w')
            self.works.append(outputBase + str(at) + '.txt')
            self.all_works.append(outputBase + str(at) + '.txt')
            output.write('\n'.join(outputData))
            output.close()

            # Increment the counter
            at += 1


if __name__ == '__main__':

    port = sys.argv[1]
    data_dir = sys.argv[2]
    if data_dir[len(data_dir)-1] != '/':
        data_dir += '/'

    master_addr = 'tcp://0.0.0.0:' + port

    s = zerorpc.Server(Master())
    s.bind(master_addr)
    s.run()