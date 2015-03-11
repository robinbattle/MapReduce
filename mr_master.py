__author__ = 'blu2'
__author__ = 'jtanigawa'

import sys

import zerorpc
import gevent



class Master(object):

    def __init__(self):
        gevent.spawn(self.controller)
        self.state = 'READY'
        self.workers = {}
        self.works = []
        self.all_works = []

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
                    #print "Die"
                    self.workers[w] = ('Die', self.workers[w][1])
                    #print self.workers[w][0]
            gevent.sleep(1)


    def register_async(self, ip, port):
        print '[Master:%s] ' % self.state,
        print 'Registered worker (%s,%s)' % (ip, port)
        c = zerorpc.Client()
        c.connect("tcp://" + ip + ':' + port)
        self.workers[(ip,port)] = ('READY', c)
        c.ping()

    def register(self, ip, port):
        gevent.spawn(self.register_async, ip, port)


    def update_work(self, work_done):
        self.works.remove(work_done)
        print self.works

    def do_job(self):
        n = len(self.workers)
        chunk = len(self.works) / n
        i = 0
        offset = 0
        procs = []

        while len(self.works) > 0:

            for w in self.workers:
                if i == (n - 1):
                    filenames = self.works[offset:]
                else:
                    filenames = self.works[offset:offset+chunk]
                proc = gevent.spawn(self.workers[w][1].do_work, data_dir, filenames, 'map')
                procs.append(proc)

                i = i + 1
                offset = offset + chunk

            print gevent.joinall(procs)

        # restore work list
        self.works = self.all_works[:]
        print "self.works " + str(self.works)

        while len(self.works) > 0:
            procs = []
            i = 0
            offset = 0
            for w in self.workers:
                if i == (n - 1):
                    filenames = self.works[offset:]
                else:
                    filenames = self.works[offset:offset+chunk]
                proc = gevent.spawn(self.workers[w][1].do_work, data_dir, filenames, 'reduce')
                procs.append(proc)

                i = i + 1
                offset = offset + chunk

            gevent.joinall(procs)

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