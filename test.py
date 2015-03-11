__author__ = 'blu2'

import sys

import zerorpc
import gevent

master_addr = 'tcp://0.0.0.0:4242'

class Master(object):

    def __init__(self):
        gevent.spawn(self.controller)
        self.state = 'READY'
        self.workers = {}
        self.fworkder = []
        self.works = []

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

    def do_job(self):
        n = len(self.workers)
        chunk = len(self.works) / n
        i = 0
        offset = 0
        procs = []
        for w in self.workers:
            if i == (n - 1):
                filenames = self.works[offset:]
            else:
                filenames = self.works[offset:offset+chunk]
            proc = gevent.spawn(self.workers[w][1].do_work, filenames, 'map')
            procs.append(proc)

            i = i + 1
            offset = offset + chunk

        gevent.joinall(procs)


        procs = []
        i = 0
        offset = 0
        for w in self.workers:
            if i == (n - 1):
                filenames = self.works[offset:]
            else:
                filenames = self.works[offset:offset+chunk]
            proc = gevent.spawn(self.workers[w][1].do_work, filenames, 'reduce')
            procs.append(proc)

            i = i + 1
            offset = offset + chunk

        gevent.joinall(procs)
        return

    def split_file(self, filename):
        splitLen = 150         # 20 lines per file
        outputBase = 'output' # output.1.txt, output.2.txt, etc.

        input = open(filename, 'r').read().split('\n')

        at = 1
        for lines in range(0, len(input), splitLen):
            # First, get the list slice
            outputData = input[lines:lines+splitLen]

            # Now open the output file, join the new slice with newlines
            # and write it out. Then close the file.
            output = open(outputBase + str(at) + '.txt', 'w')
            self.works.append(outputBase + str(at) + '.txt')
            output.write('\n'.join(outputData))
            output.close()

            # Increment the counter
            at += 1


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

    def do_work(self, filenames, work_type):
        if work_type == "map":
            for filename in filenames:
                print "map_" + filename
                self.map(filename, "map_" + filename)

        elif work_type == "reduce":
            for filename in filenames:
                print "map_" + filename + ", reduce_" + filename
                self.reduce("map_" + filename, "reduce_" + filename)

        #gevent.sleep(2)

        return 1

    def get_status(self):
        return self.status

    def map(self, input_filename, output_filename):
        input = open(input_filename, 'r').read().split('\n')
        output = open(output_filename, 'w')
        for line in range(0, len(input)):
            words = input[line].split(' ')
            for word in words:
                output.write(word + "=1,")

        output.close()

    def reduce(self, input_filename, output_filename):
        input = open(input_filename, 'r').read().split('\n')
        output = open(output_filename, 'w')
        for line in range(0, len(input)):
            maps = input[line].split(',')
            d = {}
            for map in maps:

                elems = map.split("=")
                if len(elems) != 2:
                    continue


                key = elems[0]
                value = int(elems[1])
                if key in d:
                    oldValue = int(d[key])
                    d[key] = oldValue + value
                else:
                    d[key] = value

            for key in d:
                output.write(key + "=" + str(d[key]) + ",")


        output.close()


if __name__ == '__main__':

    cmd = sys.argv[1]

    if cmd == 'master':
        s = zerorpc.Server(Master())
        s.bind(master_addr)
        s.run()
    elif cmd == 'worker':
        s = zerorpc.Server(Worker())
        ip = '0.0.0.0'
        port = sys.argv[2]
        s.bind('tcp://' + ip + ':' + port)
        c = zerorpc.Client()
        c.connect(master_addr)
        c.register(ip, port)
        s.run()
    elif cmd == 'client':
        c = zerorpc.Client()
        c.connect(master_addr)

        c.split_file("input.txt")
        c.do_job()




