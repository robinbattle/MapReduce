__author__ = 'blu2'
__author__ = 'jtanigawa'

import sys

import zerorpc
import gevent


class Master(object):

    def __init__(self):
        gevent.spawn(self.controller)
        self.state = 'Ready'
        self.workers = {}
        self.current_map_works = []
        self.current_reduce_works = []
        self.all_works = []
        self.ready_to_reduce = False

    def controller(self):
        while True:

            print '[Master:%s] ' % (self.state),
            for w in self.workers:
                print '(%s,%s,%s)' % (w[0], w[1], self.workers[w][0]),
            print

            print "map work: " + str(self.current_map_works)
            print "reduce work: " + str(self.current_reduce_works)

            for w in self.workers:
                try:
                    if self.workers[w][0] != 'Die':

                        worker_status = self.workers[w][1].ping()
                        work_status = worker_status[0]
                        print "### checking: " + str(self.workers[w][0])
                        if self.workers[w][0] != "Finished":
                            finished_work = worker_status[1]
                            work_type = worker_status[2]
                            if work_type == "Map":
                                for work in finished_work:
                                    if work in self.current_map_works:
                                        # remove from map work, add into reduce work
                                        self.current_map_works.remove(work)
                                        self.current_reduce_works.append(work)
                                        self.ready_to_reduce = True
                            elif work_type == "Reduce":
                                for work in finished_work:
                                    if work in self.current_reduce_works:
                                        # remove from reduce work
                                        self.current_reduce_works.remove(work)

                            self.workers[w] = (work_status, self.workers[w][1])
                        elif self.workers[w][0] == "Finished":
                            self.workers[w] = ("Ready", self.workers[w][1])
                    else:
                        print self.workers[w][0]
                except:
                    self.workers[w] = ('Die', self.workers[w][1])
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

    def alived_worker(self):
        count = 0
        for w in self.workers:
            if self.workers[w][0] != 'Die':
                count += 1
        return count

    def map_job(self):

        while True:
            # init
            n = self.alived_worker()
            chunk = len(self.current_map_works) / n
            i = 0
            offset = 0
            procs = []

            # break condition
            if len(self.current_map_works) <= 0:
                gevent.sleep(1)
                break

            # map
            print "workers: " + str(n)
            for w in self.workers:
                if self.workers[w][0] == 'Die':
                    continue
                if i == (n - 1):
                    filenames = self.current_map_works[offset:]
                else:
                    filenames = self.current_map_works[offset:offset+chunk]
                proc = gevent.spawn(self.workers[w][1].do_work, data_dir, filenames, 'Map')
                print proc
                procs.append(proc)

                i += 1
                offset += chunk


            gevent.joinall(procs)

            gevent.sleep(1)

        print "##### end of mapping"

    def reduce_job(self):

        while len(self.current_reduce_works) > 0 or len(self.current_map_works) > 0:

            if len(self.current_reduce_works) == 0:
                gevent.sleep(1)
                continue


            # init
            n = self.alived_worker()
            print "# lived worker: " + str(n)
            chunk = len(self.current_reduce_works) / n
            i = 0
            offset = 0
            procs = []

            # break condition
            if len(self.current_reduce_works) <= 0:
                gevent.sleep(1)
                break

            # map
            print "workers: " + str(n)
            for w in self.workers:
                if self.workers[w][0] == 'Die':
                    continue

                if i == (n - 1):
                    filenames = self.current_reduce_works[offset:]
                else:
                    filenames = self.current_reduce_works[offset:offset+chunk]
                proc = gevent.spawn(self.workers[w][1].do_work, data_dir, filenames, 'Reduce')
                procs.append(proc)

                i += 1
                offset += chunk

            gevent.joinall(procs)


            print "############### done"
            gevent.sleep(1)
        print "##### end of reducing"

    def do_job(self):
        gevent.spawn(self.map_job)

        while not self.ready_to_reduce:
            print "$$$" + " wait for mapping file"
            gevent.sleep(0.5)
            continue

        print "# start reducing"
        gevent.spawn(self.reduce_job)


    def split_file(self, filename):
        splitLen = 1000         # 20 lines per file
        outputBase = 'output'  # output.1.txt, output.2.txt, etc.

        input = open(filename, 'r').read().split('\n')

        at = 1
        for lines in range(0, len(input), splitLen):
            # First, get the list slice
            outputData = input[lines:lines+splitLen]

            # Now open the output file, join the new slice with newlines
            # and write it out. Then close the file.
            output = open(data_dir + outputBase + str(at) + '.txt', 'w')
            self.current_map_works.append(outputBase + str(at) + '.txt')
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