__author__ = 'blu2'
__author__ = 'jtanigawa'

import sys

import zerorpc
import gevent

import subprocess


class Master(object):

    def __init__(self):
        gevent.spawn(self.controller)
        self.state = 'Ready'
        self.workers = {}
        self.map_workers = []
        self.reduce_workers = []
        self.current_map_works = []
        self.current_reduce_works = {}
        self.map_works_in_progress = {}

        self.input_filename = ""
        self.split_size = 10
        self.base_filename = ""
        self.num_reducers = 1

        self.restart = True
        self.finished = False

    def controller(self):
        while True:

            print '[Master:%s] ' % (self.state),
            for w in self.workers:
                print '(%s,%s,%s)' % (w[0], w[1], self.workers[w][0]),
            print

            print "current map work:" + str(self.current_map_works)
            print "current super reduce work:" + str(self.current_reduce_works)

            for w in self.workers:
                try:
                    if self.workers[w][0] != 'Die':
                        worker_status = self.workers[w][1].ping()
                        map_work_status = worker_status[0]
                        if self.workers[w][0][0] == 'Working':
                            if map_work_status == 'Finished':
                                work_index = worker_status[1]
                                transmitting_index = worker_status[2]
                                self.current_reduce_works[work_index[0], work_index[1]] = transmitting_index, w[0], w[1]
                                map_work_status = "Finished"

                                new_status = [map_work_status, self.workers[w][0][1]]
                                self.workers[w] = (new_status, self.workers[w][1])

                        elif self.workers[w][0][0] == 'Finished':
                            if w in self.map_works_in_progress:
                                del self.map_works_in_progress[w]

                            if map_work_status == 'Ready':
                                #print "add back to map worker" + str(w)
                                self.map_workers.append(w)
                                map_work_status = "Ready"

                                new_status = [map_work_status, self.workers[w][0][1]]
                                self.workers[w] = (new_status, self.workers[w][1])

                        elif self.workers[w][0][0] == 'Ready':
                            print "I am (map) ready"

                        reduce_work_status = worker_status[3]

                        if self.workers[w][0][1] == 'Working':
                            if reduce_work_status == 'Finished' or reduce_work_status == 'Ready':
                                new_status = [self.workers[w][0][0], reduce_work_status]
                                self.workers[w] = (new_status, self.workers[w][1])

                        elif self.workers[w][0][1] == 'Finished':

                            if reduce_work_status == 'Ready':
                                new_status = [self.workers[w][0][0], reduce_work_status]
                                self.workers[w] = (new_status, self.workers[w][1])
                        elif self.workers[w][0][1] == 'Ready':
                            print "I am (reduce) ready"

                    else:
                        self.workers[w][0]  # do nothing
                except zerorpc.TimeoutExpired:
                    if w in self.map_works_in_progress:
                        if self.map_works_in_progress[w] not in self.current_map_works:
                            self.current_map_works.append(self.map_works_in_progress[w])
                        del self.map_works_in_progress[w]
                        #print "#######################"

                    new_w = self.pick_new_reducer()
                    if new_w is not None:
                        self.reduce_workers.append(new_w)
                    else:
                        print "Read to restart"
                        self.restart = True
                        self.num_reducers = self.num_avaliable_reducer()
                        print "$$$$$$$$$$$$$$$$$$$$"
                        print "%%%%%%%%%%%%%%%%%%%%"
                        print "@@@@@@@@@@@@@@@@@@@@"
                        print "$$$$$$$$$$$$$$$$$$$$"
                        print "%%%%%%%%%%%%%%%%%%%%"
                        print "@@@@@@@@@@@@@@@@@@@@"







                    self.workers[w] = ('Die', self.workers[w][1])

            gevent.sleep(1)


    def register_async(self, ip, port):
        print '[Master:%s] ' % self.state,
        print 'Registered worker (%s,%s)' % (ip, port)
        c = zerorpc.Client(timeout=0.5)
        c.connect("tcp://" + ip + ':' + port)
        self.workers[(ip,port)] = (['Ready', 'Ready'], c, [])
        c.ping()

    def register(self, ip, port):
        gevent.spawn(self.register_async, ip, port)


    def pick_new_reducer(self):
        for w in self.workers:
            if w not in self.reduce_workers:
                return w
        return None

    def num_avaliable_reducer(self):
        count = 0
        for w in self.reduce_workers:
            if self.workers[w][0] == 'Die':
                continue
            count += 1
        return count

    def num_avaliable_worker(self):
        count = 0
        for w in self.workers:
            if self.workers[w][0] == 'Die':
                continue
            count += 1
        return count

    def reducer_alive(self):
        for w in self.reduce_workers:
            if self.workers[w][0] is not 'Die':
                    return w
        return None

    def reducers_working(self):
        for w in self.reduce_workers:
            if self.workers[w][0][1] == 'Working':
                return True
        return False


    def clear_work_list(self):
        self.current_map_works = []
        self.current_reduce_works = {}
        self.map_works_in_progress = {}


    def reset_status(self):
        self.state = 'Ready'
        self.map_workers = []
        self.reduce_workers = []
        self.current_map_works = []
        self.current_reduce_works = {}
        self.map_works_in_progress = {}

        self.restart = True
        self.finished = False

        self.input_filename = ""
        self.split_size = 10
        self.base_filename = ""
        self.num_reducers = 1



        procs = []
        for w in self.workers:
            proc = gevent.spawn(self.workers[w][1].reset_params)
            procs.append(proc)

        gevent.joinall(procs)

    def map_job(self):
        while len(self.current_map_works) > 0:
            if self.restart:
                break

            if len(self.map_workers) <= 0:
                #print "all mappers are busy"
                gevent.sleep(0.1)
                continue
            map_worker_p = self.map_workers.pop()
            map_work_index = self.current_map_works.pop()
            self.map_works_in_progress[map_worker_p] = map_work_index
            #print "map_worker_p:" + str(map_worker_p)
            #print "map_work_index:" + str(map_work_index)

            new_status = ["Working", self.workers[map_worker_p][0][1]]
            self.workers[map_worker_p] = (new_status, self.workers[map_worker_p][1])
            proc = gevent.spawn(self.workers[map_worker_p][1].do_map, data_dir, self.input_filename, map_work_index,
                                self.num_reducers)

            gevent.sleep(0.5)

        print "##### end of mapping"

    def reduce_job(self):
        print "##### start reducing"
        print self.current_map_works


        while len(self.current_reduce_works.keys()) > 0 or len(self.current_map_works) > 0 or \
            len(self.map_works_in_progress.keys()) > 0:

            if self.restart:
                break


            #print "########## i am in reducing circle"

            if len(self.current_reduce_works.keys()) == 0:
                gevent.sleep(0.1)
                continue

            reduce_work_key = self.current_reduce_works.keys()[0]
            reduce_work_list = self.current_reduce_works[reduce_work_key]
            transitting_index = reduce_work_list[0]
            ip = reduce_work_list[1]
            port = reduce_work_list[2]

            print "Trannsitting index:" + str(transitting_index)

            try:

                index = 0
                procs = []
                #print "self.reduce_workers: " + str(self.reduce_workers)
                #for w in self.reduce_workers:
                #    print str(w) + str(self.workers[w])

                if self.reducers_working():
                    print "not all reducers ready, wait"
                    gevent.sleep(0.1)
                    continue

                for w in self.reduce_workers:
                    if self.workers[w][0] == 'Die':
                        continue
                    if self.workers[w][0][1] == 'Working':
                        continue
                    new_status = [self.workers[w][0][0], "Working"]
                    self.workers[w] = (new_status, self.workers[w][1])

                    file_index = self.reduce_workers.index(w)
                    if index > len(transitting_index) - 1:
                        break
                    reduce_index = transitting_index[index]
                    reduce_work = reduce_index, ip, port
                    print str(w) + " will do " + str(reduce_work)
                    proc = gevent.spawn(self.workers[w][1].do_reduce, reduce_work, data_dir, self.base_filename, file_index)
                    procs.append(proc)
                    index += 1
                gevent.joinall(procs, raise_error=True)

                #print "finished reduce work"
                #print "start output"

                procs = []
                for w in self.reduce_workers:
                    if self.workers[w][0] == 'Die':
                        continue
                    file_index = self.reduce_workers.index(w)
                    proc = gevent.spawn(self.workers[w][1].write_to_file, data_dir, self.base_filename + str(file_index) + ".txt")
                    procs.append(proc)
                gevent.joinall(procs, raise_error=True)


                print "***** delete key:" + str(reduce_work_key)
                del self.current_reduce_works[reduce_work_key]

                #print "finished output"
            except zerorpc.TimeoutExpired:
                self.current_map_works.append(reduce_work_key)

                c = zerorpc.Client(timeout=2)
                c.connect("tcp://" + ip + ':' + port)
                c.force_reset_to_map_ready()
                #print ip + ':' + port + " should be ready"

                new_w = self.pick_new_reducer()
                if new_w is not None:
                    self.reduce_workers.append(new_w)
                else:
                    print "HHHHHHHHHHHHHHHHHHHH"
                    self.restart = True
                    self.num_reducers = self.num_avaliable_reducer()
                    print "$$$$$$$$$$$$$$$$$$$$"
                    print "%%%%%%%%%%%%%%%%%%%%"
                    print "@@@@@@@@@@@@@@@@@@@@"
                    print "$$$$$$$$$$$$$$$$$$$$"
                    print "%%%%%%%%%%%%%%%%%%%%"
                    print "@@@@@@@@@@@@@@@@@@@@"

                #print "######################################"
                #print "add " + str(reduce_work_key) + " back to self.current_map_work"
                #print "######################################"



            gevent.sleep(0.5)

        if not self.restart:
            self.finished = True

    def do_word_count(self, filename, split_size, num_reducers, base_filename):

        self.reset_status()


        # init params
        self.input_filename = filename
        self.split_size = int(split_size)
        self.num_reducers = int(num_reducers)
        self.base_filename = base_filename




        #self.map_workers.append(('0.0.0.0', '10001'))
        #self.map_workers.append(('0.0.0.0', '10002'))
        #self.reduce_workers.append(('0.0.0.0', '10000'))
        #self.reduce_workers.append(('0.0.0.0', '10001'))


        procs = []
        while True:
            gevent.sleep(3)
            if self.restart:
                print "#############################################################################"

                num_avaliable_worker = self.num_avaliable_worker()
                if self.num_reducers > num_avaliable_worker:
                    self.num_reducers = num_avaliable_worker

                # clear map/reduce worker list
                self.map_workers = []
                self.reduce_workers = []

                # create map/reduce worker list
                count = 0
                for w in self.workers:
                    if self.workers[w][0] == 'Die':
                        continue
                    self.map_workers.append(w)
                    if count < self.num_reducers:
                        self.reduce_workers.append(w)
                        count += 1

                print "We have " + str(len(self.map_workers)) + " mappers, and " + str(len(self.reduce_workers)) + " reducers"
                print "Mapper: " + str(self.map_workers)
                print "Reducers:" + str(self.reduce_workers)

                # clear map/reduce work list
                self.clear_work_list()
                # split file, this will also assign work to current_map_work
                self.split_file()

                # reset restart
                self.restart = False

                for proc in procs:
                    gevent.kill(proc)
                procs = []
                # spawn map job
                procs.append(gevent.spawn(self.map_job))
                # spawn reduce job
                procs.append(gevent.spawn(self.reduce_job))
                gevent.joinall(procs)
            if self.finished:
                break
            gevent.sleep(1)





    def split_file(self):
        chunk = self.split_size
        input = open(data_dir + self.input_filename, 'r').read()

        split_list = []
        offset = 0

        while offset < len(input):
            end = input.find(' ', offset + chunk)
            if end != -1:
                new_chunk = end - offset
                work = offset, offset + new_chunk
                split_list.append(work)
                offset += new_chunk
            else:
                new_chunk = len(input) - 1 - offset
                work = offset, offset + new_chunk
                split_list.append(work)
                break

        self.current_map_works = split_list

        return split_list


if __name__ == '__main__':

    port = sys.argv[1]
    data_dir = sys.argv[2]
    if data_dir[len(data_dir)-1] != '/':
        data_dir += '/'

    master_addr = 'tcp://0.0.0.0:' + port

    s = zerorpc.Server(Master())
    s.bind(master_addr)
    s.run()