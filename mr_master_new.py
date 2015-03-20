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
        self.map_workers = []
        self.reduce_workers = []
        self.current_map_works = []
        self.current_reduce_works = {}
        self.map_works_in_progress = {}
        self.reduce_works_in_progress = {}
        self.all_works = []
        self.ready_to_reduce = False

        self.input_filename = ""

    def controller(self):
        while True:

            print '[Master:%s] ' % (self.state),
            for w in self.workers:
                print '(%s,%s,%s)' % (w[0], w[1], self.workers[w][0]),
            print

            print "current map work:" + str(self.current_map_works)
            print "current reduce work:" + str(self.current_reduce_works)


            for w in self.workers:
                try:
                    if self.workers[w][0] != 'Die':
                        worker_status = self.workers[w][1].ping()
                        map_work_status = worker_status[0]
                        if self.workers[w][0][0] == 'Working':
                            if map_work_status == 'Finished':
                                transmitting_index = worker_status[2]
                                print "add work here %%%%%%%%%"
                                self.add_to_reduce_work(transmitting_index, w[0], w[1])
                                #self.current_reduce_works.append([transmitting_index, w[1]])
                                print "###" + str(self.current_reduce_works)
                                #self.update_reduce_work_list_on_worker(self.current_reduce_works)
                                map_work_status = "Finished"

                                new_status = [map_work_status, self.workers[w][0][1]]
                                self.workers[w] = (new_status, self.workers[w][1])

                        elif self.workers[w][0][0] == 'Finished':
                            print "@@@ self_map in progress" + str(self.map_works_in_progress)

                            if w in self.map_works_in_progress:
                                del self.map_works_in_progress[w]

                            if map_work_status == 'Ready':
                                print "add back to map worker" + str(w)
                                self.map_workers.append(w)
                                map_work_status = "Ready"

                                new_status = [map_work_status, self.workers[w][0][1]]
                                self.workers[w] = (new_status, self.workers[w][1])

                        elif self.workers[w][0][0] == 'Ready':
                            print "I am (map) ready"

                        reduce_work_status = worker_status[3]

                        print "reduce_work_status:" + str(reduce_work_status)

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
                        print self.workers[w][0]
                except:
                    if w in self.map_works_in_progress:
                        self.current_map_works.append(self.map_works_in_progress[w])
                        del self.map_works_in_progress[w]
                        print "#######################"

                    if w in self.reduce_works_in_progress:
                        reduce_work_list = self.reduce_works_in_progress[w]
                        new_w = self.pick_new_reducer()
                        if new_w is not None:
                            print "%%%%%%%%%%%%%%%%%%%%%%"
                            print "Add:" + str(new_w)
                            print "Remove:" + str(w)


                            # add new reducer
                            self.reduce_workers.append(new_w)
                            # remove old reducer
                            self.reduce_workers.remove(w)
                            # reassign reduce work in progress
                            print "reduce_work_list_in_progress:" + str(w) + ">> " + str(reduce_work_list)
                            for reduce_work in reduce_work_list:
                                print "Reassign: " + str(reduce_work) + " :" + str(w) + "==>" + str(new_w)

                                if new_w in self.current_reduce_works:
                                    self.current_reduce_works[new_w].append((reduce_work[0], reduce_work[1], reduce_work[2]))
                                else:
                                    self.current_reduce_works[new_w] = [(reduce_work[0], reduce_work[1], reduce_work[2])]
                                # dest may fail
                            print "new_reduce_works: " + str(self.current_reduce_works)
                        print "************************"

                    # temp
                    for worker in self.current_reduce_works:
                        reduce_work_list = self.current_reduce_works[worker]
                        for reduce_work in reduce_work_list:
                            ip = reduce_work[1]
                            port = reduce_work[2]
                            if ip == w[1] and port == w[2]:
                                reduce_work_list.remove(reduce_work)
                                print "***************"
                                print "remove: " + str(reduce_work)
                                print "***************"


                    self.workers[w] = ('Die', self.workers[w][1])

            gevent.sleep(1)

    def add_to_reduce_work(self, transmitting_index, ip, port):
        index = 0
        for w in self.reduce_workers:
            if w in self.current_reduce_works:
                self.current_reduce_works[w].append((transmitting_index[index], ip, port))
            else:
                self.current_reduce_works[w] = [(transmitting_index[index], ip, port)]
            index += 1

    def add_to_reduce_work_in_process(self, w, transmitting_index_elem, ip, port):
        print "Trans: " + str(transmitting_index_elem)
        print "IP:" + str(ip)
        print "Port:" + str(port)

        if w in self.reduce_works_in_progress:
            self.reduce_works_in_progress[w].append((transmitting_index_elem, ip, port))
        else:
            self.reduce_works_in_progress[w] = [(transmitting_index_elem, ip, port)]



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

    def reducer_alive(self):
        for w in self.reduce_workers:
            if self.workers[w][0] is not 'Die':
                    return w
        return None

    def alived_worker(self):
        count = 0
        for w in self.workers:
            if self.workers[w][0] != 'Die':
                count += 1
        return count

    def map_job(self):
        while len(self.current_map_works) > 0:
            if len(self.map_workers) <= 0:
                print "all mappers are busy"
                gevent.sleep(0.1)
                continue
            map_worker_p = self.map_workers.pop()
            map_work_index = self.current_map_works.pop()
            self.map_works_in_progress[map_worker_p] = map_work_index
            print "@@@ self_map in progress" + str(self.map_works_in_progress)

            print "map_worker_p:" + str(map_worker_p)
            print "map_work_index:" + str(map_work_index)

            new_status = ["Working", self.workers[map_worker_p][0][1]]
            self.workers[map_worker_p] = (new_status, self.workers[map_worker_p][1])
            proc = gevent.spawn(self.workers[map_worker_p][1].do_map, data_dir, self.input_filename, map_work_index, num_reducers)

            gevent.sleep(1)

        print "##### end of mapping"

    def reduce_job(self):
        print "##### start reducing"

        while not self.dict_is_empty(self.current_reduce_works) or len(self.current_map_works) > 0\
                or len(self.map_works_in_progress.keys()) > 0:

            print "Map In Progess:" + str(self.map_works_in_progress)

            if len(self.current_reduce_works) == 0:
                print "no reduce work, go to sleep"
                gevent.sleep(1)
                continue
            for w in self.reduce_workers:
                if self.workers[w][0] == 'Die':
                    continue

                if self.workers[w][0][1] == 'Working':
                    continue

                new_status = [self.workers[w][0][0], "Working"]
                self.workers[w] = (new_status, self.workers[w][1])

                if w not in self.current_reduce_works:
                    continue

                if len(self.current_reduce_works[w]) == 0:
                    continue
                reduce_work = self.current_reduce_works[w].pop()

                print "Reduce work: " + str(reduce_work)

                self.add_to_reduce_work_in_process(w, reduce_work[0], reduce_work[1], reduce_work[2])
                gevent.spawn(self.workers[w][1].do_reduce, reduce_work)

            gevent.sleep(1)

        print "##### end of reducing"
        count = 0
        for w in self.reduce_workers:
            print "############ write"
            gevent.spawn(self.workers[w][1].write_to_file, data_dir, "output" + str(count) + ".txt")
            count += 1

    def dict_is_empty(self, dict):
        for key in dict.keys():
            if len(dict[key]) > 0:
                return False
            else:
                continue
        return True

    def update_reduce_work_list_on_worker(self):
            for w in self.reduce_workers:
                index = self.reduce_workers.index()
                gevent.spawn(self.workers[w][1].update_reduce_works, self.current_reduce_works, index)
            self.current_reduce_works = []



    def do_job(self):
        print "in do job"
        print self.current_map_works
        count = 0
        for w in self.workers:
            self.map_workers.append(w)

            if count < num_reducers:
                self.reduce_workers.append(w)
                count += 1

        print "We have " + str(len(self.map_workers)) + " mappers, and " + str(len(self.reduce_workers)) + " reducers"


        gevent.spawn(self.map_job)

        #while not self.ready_to_reduce:
        #    print "$$$" + " wait for mapping file"
        #    gevent.sleep(0.5)
        #    continue

        #print "# start reducing"
        gevent.spawn(self.reduce_job)

    def split_file(self, filename, split_size):
        self.input_filename = filename
        chunk = split_size
        input = open(data_dir + filename, 'r').read()

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

    num_reducers = 1

    s = zerorpc.Server(Master())
    s.bind(master_addr)
    s.run()