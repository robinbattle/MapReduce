__author__ = 'blu2'
__author__ = 'jtanigawa'

import sys

import zerorpc
import gevent


class Worker(object):

    def __init__(self):
        gevent.spawn(self.controller)
        self.finished_map_works = []
        self.finished_reduce_works = []
        self.map_status = "Ready"
        self.reduce_status = "Ready"
        pass

    def controller(self):
        while True:
            print('[Worker]')
            gevent.sleep(1)

    def ping(self):
        print '[Worker] Ping from Master'
        map_status = self.map_status
        reduce_status = self.reduce_status
        finished_map_works_copy = self.finished_map_works[:]
        finished_reduce_works_copy = self.finished_reduce_works[:]
        self.finished_map_works = []
        self.finished_reduce_works = []

        if map_status == "Finished":
            self.map_status = "Ready"
        if reduce_status == "Finished":
            self.reduce_status = "Ready"

        return map_status, finished_map_works_copy, reduce_status, finished_reduce_works_copy

    def read_from_file(self, data_dir, filename):
        input = open(data_dir + filename, 'r').read().split('\n')
        return input

    def write_to_file(self, data_dir, filename, output_data):
        output = open(data_dir + filename, 'w')
        output.write(str(output_data))
        output.close()

    def do_work(self, data_dir, filenames, work_type):

        if work_type == "Map":
            self.map_status = "Working"
            for filename in filenames:
                print "map_" + filename
                input_filename = filename
                output_filename = "map_" + filename
                input = self.read_from_file(data_dir, input_filename)
                output = self.map(input)
                self.write_to_file(data_dir, output_filename, output)
                self.finished_map_works.append(filename)
            self.map_status = "Finished"
        elif work_type == "Reduce":
            self.reduce_status = "Working"
            for filename in filenames:
                print "map_" + filename + ", reduce_" + filename
                input_filename = "map_" + filename
                output_filename = "reduce_" + filename
                input = self.read_from_file(data_dir, input_filename)
                output = self.reduce(input)
                self.write_to_file(data_dir, output_filename, output)
                # update work status
                self.finished_reduce_works.append(filename)
            self.reduce_status = "Finished"

    def map(self, input):
        s_list = []
        for line in range(0, len(input)):
            words = input[line].split(' ')
            for word in words:
                s_list.append(word + "=1\n")
        return ''.join(s_list)


    def reduce(self, input):
        d = {}
        for line in range(0, len(input)):
            elems = input[line].split('=')
            if len(elems) != 2:
                continue

            key = elems[0]
            try:
                value = int(elems[1])
            except:
                # this may need to be handle properly later
                value = 0
                print elems[1]

            if key in d:
                old_value = int(d[key])
                d[key] = old_value + value
            else:
                d[key] = value

        s_list = []
        for key in d:
            s_list.append(key + "=" + str(d[key]) + "\n")

        return ''.join(s_list)



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





