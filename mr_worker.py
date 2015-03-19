__author__ = 'blu2'
__author__ = 'jtanigawa'

import sys

import zerorpc
import gevent

import math
import re


class Worker(object):

    def __init__(self):
        gevent.spawn(self.controller)
        self.finished_map_works = []
        self.finished_reduce_works = []
        self.map_status = "Ready"
        self.reduce_status = "Ready"
        self.map_work_index = 0, 0
        self.num_reducers = 0

        self.current_map = ""
        self.transmitting_index = []
        self.reduce_work = []
        self.reduce_worker_index = 0

        self.reduce_dict = {}
        pass

    def controller(self):
        while True:
            print('[Worker]')
            gevent.sleep(1)

    def ping(self):
        print '[Worker] Ping from Master'
        map_status = self.map_status

        if map_status == "Finished":
            if len(self.transmitting_index) == 0:
                self.map_status = "Ready"

        reduce_status = self.reduce_status

        if reduce_status == "Finished":
            print "I am Finished(reduce)"
            self.reduce_status = "Ready"


        #if map_status == "Finished":
        #    self.map_status = "Ready"

        return map_status, self.map_work_index, self.transmitting_index, reduce_status

    def grab_map_chunk(self, reduce_work):
        print "try to grab"
        index = reduce_work[0]
        ip = reduce_work[1]
        port = reduce_work[2]

        c = zerorpc.Client(timeout=2)
        c.connect("tcp://" + ip + ':' + port)
        text = c.send_map_chunk(index)
        return text

    def send_map_chunk(self, index):
        start = index[0]
        end = index[1]
        text = self.current_map[start:end]
        self.transmitting_index.remove((start, end))
        print "Trans: " + str(self.transmitting_index)
        return text



    def read_from_file(self, data_dir, filename):
        input = open(data_dir + filename, 'r').read()
        return input

    def write_to_file(self, data_dir, filename):
        output = open(data_dir + filename, 'w')

        for key in self.reduce_dict.keys():
            output.write(str(key) + '=' + str(self.reduce_dict[key]) + '\n')

        #output.write(str(self.reduce_dict))
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


    def do_map(self, data_dir, filename, work_index, num_reducers):
        self.num_reducers = num_reducers
        self.map_work_index = work_index
        self.map_status = "Working"
        start = work_index[0]
        end = work_index[1]
        input = self.read_from_file(data_dir, filename)[start:end]
        output = self.map(input)
        self.current_map = output
        self.split_map()
        print input
        print output

        print "work_index:" + str(work_index)
        print "Trans: " + str(self.transmitting_index)

        self.map_status = "Finished"

    def do_reduce(self, reduce_work):
        self.reduce_status = "Working"
        print "*******" + str(reduce_work)
        map_chunk = self.grab_map_chunk(reduce_work)
        print map_chunk
        self.reduce(map_chunk)

        self.reduce_status = "Finished"



    def map(self, input):
        s_list = []
        words = input.split()
        for word in words:
            #word = word.replace('\n', '')
            word = re.sub('[^0-9a-zA-Z]+', '', word)
            s_list.append(word + "=1")

        # combine
        s_list = self.combine(s_list)
        # sort
        s_list.sort()

        return s_list
        #return ''.join(str(s_list))


    def combine(self, words):
        d = {}
        for word in words:
            elems = word.split('=')
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

        dictlist = []
        for key, value in d.iteritems():
            temp = str(key) + '=' + str(value)
            dictlist.append(temp)

        return dictlist

    def split_map(self):
        output = self.current_map
        chunk = int(math.ceil(float(len(output))/self.num_reducers))
        split_list = []
        offset = 0

        print "chunk:" + str(chunk)
        print "len: " + str(len(output))

        while offset < len(output):
            print offset

            if offset + chunk > len(output):
                work = offset, len(output)
            else:
                work = offset, offset + chunk
            split_list.append(work)
            offset += chunk

        self.transmitting_index = split_list

        return split_list



    def reduce(self, words):
        for word in words:
            elems = word.split('=')
            if len(elems) != 2:
                continue
            key = elems[0]
            try:
                value = int(elems[1])
            except:
                # this may need to be handle properly later
                value = 0
                print elems[1]

            if key in self.reduce_dict:
                old_value = int(self.reduce_dict[key])
                self.reduce_dict[key] = old_value + value
            else:
                self.reduce_dict[key] = value


    def update_reduce_work(self, reduce_works, index):
        self.reduce_worker_index = index
        for reduce_work in reduce_works:
            self.reduce_work.append([reduce_work[0][index], reduce_work[1]])


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





