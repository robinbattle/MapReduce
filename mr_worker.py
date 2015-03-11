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

        if work_type == "map":
            for filename in filenames:
                print "map_" + filename
                self.map(data_dir, filename)

        elif work_type == "reduce":
            for filename in filenames:
                print "map_" + filename + ", reduce_" + filename
                self.reduce(data_dir, filename)


    def update_work_status_async(self, filename):
        c = zerorpc.Client()
        c.connect(master_addr)
        c.update_work(filename)

    def map(self, data_dir, filename):
        input_filename = filename
        output_filename = "map_" + filename
        input = open(data_dir + input_filename, 'r').read().split('\n')
        output = open(data_dir + output_filename, 'w')
        for line in range(0, len(input)):
            words = input[line].split(' ')
            for word in words:
                output.write(word + "=1,")

        output.close()

        # update work status
        self.update_work_status_async(filename)


    def reduce(self, data_dir, filename):
        input_filename = "map_" + filename
        output_filename = "reduce_" + filename
        input = open(data_dir + input_filename, 'r').read().split('\n')
        output = open(data_dir + output_filename, 'w')
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

        # update work status
        self.update_work_status_async(filename)


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





