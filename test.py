__author__ = 'blu2'
import math

import re

import gevent
import zerorpc

def map(input):
    s_list = []
    words = input.split(' ')
    for word in words:
        word = word.replace('\n', '')
        s_list.append(word + "=1")

    # combine
    s_list = combine(s_list)
    # sort
    s_list.sort()

    return s_list
    #return ''.join(str(s_list))


def combine(words):
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
        temp = [key, value]
        dictlist.append(temp)

    return dictlist


def split_map():

    chunk = int(math.ceil(float(len(output))/num_reducers))
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

    transmitting_index = split_list

    return split_list





class Test(object):

    def fun1(self):
        #while True:
        #    print 1
        #    gevent.sleep(1)
        pass

    def fun2(self):
        print 2


def foo():

    #print('Running in foo')
    gevent.sleep(1)
    int('a')
    #print('Explicit context switch to foo again')

def bar():
    #print('Explicit context to bar')
    gevent.sleep(0)
    print 1
    #print('Implicit context switch back to bar')


try:
    procs = []
    procs.append(gevent.spawn(foo))
    procs.append(gevent.spawn(bar))




    gevent.joinall(procs, raise_error=True)
except:
    print "error"


"""
num_reducers = 3
chunk = 30
input = open("file/input.txt", 'r').read()

output = "not working at all be careful haha cool".split()
print split_map()


s = 'h^&ell`.,|o w]{+orld'
s = s.replace('[^0-9a-zA-Z]+', '')
s = re.sub('[^0-9a-zA-Z]+', '', s)
print s



a = ["a", "b", "c"]

for s in a:
    if s == "b":
        a.remove(s)
print a





split_list = []
offset = 0
print "len: " + str(len(input))

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


print split_list



for index in split_list:
    print "------------"
    line =  input[index[0] : index[1]]
    print line
    print "###########"
    print line.split()

"""
#output = map(input)
#print output
#print split_map()

#print output[0:25]
