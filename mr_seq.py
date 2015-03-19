__author__ = 'blu2'
__author__ = 'jtanigawa'


def read_from_file(self, data_dir, filename):
    input = open(data_dir + filename, 'r').read()
    return input

def write_to_file(self, data_dir, filename):
    output = open(data_dir + filename, 'w')
    output.write(str(self.reduce_dict))
    output.close()



