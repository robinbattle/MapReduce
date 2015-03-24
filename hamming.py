import sys


class Parity(object):

    def get_parity(self, s, position):
        counter = 0
        skip = False
        sum = 0
        for i in range(position - 1, len(s)):
            if counter == position:
                counter = 0
                if skip:
                    skip = False
                else:
                    skip = True
            counter += 1
            if skip:
                continue
            else:
                b = s[i]
                if b == '1':
                    sum += 1
        parity = sum % 2
        return parity

    def get_ascii(self, byte_str):
        # Get ASCII character from a binary string
        data = byte_str[1:8]
        value = int(data, 2)
        return chr(value)


class HammingASCII(Parity):

    def encode(self, text):
        encode_list = ""
        for c in text:
            # Get binary string representation of c
            binary_str = "{0:08b}".format(ord(c))
            harming_str = self.get_hamming_bit(binary_str)
            encode_list += harming_str

        return encode_list

    def get_hamming_bit(self, binary_str):
        encode_list = ""
        pow = 0
        start = 0
        end = 0
        insert_parity_bit = True
        while insert_parity_bit:

            position = 2 ** pow
            start = end
            end = start + position - 1
            if end >= len(binary_str):
                end = len(binary_str)
                insert_parity_bit = False

            encode_list += "_" + binary_str[start:end]
            pow += 1

        pow = 1
        position = 1
        while position < len(encode_list):
            parity = self.get_parity(encode_list, position)
            encode_list = encode_list[: position-1] + str(parity) + encode_list[position:]
            position = 2 ** pow
            pow += 1
        return encode_list

    def decode(self, decode_list):
        out_string = ''
        while len(decode_list) > 0:
            if decode_list[0] == '\n':
                break
            hamming_str = decode_list[0:12]
            byte_str = self.strip_hamming_bit(hamming_str)
            out_string += self.get_ascii(byte_str)
            decode_list = decode_list[12:]

            if len(decode_list) < 12:
                break

        return out_string

    def strip_hamming_bit(self, hamming_str):
        pow = 0
        decode_str = ""
        while True:
            position = 2 ** pow
            position_next = 2 ** (pow + 1) - 1


            if position > len(hamming_str):
                break
            if position_next > len(hamming_str):
                position_next = len(hamming_str)

            decode_str += hamming_str[position:position_next]
            pow += 1
        return decode_str

    def chk(self, text):

        list = []
        pos = 0
        while len(text) > 0:
            if text[0] == '\n':
                break
            hamming_str = text[0:12]
            adder = self.check_hamming_bit(hamming_str)
            if adder > 0:
                list.append(pos*12 + adder)
            pos += 1
            text = text[12:]

            if len(text) < 12:
                break
        return list

    def check_hamming_bit(self, hamming_str):
        # set "parity bit" to "_"
        pow = 0
        checker_list = ""
        while True:
            position = 2 ** pow
            position_next = 2 ** (pow + 1) - 1

            if position > len(hamming_str):
                break
            if position_next > len(hamming_str):
                position_next = len(hamming_str)

            checker_list += "_" + hamming_str[position:position_next]
            pow += 1

        # compare "existing parity bit" and "calculating parity bit"
        pow = 1
        position = 1
        adder = 0
        while position < len(hamming_str):
            existing_bit = int(hamming_str[position - 1])
            calculating_bit = self.get_parity(checker_list, position)

            if existing_bit != calculating_bit:
                adder += position

            position = 2 ** pow
            pow += 1
        return adder

    def fix(self, text):
        positions = self.chk(text)

        char_array = list(text)

        for position in positions:
            if char_array[position-1] == '1':
                char_array[position-1] = '0'
            elif char_array[position-1] == '0':
                char_array[position-1] = '1'
            else:
                print "file corrupted"
                sys.exit()

        return "".join(char_array)

    def err(self, pos, text):
        pos = int(pos)
        char_array = list(text)

        current_bit = char_array[pos-1]
        if current_bit == '1':
            char_array[pos-1] = '0'
        elif current_bit == '0':
            char_array[pos-1] = '1'

        #return text
        return "".join(char_array)


class HammingBinary():

    ascii = HammingASCII()

    def bin2asc(self, code):
        decode_list = ""
        for c in code:
            binary_str = "{0:08b}".format(ord(c))
            decode_list += binary_str
        return decode_list

    def asc2bin(self, code):
        binary_encode_array = []
        while len(code) > 0:
            bin_char = chr(int(code[:8], 2))
            binary_encode_array.append(bin_char)
            code = code[8:]
        return "".join(binary_encode_array)

    def makeupByte(self, code):
        if len(code) % 8 != 0:
            code += '0000'
        return code

    def encode(self, text):
        # convert as ascii
        encode_list = self.ascii.encode(text)
        # try make up bytes
        encode_list = self.makeupByte(encode_list)
        # convert to binary
        bin_code = self.asc2bin(encode_list)
        return bin_code

    def decode(self, code):
        # convert as ascii
        decode_list = self.bin2asc(code)
        # convert to text
        text = self.ascii.decode(decode_list)
        return text

    def chk(self, code):
        # convert as ascii
        decode_list = self.bin2asc(code)
        # check as ascii
        positions = self.ascii.chk(decode_list)
        return positions

    def fix(self, code):
        # convert as ascii
        decode_list = self.bin2asc(code)
        # fix as ascii
        fix_code = self.ascii.fix(decode_list)
        # convert to binary
        bin_code = self.asc2bin(fix_code)
        return bin_code

    def err(self, pos, code):
        # convert as ascii
        decode_list = self.bin2asc(code)
        # error as ascii
        error_code = self.ascii.err(pos, decode_list)
        # convert to binary
        bin_code = self.asc2bin(error_code)
        return bin_code


class HammingFile(object):
    ascii = HammingASCII()
    binary = HammingBinary()

    def __init__(self, code_type, in_filename, out_filename):
        self.code_type = code_type
        self.in_filename = in_filename
        self.out_filename = out_filename

    def encode_file(self):
        infile = open(self.in_filename)
        outfile = open(self.out_filename, 'wb')
        text = infile.read()
        encoded_text = ""
        if self.code_type == 'asc':
            encoded_text = self.ascii.encode(text)
        elif self.code_type == "bin":
            encoded_text = self.binary.encode(text)

        outfile.write(''.join(encoded_text))

        infile.close()
        outfile.close()

    def decode_file(self):
        """Return decoded file as a string."""
        infile = open(self.in_filename)
        text = infile.read()
        decoded_text = ""
        if self.code_type == "asc":
            decoded_text = self.ascii.decode(text)
        elif self.code_type == "bin":
            decoded_text = self.binary.decode(text)
        outfile = open(self.out_filename, 'w')
        outfile.write(decoded_text)

    def check_file(self):
        infile = open(self.in_filename)
        text = infile.read()
        if self.code_type == "asc":
            positions = self.ascii.chk(text)
        elif self.code_type == "bin":
            positions = self.binary.chk(text)
        return positions

    def fix_file(self):
        infile = open(self.in_filename)
        text = infile.read()
        if self.code_type == "asc":
            fix_text = self.ascii.fix(text)
        elif self.code_type == "bin":
            fix_text = self.binary.fix(text)
        outfile = open(self.out_filename, 'w')
        outfile.write(fix_text)

    def error_file(self, pos):
        infile = open(self.in_filename)
        text = infile.read()
        if self.code_type == "asc":
            error_text = self.ascii.err(pos, text)
        elif self.code_type == "bin":
            error_text = self.binary.err(pos, text)
        outfile = open(self.out_filename, 'w')
        outfile.write(error_text)

    def __str__(self):
        return self.decoded_text


class Usage(object):
    def feedback(self):
        text = "\n        Unknown cmd/Incorrect args."
        print text + "\n" + self.helper()

    def helper(self):
        text = """\
        try $ python hamming.py <type> <command> <arg1> <arg2>

        Where <type> is one of:
            asc - ASCII encoding
            bin - Binary encoding
        Where <command> is one of:
            enc <infile> <outfile>
                Encode
            dec  <infile> <outfile>
                Decode
            chk <infile>
                Check
            fix <infile> <outfile>
                Fix
            err <pos> <infile> <outfile>
                Create an error at bit postion <pos>
        """
        return text


if __name__ == '__main__':
    usage = Usage()
    if len(sys.argv) <= 4:
        usage.feedback()
        sys.exit()
    if sys.argv[1] != 'asc' and sys.argv[1] != 'bin':
        usage.feedback()
        sys.exit()

    cmd = sys.argv[2]

    if cmd == 'enc' and len(sys.argv) == 5:
        encoder = HammingFile(sys.argv[1], sys.argv[3], sys.argv[4])
        encoder.encode_file()

    elif cmd == 'dec' and len(sys.argv) == 5:
        decoder = HammingFile(sys.argv[1], sys.argv[3], sys.argv[4])
        decoder.decode_file()

    elif cmd == 'chk' and len(sys.argv) == 4:
        checker = HammingFile(sys.argv[1], sys.argv[3], None)
        positions = checker.check_file()
        if positions:
            print("bad byte positions:")
            print positions
        else:
            print("All parity bits are valid.")

    elif cmd == 'fix' and len(sys.argv) == 5:
        fixer = HammingFile(sys.argv[1], sys.argv[3], sys.argv[4])
        fixer.fix_file()

    elif cmd == 'err' and len(sys.argv) == 6:
        errorer = HammingFile(sys.argv[1], sys.argv[4], sys.argv[5])
        errorer.error_file(sys.argv[3])

    else:

        print usage.feedback()


    #"01001101"
    #"010010011101"
