import sys

def line_prepender(filename, line):
    with open(filename, 'r+') as f:
        content = f.read()
        f.seek(0, 0)
        f.write(line.rstrip('\r\n') + '\n' + content)

def line_adder(filename, line):
    file_obj = open(filename, 'a')
    file_obj.write('</packets>')
    file_obj.close()

def main():
    filename = str(sys.argv[1])
    # line_prepender(filename, "<?xml version="1.0" ?>")
    line_prepender(filename, "<packets>")
    line_adder(filename, "</packets>")


if __name__ == "__main__":
    main()
