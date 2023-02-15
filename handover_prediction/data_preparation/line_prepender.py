import os

def line_prepender(filename, line):
    with open(filename, 'r+') as f:
        content = f.read()
        f.seek(0, 0)
        f.write(line.rstrip('\r\n') + '\n' + content)

def line_adder(filename, line):
    file_obj = open(filename, 'a')
    file_obj.write('</packets>')
    file_obj.close()

def prepender(filename):
    line_prepender(filename, "<packets>")
    line_adder(filename, "</packets>")

def main():
    input_folder = "RRC_Output"
    subfolders = os.listdir(input_folder)
    for folder in subfolders:
        path = "RRC_Output/" + folder
        level2_f = os.listdir(path) # level 2 folders
        for f2 in level2_f:
            folder_path = path + "/" + f2
            level3_f = os.listdir(folder_path)
            for f3 in level3_f:
                filepath = folder_path + "/" + f3
                prepender(filepath)

if __name__ == "__main__":
    main()
