import os
import sys
print("What is the address of the node to connect to?")
addr = input()
print("Welcome to the key-value store!")

base_cmd = "java Client " + addr

while(True):
    print("Type command and press enter (for help type 'h'):")
    inp = input()
    argv = inp.split();
    curr_cmd = base_cmd
    for arg in argv:
        curr_cmd += " " + arg
    os.system(curr_cmd)

def usage():
    print("\n\nCommands:")
    print("change (to change node address)")
    print("join")
    print("leave")
    print("put <filename>")
    print("get <filename>")
    print("delete <filename>")





