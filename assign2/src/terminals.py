import os
import sys
from time import sleep

n = 4
if(len(sys.argv)>1):
    n = sys.argv[1]

for i in range(n):
    os.system('gnome-terminal -- bash -c "cd build/classes/java/main && java -Djava.rmi.server.codebase=file:./ StorageNode; exec bash"')
    sleep(0.5)