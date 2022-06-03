import os
import sys
from time import sleep, time
os.system('gradle build')
os.system('cd build/classes/java/main/ && rmiregistry &')

n = 4
if(len(sys.argv)>1):
    n = sys.argv[1]
IP_mcast_addr = "230.0.0.0"
IP_mcast_port = "4321"
addrs = []
for i in range(int(n)):
    addrs.append("127.0.0." + str(i+5))

for i in range(int(n)):
    cmd = f'gnome-terminal -- bash -c "cd build/classes/java/main && java -Djava.rmi.server.codebase=file:./ StorageNode {IP_mcast_addr} {IP_mcast_port} {addrs[i]}; exec bash"'
    os.system(cmd)
    sleep(0.5)