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
Store_port = 5000
node_id = 0
for i in range(int(n)):
    node_id += 1 #int(time())
    cmd = f'gnome-terminal -- bash -c "cd build/classes/java/main && java -Djava.rmi.server.codebase=file:./ StorageNode {IP_mcast_addr} {IP_mcast_port} {node_id} {Store_port}; exec bash"'
    print(cmd)
    os.system(cmd)
    Store_port += 1
    sleep(0.5)