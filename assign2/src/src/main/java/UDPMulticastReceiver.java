import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static java.lang.Integer.parseInt;

public class UDPMulticastReceiver implements Callable {

    StorageNode node;
    public UDPMulticastReceiver(StorageNode node) {
        this.node = node;
    }

    public void receiveUDPMessage(String ip, int port) throws IOException {
        byte[] buffer = new byte[1024];
        MulticastSocket socket = new MulticastSocket(port);
        InetAddress group = InetAddress.getByName(ip);
        socket.joinGroup(group);
        System.out.println("[Mcast Receiver] Listening for multicast messages... on " + ip + ":" + port);
        while (node.inGroup) {
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            socket.receive(packet);
            String msg = new String(packet.getData(), packet.getOffset(), packet.getLength());

            //node.ses.schedule(new MsgProcessor(node, msg), 0, TimeUnit.SECONDS); // Schedule
            if ("OK".equals(msg)) { //TODO: Remove this when shutdown is implemented
                System.out.println("No more message. Exiting : " + msg);
                break;
            }
        }
        System.out.println("[Mcast Receiver] Stopped listening for multicast messages.");
        socket.leaveGroup(group);
        socket.close();
    }

    @Override
    public Object call() {
        try {
            receiveUDPMessage(node.IP_mcast_addr, node.IP_mcast_port);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return null;
    }

}