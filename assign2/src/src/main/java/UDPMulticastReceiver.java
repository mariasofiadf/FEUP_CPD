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
        MulticastSocket socket = new MulticastSocket(4321);
        InetAddress group = InetAddress.getByName("230.0.0.0");
        socket.joinGroup(group);
        System.out.println("[Mcast Receiver] Listening for multicast messages...");
        while (true) {
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            socket.receive(packet);
            String msg = new String(packet.getData(), packet.getOffset(), packet.getLength());

            node.ses.schedule(new MsgProcessor(node, msg), 0, TimeUnit.SECONDS); // Schedule
            if ("OK".equals(msg)) { //TODO: Remove this when shutdown is implemented
                System.out.println("No more message. Exiting : " + msg);
                break;
            }
        }
        socket.leaveGroup(group);
        socket.close();
    }

    @Override
    public Object call() {
        try {
            receiveUDPMessage("230.0.0.0", 4321);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return null;
    }

}