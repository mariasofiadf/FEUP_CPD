import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.concurrent.Callable;

public class UDPMulticastSender implements Callable {

        String msg;
    public UDPMulticastSender(String msg) {
            this.msg = msg;
    }
    public static void sendUDPMessage(String message,
                                      String ipAddress, int port) throws IOException {
        DatagramSocket socket = new DatagramSocket();
        InetAddress group = InetAddress.getByName(ipAddress);
        byte[] msg = message.getBytes();
        DatagramPacket packet = new DatagramPacket(msg, msg.length,
                group, port);
        socket.send(packet);
        socket.close();
    }

    @Override
    public Object call() throws Exception {
        sendUDPMessage(msg, "230.0.0.0", 4321);
        return msg + " executed";
    }
}