import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.concurrent.Callable;

public class UnicastReceiver implements Callable {

    Integer port;
    String address;

    public UnicastReceiver(String address, Integer port){
        this.address = address;
        this.port = port;
    }

    @Override
    public Object call() throws Exception {
        System.out.println("port: " + port);
        DatagramSocket server = new DatagramSocket(port, InetAddress.getLocalHost());
        System.out.println("[Unicast Receiver] Running on " + server.getLocalSocketAddress());
        DatagramPacket recvPacket = new DatagramPacket(new byte[1000], 1000);
        while (true)
        {
            server.receive(recvPacket);

            byte[] receiveMsg = Arrays.copyOfRange(recvPacket.getData(),
                    recvPacket.getOffset(),
                    recvPacket.getOffset() + recvPacket.getLength());

            System.out.println("Handing at client "
                    + recvPacket.getAddress().getHostName() + " ip "
                    + recvPacket.getAddress().getHostAddress());

            System.out.println("Server Receive Data:" + new String(receiveMsg));

            server.send(recvPacket);
        }
    }
}
