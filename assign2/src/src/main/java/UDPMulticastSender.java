import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static java.lang.String.valueOf;

public class UDPMulticastSender implements Callable {

    String msg;
    StorageNode node;
    DatagramChannel dc;
    InetSocketAddress address;
    public UDPMulticastSender(StorageNode node, String msg, DatagramChannel dc, InetSocketAddress address) {
            this.msg = msg;
            this.node = node;
            this.dc = dc;
            this.address = address;
    }
    public static void sendUDPMessage(String message, String ipAddress, int port) throws IOException {
        InetAddress group = InetAddress.getByName(ipAddress);
        DatagramSocket socket = new DatagramSocket();
        byte[] msg = message.getBytes();
        DatagramPacket packet = new DatagramPacket(msg, msg.length,group,port);
        ByteBuffer buffer = ByteBuffer.wrap(msg);
        socket.send(packet);
        socket.close();
    }

    @Override
    public Object call() throws Exception {
        String content = "";
        switch (msg){
            case Constants.JOIN -> content = join();
            case Constants.LEAVE -> content = leave();
            case Constants.MEMBERSHIP -> content = membership();
        }
        ByteBuffer bb = ByteBuffer.wrap(content.getBytes());
        dc.send(bb, address);
        bb.flip();
        return "";
    }

    private String membership() {
        Message message = new Message();
        Map<String, String> map = new HashMap<>();
        map.put("action", Constants.MEMBERSHIP);
        int i = 0;
        for( String key : node.membershipLog.keySet()){
            map.put(key, node.membershipLog.get(key).toString());
            i++;
            if(i >= Constants.MAX_LOG) break;
        }

        String msg = message.assembleMsg(map);
        System.out.println("[Mcast Sender] Sending membership msg to " + node.IP_mcast_addr + ":" + node.IP_mcast_port);
        if(node.inGroup) node.q.add(new Task(Constants.MEMBERSHIP,Constants.MEMBERSHIP_INTERVAL));
        else System.out.println("[Mcast Sender] Stopped sending membership msg");
        return msg;
    }

    //Assembles join msg
    public String join() {
        Message message = new Message();
        Map<String, String> map = new HashMap<>();
        map.put("action", Constants.JOIN);
        map.put("id", node.id);
        map.put("counter", valueOf(node.counter));
        String msg = message.assembleMsg(map);
        System.out.println("[Mcast Sender] Sending join msg to "  + node.IP_mcast_addr + ":" + node.IP_mcast_port);
        node.counter ++;
        return msg;
    }

    //TODO: maybe refactor this since join() and leave() are very similar
    //Assembles leave msg
    public String leave() {
        Message message = new Message();
        Map<String, String> map = new HashMap<>();
        map.put("action", Constants.LEAVE);
        map.put("id", node.id);
        map.put("counter", valueOf(node.counter));
        String msg = message.assembleMsg(map);
        System.out.println("[Mcast Sender] Sending leave msg to "  + node.IP_mcast_addr + ":" + node.IP_mcast_port);
        node.counter ++;
        return msg;
    }


}