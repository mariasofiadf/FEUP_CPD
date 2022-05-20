import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static java.lang.String.valueOf;

public class UDPMulticastSender implements Callable {

    String msg;
    StorageNode node;
    public UDPMulticastSender(StorageNode node, String msg) {
            this.msg = msg;
            this.node = node;
    }
    public static void sendUDPMessage(String message, String ipAddress, int port) throws IOException {
        DatagramSocket socket = new DatagramSocket();
        InetAddress group = InetAddress.getByName(ipAddress);
        byte[] msg = message.getBytes();
        DatagramPacket packet = new DatagramPacket(msg, msg.length, group, port);
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
        sendUDPMessage(content, node.IP_mcast_addr, node.IP_mcast_port);
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
        if(node.inGroup)
            node.ses.schedule(new UDPMulticastSender(node, Constants.MEMBERSHIP), 5, TimeUnit.SECONDS);
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