import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static java.lang.String.valueOf;

public class Sender {

    StorageNode node;

    public Sender(StorageNode node) {
        this.node = node;
    }


    public String sendJoin() throws IOException {
        DatagramSocket socket = new DatagramSocket(new InetSocketAddress(0));
        NetworkInterface outgoingIf = NetworkInterface.getByName(Constants.INTERFACE);
        socket.setOption(StandardSocketOptions.IP_MULTICAST_IF, outgoingIf);

        InetAddress mcastaddr = InetAddress.getByName(node.IP_mcast_addr);
        InetSocketAddress dest = new InetSocketAddress(mcastaddr, node.IP_mcast_port);
        Message message = new Message();
        Map<String, String> map = new HashMap<>();
        map.put(Constants.ACTION, Constants.JOIN);
        map.put(Constants.ID, node.id);
        map.put(Constants.COUNTER, valueOf(node.counter));
        map.put(Constants.ADDRESS, node.localAddress);
        map.put(Constants.MEMBERSHIP_PORT, valueOf(node.membershipPort));
        map.put(Constants.PORT, valueOf(node.port));

        byte[] buf = message.assembleMsg(map).getBytes();
        DatagramPacket packet = new DatagramPacket(buf, buf.length, dest);
        socket.send(packet);
        socket.close();
        node.sentJoins++;
        if (Constants.DEBUG) System.out.println("Sent Join");
        return "";
    }

    public String sendLeave() throws IOException {
        DatagramSocket socket = new DatagramSocket();
        InetAddress group = InetAddress.getByName(node.IP_mcast_addr);

        Message message = new Message();
        Map<String, String> map = new HashMap<>();
        map.put(Constants.ACTION, Constants.LEAVE);
        map.put(Constants.ID, node.id);
        map.put(Constants.COUNTER, valueOf(node.counter));

        byte[] buf = message.assembleMsg(map).getBytes();
        DatagramPacket packet = new DatagramPacket(buf, buf.length, group, node.IP_mcast_port);
        socket.send(packet);
        socket.close();
        node.members.remove(node.id);
        node.ses.schedule(()->node.redistributeValues(),1,TimeUnit.SECONDS);
        if (Constants.DEBUG) System.out.println("Sent Leave");
        return "Sent Leave";
    }


    public String sendLog() throws IOException {
        DatagramSocket socket = new DatagramSocket(new InetSocketAddress(0));
        NetworkInterface outgoingIf = NetworkInterface.getByName(Constants.INTERFACE);
        socket.setOption(StandardSocketOptions.IP_MULTICAST_IF, outgoingIf);

        InetAddress mcastaddr = InetAddress.getByName(node.IP_mcast_addr);
        InetSocketAddress dest = new InetSocketAddress(mcastaddr, node.IP_mcast_port);

        Message message = new Message();
        Map<String, String> map = new HashMap<>();
        map.put(Constants.ACTION, Constants.LOG);
        int i = 0;
        for(String key : node.membershipLog.keySet()){
            map.put(key, node.membershipLog.get(key).toString());
            i++;
            if(i >= Constants.MAX_LOG) break;
        }

        byte[] buf = message.assembleMsg(map).getBytes();
        DatagramPacket packet = new DatagramPacket(buf, buf.length, dest);
        socket.send(packet);
        socket.close();
        if(Constants.DEBUG) System.out.println("Sent Log");
        if(node.inGroup) node.ses.schedule(this::sendLog,Constants.LOG_INTERVAL*1000 + Constants.LOG_INTERVAL*1000*(1/node.membershipLog.size()), TimeUnit.MILLISECONDS);
        else if (Constants.DEBUG) System.out.println("Stopped sending log msg");
        return "";
    }


    public String sendPut(String nodeId, String key, byte[] bs) throws IOException {
        SocketChannel socketChannel = SocketChannel.open();
        InetSocketAddress address = new InetSocketAddress(node.memberInfo.get(nodeId).address, node.memberInfo.get(nodeId).port);
        socketChannel.connect(address);
        Message message = new Message();
        Map<String, String> map = new HashMap<>();
        map.put(Constants.ACTION, Constants.PUT);
        map.put(Constants.KEY, key);
        map.put(Constants.BODY, new String(bs));
        byte[] buf = message.assembleMsg(map).getBytes();
        socketChannel.write(ByteBuffer.wrap(buf));
        socketChannel.close();
        if(Constants.DEBUG) System.out.println("Sent put to " + nodeId);
        return "";
    }

    public String sendDelete(String nodeId, String key) throws IOException {
        SocketChannel socketChannel = SocketChannel.open();
        InetSocketAddress address = new InetSocketAddress(node.memberInfo.get(nodeId).address, node.memberInfo.get(nodeId).port);
        socketChannel.connect(address);
        Message message = new Message();
        Map<String, String> map = new HashMap<>();
        map.put(Constants.ACTION, Constants.DELETE);
        map.put(Constants.KEY, key);
        byte[] buf = message.assembleMsg(map).getBytes();
        socketChannel.write(ByteBuffer.wrap(buf));
        socketChannel.close();
        return "";
    }


    public String sendMembership (InetSocketAddress address) throws IOException, InterruptedException {
        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.connect(address);

        Message message = new Message();
        Map<String, String> map = new HashMap<>();
        map.put(Constants.ACTION, Constants.MEMBERSHIP);
        for(String key : node.members){
            MemberInfo memberInfo = node.memberInfo.get(key);
            if(memberInfo != null)  map.put(key, memberInfo.address + ":" + memberInfo.membershipPort+ ":" + memberInfo.port);
        }

        byte[] buf = message.assembleMsg(map).getBytes();
        TimeUnit.MILLISECONDS.sleep((new Random()).nextInt(0,100));
        socketChannel.write(ByteBuffer.wrap(buf));
        socketChannel.close();
        return "";
    }

}

