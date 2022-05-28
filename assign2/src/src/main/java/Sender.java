import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import static java.lang.String.valueOf;

public class Sender implements Callable {

    String msg;
    StorageNode node;
    DatagramChannel dc;
    InetSocketAddress address;
    public Sender(StorageNode node, String msg, DatagramChannel dc, InetSocketAddress address) {
            this.msg = msg;
            this.node = node;
            this.dc = dc;
            this.address = address;
    }

    public Sender(StorageNode node, String msg, InetSocketAddress address) {
        this.node = node;
        this.msg = msg;
        this.address = address;
        this.dc = null;
    }

    public void sendMcast() throws IOException {
        String content = "";
        switch (msg){
            case Constants.JOIN -> content = join();
            case Constants.LEAVE -> content = leave();
            case Constants.LOG -> content = log();
        }
        ByteBuffer bb = ByteBuffer.wrap(content.getBytes());
        dc.send(bb, address);
        bb.flip();
        System.out.println("[Sender] Sent " + msg + " on multicast");
    }

    public void sendUcast() throws IOException {
        String content = "";
        System.out.println("[Sender] Connecting on unicast to " + address);
        SocketChannel socketChannel = SocketChannel.open(address);
        switch (msg){
            case Constants.MEMBERSHIP -> content = membership();
        }
        ByteBuffer buffer = ByteBuffer.wrap(content.getBytes());
        socketChannel.write(buffer);
        buffer.clear();
        System.out.println("[Sender] Sent " + msg + "on unicast to " + address);


        socketChannel.close();
    }

    @Override
    public Object call() throws Exception {
        if(dc != null) sendMcast();
        else sendUcast();
        return "";
    }

    private String membership(){
        Message message = new Message();
        Map<String, String> map = new HashMap<>();
        map.put("action", Constants.MEMBERSHIP);
//        int i = 0;
//        for(String key : node.membershipLog.keySet()){
//            map.put(key, node.membershipLog.get(key).toString());
//            i++;
//            if(i >= Constants.MAX_LOG) break;
//        }

        return message.assembleMsg(map);
    }

    private String log() {
        Message message = new Message();
        Map<String, String> map = new HashMap<>();
        map.put("action", Constants.LOG);
        int i = 0;
        for(String key : node.membershipLog.keySet()){
            map.put(key, node.membershipLog.get(key).toString());
            i++;
            if(i >= Constants.MAX_LOG) break;
        }

        String msg = message.assembleMsg(map);
        if(node.inGroup) node.qMcast.add(new Task(Constants.LOG,Constants.MEMBERSHIP_INTERVAL));
        else System.out.println("[Sender] Stopped sending membership msg");
        return msg;
    }

    //Assembles join msg
    public String join() {
        Message message = new Message();
        Map<String, String> map = new HashMap<>();
        map.put("action", Constants.JOIN);
        map.put("id", node.id);
        map.put("counter", valueOf(node.counter));
        map.put("address", node.localAddress);
        map.put("port", valueOf(node.membershipPort));
        String msg = message.assembleMsg(map);
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
        node.counter ++;
        return msg;
    }


}