import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static java.lang.Integer.parseInt;
import static java.lang.Integer.valueOf;

public class MsgProcessor implements Callable {
    DatagramChannel dc = null;
    SocketChannel sc = null;
    StorageNode node;
    public MsgProcessor(StorageNode node, DatagramChannel dc){
        this.dc = dc;
        this.node = node;
    }
    public MsgProcessor(StorageNode node, SocketChannel sc){
        this.sc = sc;
        this.node = node;
    }

    public void processJoin(Map<String, String> map){
        node.memberInfo.put(map.get(Constants.ID), new MemberInfo(map.get(Constants.ADDRESS),valueOf(map.get(Constants.PORT))));
        node.addMembershipEntry(map.get(Constants.ID), parseInt(map.get(Constants.COUNTER)));
        if(map.get(Constants.ID).equals(node.id)) return;
        InetSocketAddress address = new InetSocketAddress(map.get(Constants.ADDRESS), Integer.parseInt(map.get(Constants.PORT)));
        node.qUcast.put(map.get(Constants.ID), new LinkedList<>());
        node.qUcast.get(map.get(Constants.ID)).add(new Task(Constants.MEMBERSHIP));
    }

    public void processLeave(Map<String, String> map){
        if(map.get(Constants.ID).equals(node.id)) return;
        node.memberInfo.remove(map.get(Constants.ID));
        node.addMembershipEntry(map.get(Constants.ID), parseInt(map.get(Constants.COUNTER)));
    }

    public void processLog(Map<String, String> map){
        map.forEach((k, v) -> {
            if(!k.equalsIgnoreCase(Constants.ACTION) && !k.equalsIgnoreCase(Constants.BODY) && !k.equals(node.id))
                node.addMembershipEntry(k,parseInt(v));
        });
    }

    public void processMembership(Map<String, String> map){
        map.forEach((k, v) -> {
            if(!k.equalsIgnoreCase(Constants.ACTION) && !k.equalsIgnoreCase(Constants.BODY) && !k.equals(node.id))
                if(!node.members.contains(k)) node.members.add(k);
        });
        if(node.receivedMembership > Constants.MIN_RECEIVED_MEMBERSHIP)
            node.qMcast.add(new Task(Constants.JOIN));
    }

    public void process(String msg){
        Message message = new Message();
        Map<String, String> map;
        try {
            map = message.disassembleMsb(msg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(map.get(Constants.ACTION) == null) return;
        System.out.println("[Receiver] Received " + map.get(Constants.ACTION));
        switch (map.get(Constants.ACTION)) {
            case Constants.JOIN -> processJoin(map);
            case Constants.LEAVE -> processLeave(map);
            case Constants.LOG -> processLog(map);
            case Constants.MEMBERSHIP -> processMembership(map);
            default -> {}
        }
    }

    @Override
    public Object call() throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(1000);
        String msg = "";
        if(dc != null){
            dc.receive(bb);
            bb.flip();
            byte[] data = new byte[bb.limit()];
            bb.get(data);
            msg = new String(data);
            bb.clear();
        }else if(sc != null){
            sc.read(bb);
            msg = new String(bb.array()).trim();
            sc.close();
        }

        process(msg);
        return null;
    }
}
