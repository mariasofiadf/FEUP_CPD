import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectableChannel;
import java.util.Map;
import java.util.concurrent.Callable;

import static java.lang.Integer.parseInt;

public class MsgProcessor implements Callable {
    DatagramChannel dc;
    SelectableChannel sc;
    StorageNode node;
    public MsgProcessor(StorageNode node, DatagramChannel dc){
        this.dc = dc;
        this.node = node;
    }
    public MsgProcessor(StorageNode node, SelectableChannel sc){
        this.sc = sc;
        this.node = node;
    }

    public void process(String msg){
        Message message = new Message();
        Map<String, String> map;
        try {
            map = message.disassembleMsb(msg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println("[Msg Processor] Received " + map.get(Constants.ACTION));
        switch (map.get(Constants.ACTION)) {
            case Constants.JOIN, Constants.LEAVE -> {
                if(map.get(Constants.ID).equals(node.id)) return;
                node.addMembershipEntry(map.get(Constants.ID), parseInt(map.get(Constants.COUNTER)));
            }
            case Constants.MEMBERSHIP -> map.forEach((k, v) -> {
                if(!k.equalsIgnoreCase(Constants.ACTION) && !k.equalsIgnoreCase(Constants.BODY) && !k.equals(node.id)) node.addMembershipEntry(k,parseInt(v));
            });
            default -> {
            }
        }
    }

    @Override
    public Object call() throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(1000);
        dc.receive(bb);
        bb.flip();
        byte[] data = new byte[bb.limit()];
        bb.get(data);
        String msg = new String(data);
        bb.clear();
        process(msg);
        return null;
    }
}
