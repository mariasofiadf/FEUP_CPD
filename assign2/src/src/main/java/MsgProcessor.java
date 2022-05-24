import java.util.Map;
import java.util.concurrent.Callable;

import static java.lang.Integer.parseInt;

public class MsgProcessor implements Callable {
    String msg;
    StorageNode node;
    public MsgProcessor(StorageNode node, String msg){
        this.msg = msg;
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
    public Object call() {
        process(msg);
        return null;
    }
}