import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.lang.Integer.parseInt;

public class Processor {

    StorageNode node;

    public Processor(StorageNode node) {
        this.node = node;
    }

    void process(String msg, SocketChannel sc){
        Message message = new Message();
        Map<String, String> map;
        try {
            map = message.disassembleMsb(msg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(map.get(Constants.ACTION) == null) return;
        System.out.println("Received " + map.get(Constants.ACTION));
        switch (map.get(Constants.ACTION)) {
            case Constants.JOIN -> processJoin(map);
            case Constants.LEAVE -> processLeave(map);
            case Constants.LOG -> processLog(map);
            case Constants.MEMBERSHIP -> processMembership(map);
            case Constants.PUT -> processPut(map);
            case Constants.DELETE -> processDelete(map);
            case Constants.GET -> {
                try {
                    processGet(map,sc);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            default -> {}
        }
        try {
            sc.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }



    private void processDelete(Map<String, String> map) {
        String key = map.get(Constants.KEY);
        System.out.println("Deleting key: " + key);
        node.keyPathMap.remove(key);
        node.fileController.delKeyVal(key);
    }

    private void processGet(Map<String, String> map, SocketChannel sc) throws IOException {
        Message message = new Message();
        String key = map.get(Constants.KEY);
        String value = "";
        if(node.keyPathMap.get(key) != null){
            value = node.fileController.readKeyVal(key);
        }
        Map<String, String> mapResp = new HashMap<>();
        mapResp.put(Constants.ACTION,Constants.GET_RESP);
        mapResp.put(Constants.BODY, value);
        byte[] buf = message.assembleMsg(mapResp).getBytes();
        sc.write(ByteBuffer.wrap(buf));
    }

    private void processPut(Map<String, String> map) {
        String key = map.get(Constants.KEY); byte[] val = map.get(Constants.BODY).getBytes();
        System.out.println("Saving key: " + key + " value: " + map.get(Constants.BODY));
        node.keyPathMap.put(key,key);
        node.fileController.saveKeyVal(key, val);
    }

    private void processLog(Map<String, String> map) {
        map.forEach((k, v) -> {
            if(!k.equalsIgnoreCase(Constants.ACTION) && !k.equalsIgnoreCase(Constants.BODY) && !k.equals(node.id))
                node.addMembershipEntry(k,parseInt(v));
        });
    }

    private void processMembership(Map<String, String> map) {
        node.receivedMembership++;
        map.forEach((k, v) -> {
            if(!k.equalsIgnoreCase(Constants.ACTION) && !k.equalsIgnoreCase(Constants.BODY)){
                if(!node.members.contains(k)) {
                    System.out.println("Added member: " + k.substring(0,6));
                    node.members.add(k);
                }
                if(!node.memberInfo.containsKey(k)){
                    String[] parts = v.split(":");
                    node.memberInfo.put(k, new MemberInfo(parts[0],Integer.parseInt(parts[1]),Integer.parseInt(parts[2])));
                }
            }
        });
    }

    void processLeave(Map<String, String> map) {
        if(map.get(Constants.ID).equals(node.id)) return;
        node.ses.submit(()->node.memberInfo.remove(map.get(Constants.ID)));
        node.ses.submit(()->node.addMembershipEntry(map.get(Constants.ID), parseInt(map.get(Constants.COUNTER))));
    }


    void processJoin(Map<String, String> map) {
        if(map.get(Constants.ID).equals(node.id)) return;
        var id2 = map.get(Constants.ID);
        if(node.sentMembershipsTo.contains(id2)) return;
        node.sentMembershipsTo.add(map.get(Constants.ID));
        node.ses.schedule(()->node.sentMembershipsTo.remove(id2),15, TimeUnit.SECONDS);
        System.out.println(map.get(Constants.ID).substring(0,6) + " joined the cluster");

        node.ses.submit(() -> node.memberInfo.put(map.get(Constants.ID), new MemberInfo(map.get(Constants.ADDRESS),
                Integer.valueOf(map.get(Constants.MEMBERSHIP_PORT)),Integer.valueOf(map.get(Constants.PORT)))));

        node.ses.submit(() -> node.addMembershipEntry(map.get(Constants.ID), parseInt(map.get(Constants.COUNTER))));
        InetSocketAddress address = new InetSocketAddress(map.get(Constants.ADDRESS), Integer.parseInt(map.get(Constants.MEMBERSHIP_PORT)));


        node.ses.submit(()-> {
            try {
                node.sender.sendMembership(address);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        node.ses.schedule(()->node.redistributeValues(),1,TimeUnit.SECONDS);
    }



}
