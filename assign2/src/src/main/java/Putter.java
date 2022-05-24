import java.util.concurrent.Callable;

public class Putter implements Callable {

    StorageNode node;
    String key;
    byte[] value;
    public Putter(StorageNode node, String key, byte[] value) {
        this.key = key;
        this.value = value;
        this.node = node;
    }

    @Override
    public Object call() {
        String nodeId = node.getResponsibleNode(this.key);
        if(nodeId.equals(node.id)){
            System.out.println("[put] Inserting key " + key);
            node.keyPathMap.put(key,key);
        }
        else System.out.println("[put] Not my key ("+key+")... redirecting it to " + nodeId.substring(0,6));
        return null;
    }
}
