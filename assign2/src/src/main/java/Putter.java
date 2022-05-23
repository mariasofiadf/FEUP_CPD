import java.util.concurrent.Callable;

public class Putter implements Callable {

    StorageNode node;
    String key;
    String value;
    public Putter(StorageNode node, String key, String value) {
        this.key = key;
        this.value = value;
        this.node = node;
    }

    @Override
    public Object call() throws Exception {
        return null;
    }
}
