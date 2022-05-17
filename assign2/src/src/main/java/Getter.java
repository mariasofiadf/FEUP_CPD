import java.util.concurrent.Callable;

public class Getter implements Callable {

    int key;
    Getter(int key){this.key = key;}

    @Override
    public Object call() {
        
        System.out.println("Running getter");
        return "get not implemented yet";
    }
}
