import java.util.concurrent.Callable;

public class Getter implements Callable {

    @Override
    public Object call() {
        System.out.println("Running getter");
        return "get not implemented yet";
    }
}
