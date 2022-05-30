import java.util.Scanner;
import java.util.concurrent.Callable;

public class DebugHelper implements Callable {
    StorageNode node;
    public DebugHelper(StorageNode node) {
        this.node = node;
    }

    @Override
    public Object call() throws Exception {
        //For debug purposes:
        Scanner scanner = new Scanner(System.in);
        char cmd; boolean stop = false;
        while(!stop){
            cmd = scanner.next().charAt(0);
            switch (Character.toLowerCase(cmd)){
                case 'q': stop = true;
                case 'j': node.join(); break;
                case 'l': node.leave(); break;
                case 'm': node.showMembers(); break;
                case 'h': node.showMembershipLog(); break;
                case 'k': node.showKeys(); break;
                case 'p': node.put("5xafas", "Hello World!".getBytes()); break;
                case 'g': node.get("5xafas"); break;
                default: System.out.println("Invalid key");
            }
        }
        System.out.println("Debug helper stopped");
        return null;
    }
}
