import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Callable;

public class DebugHelper implements Callable {
    final StorageNode node;
    final String text = "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been" +
            " the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type" +
            " and scrambled it to make a type specimen book.";
    List<String> words = Arrays.stream(text.split(" ")).toList();
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
                case 'd':
                    node.delete("5xsffa");break;
//                    for (String word : words) {
//                        String key = new Hash().hash(word);
//                        node.delete(key);
//                    }break;
                case 'p':
                    node.put("5xsffa", "Hello World!".getBytes());break;
//                    for (String word : words) {
//                        String key = new Hash().hash(word);
//                        node.put(key,word.getBytes());
//                    } break;
                case 'g':
                    System.out.println("Get " + "5xsffa->" + node.get("5xsffa"));break;
//                    for (String word : words) {
//                        String key = new Hash().hash(word);
//                        System.out.println("Got value: " + node.get(key));
//                    }break;
                default: System.out.println("Invalid key");
            }
        }
        System.out.println("Debug helper stopped");
        return null;
    }
}
