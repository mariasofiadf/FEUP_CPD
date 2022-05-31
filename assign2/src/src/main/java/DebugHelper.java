import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.Callable;

public class DebugHelper implements Callable {
    StorageNode node;
    String text = "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been" +
            " the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type" +
            " and scrambled it to make a type specimen book. It has survived not only five centuries, but also the" +
            " leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s" +
            " with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop" +
            " publishing software like Aldus PageMaker including versions of Lorem Ipsum";
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
                case 'p':
                    for (String word : words) {
                        String key = new Hash().hash(word);
                        node.put(key,word.getBytes());
                    } break;
                case 'g':
                    for (String word : words) {
                        String key = new Hash().hash(word);
                        System.out.println("Got value: " + node.get(key));
                    }break;
                default: System.out.println("Invalid key");
            }
        }
        System.out.println("Debug helper stopped");
        return null;
    }
}
