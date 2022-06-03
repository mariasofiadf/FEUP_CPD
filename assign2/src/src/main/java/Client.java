import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

public class Client {
    
    public static void main(String[] args) {
        
        String node_ap;
        String operation;
        String opnd = null;
        String addr;
        String service_name = null;
        //StorageNode node;

        try{
            node_ap = args[0];
            operation = args[1];
            String key ="";
            String str;
            int port = 0;
            if(args.length == 3){
                opnd = args[2];
                key = new Hash().hash(opnd);
            }

            //node_ap = <IP address>
            //if ip == rmi -> ip address and the name of the remote object providing the service
            if(node_ap.contains(":")){
                String[] ss = node_ap.split(":");
                addr = ss[0];
                port = Integer.parseInt(ss[1]);
            }
            else addr = node_ap;

            Registry registry = LocateRegistry.getRegistry(addr);
            NodeInterface node = (NodeInterface) registry.lookup(addr);

            switch (operation) {
                case "put" -> {
                    //opnd here is the pathname of the file that contains the bytes to store
                    node.put(key, Files.readAllBytes(Paths.get(opnd)));
                    System.out.println("Put " + opnd + "!");
                }
                case "get" -> {
                    //opnd here is a key
                    str = node.get(key);
                    System.out.println("File " + opnd + ": \n" + str);
                }
                case "delete" -> {
                    str = node.delete(key);
                    System.out.println("Deleted " + opnd + "!");
                }
                case "join" -> {
                    node.join();
                    System.out.println("Joined cluster!");
                }
                case "leave" -> {
                    node.leave();
                    System.out.println("Left cluster!");
                }
                default -> System.out.println("Invalid operation");
            }
  
        }
        catch(Exception e){
            System.out.println("Error: " + e);
        }

    }
}
