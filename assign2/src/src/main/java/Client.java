import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.FileWriter;


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
            if(args.length == 3){
                opnd = args[2];
                String delimiter = System.getProperty("file.separator");
                String opndKey = opnd;
                if(opnd.lastIndexOf(delimiter)!=-1){
                    opndKey = opnd.substring(opnd.lastIndexOf(delimiter)+1, opnd.length());
                }
                key = new Hash().hash(opndKey);
            }

            //node_ap = <IP address>
            //if ip == rmi -> ip address and the name of the remote object providing the service
            if(node_ap.contains(":")){
                String[] ss = node_ap.split(":");
                addr = ss[0];
                service_name = ss[1];
            }
            else addr = node_ap;

            Registry registry = LocateRegistry.getRegistry(addr);
            Functions node = (Functions) registry.lookup(service_name == null ? Constants.REG_FUNC_VAL : service_name);

            switch (operation) {
                case "put" -> {
                    //opnd here is the pathname of the file that contains the bytes to store
                    node.put(key, Files.readAllBytes(Paths.get(opnd)));
                    System.out.println("Put " + opnd + "!");
                }
                case "get" -> {
                    //opnd here is a key
                    str = node.get(key);
                    FileWriter output = new FileWriter("output.txt");
                    output.write(str);
                    output.close();
                    System.out.println("Saved file in output.txt!");
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
