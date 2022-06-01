import java.rmi.registry.Registry;
import java.net.InetAddress;
import java.rmi.registry.LocateRegistry;
import java.io.File;
import java.util.Scanner;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Client {

    
    public static void main(String[] args) {
        
        String node_ap;
        String operation;
        String opnd = null;
        String addr = null;
        String service_name = null;
        //StorageNode node;

        try{
            node_ap = args[0];
            operation = args[1];
            String key ="";
            String str;
            if(args.length == 3){
                opnd = args[2];
                key = new Hash().hash(opnd);
            }

            //node_ap = <IP address>
            //if ip == rmi -> ip address and the name of the remote object providing the service
            if(node_ap.contains(":")){
                String ss[] = node_ap.split(":");
                addr = ss[0];
                service_name = ss[1];
            }
            else addr = node_ap;

            Registry registry = LocateRegistry.getRegistry(addr);
            Functions node = (Functions) registry.lookup(service_name == null ? Constants.REG_FUNC_VAL : service_name);

            switch(operation){

                case "put":
                    //opnd here is the pathname of the file that contains the bytes to store
                    node.put(key, Files.readAllBytes(Paths.get(opnd)));
                    System.out.println("Put " + opnd + "!");
                    break;
                
                case "get":
                    //opnd here is a key
                    str = node.get(key);
                    System.out.println("File " + opnd + ": \n" + str);
                    break;

                case "delete":
                    str = node.delete(key);
                    System.out.println("Deleted " + opnd + "!");
                    break;

                case "join":
                    node.join();
                    System.out.println("Joined cluster!");
                    break;
                    
                case "leave":
                    node.leave();
                    System.out.println("Left cluster!");
                    break;

                default:                
                    System.out.println("Invalid operation");
                    break;
            }
  
        }
        catch(Exception e){
            System.out.println("Error: " + e);
        }

    }
}
