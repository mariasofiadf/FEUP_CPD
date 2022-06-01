import java.rmi.registry.Registry;
import java.net.InetAddress;
import java.rmi.registry.LocateRegistry;
import java.io.File;
import java.util.Scanner;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Client {

    StorageNode node;
    String node_ap;
    String operation;
    String opnd;

    public Client(StorageNode node){
        this.node = node;
    }

    public void main(String[] args) {

        try {
            Registry registry = LocateRegistry.getRegistry();
            Functions functionsStub = (Functions) registry.lookup(Constants.REG_FUNC_VAL);
            String response = functionsStub.join();
            System.out.println("response: " + response);
        } catch (Exception e) {
            System.err.println("Client exception: " + e);
            e.printStackTrace();
        }

        try{
            node_ap = args[0];
            operation = args[1];

            //node_ap = <IP address>:<port number>
            //if ip == remi -> ip address and the name of the remote object providing the service
            //operation -> Key-value: put, get and delete or membership: join and leave
            //opnd -> put: pathname, get or delete: hexa string the key

            //rmi service is used
            //InetAddress ip = InetAddress.getLocalHost();
            //String name = ip.getHostName();
            
            String delimiter = ":";
            int end = node_ap.indexOf(delimiter);
            //find delimiter if end != -1
            if(end != -1){
                String ipAux = node_ap.substring(0, end);
                InetAddress ip = InetAddress.getByName(ipAux);
                
                String nameAux = node_ap.substring(end + 1, node_ap.length());
                
                if(nameAux != ip.getHostName()){
                    System.out.println("Host name for the given IP doesn't correspond to the IP's host name.");
                }
            }

            //criar new storage node

            switch(operation){
                case "put":
                    break;
                case "get":
                    break;
                case "delete":
                    break;
                case "join":
                    node.join();
                    break;
                case "leave":
                    node.leave();
                    break;
                default:
                    System.out.println("Invalid operation");
                    break;
            }

            if(operation != "join" && operation != "leave"){
                try{

                    var opnd = args[2];
                    String key = new Hash().hash(opnd);
                    String str;

                    switch(operation){

                        case "put":
                            //opnd here is the pathname of the file that contains the bytes to store
                            str = Files.readString(Paths.get(opnd));
                            node.put(key, str.getBytes());
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
                    }
                    if(operation == "put"){
                        
                    }
                    else if(operation == "get"){
                        
                    }
                    else{
                        System.out.println("Not a valid operand for the above operation.");
                    }
                }
                catch(Exception e){
                    System.out.println("You need to introduce more information for operations: put, delete and get.");
                }
            }
        }
        catch(Exception e){
            System.out.println("Incorrect arguments.");
        }

    }
}
