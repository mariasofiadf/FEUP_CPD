import java.rmi.registry.Registry;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.rmi.registry.LocateRegistry;


public class Client {
    public static void main(String[] args) {

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
            var node_ap = args[0];
            String operation = args[1];

            //node_ap = <IP address>:<port number>
            //if ip == remi -> ip address and the name of the remote object providing the service
            //operation -> Key-value: put, get and delete or membership: join and leave
            //opnd -> put: pathname, get or delete: hexa string the key

            //rmi service is used
            InetAddress ip = InetAddress.getLocalHost();
            String name = ip.getHostName();

            String delimiter = ":";
            int end = node_ap.indexOf(delimiter);
            //find delimiter if end != -1
            if(end != -1){
                //InetAddress ip = node_ap.substring(0, end);
            }

            switch(operation){
                case "put":
                    break;
                case "get":
                    break;
                case "delete":
                    break;
                case "join":
                    break;
                case "leave":
                    break;
                default:
                    System.out.println("Invalid operation");
                    break;
            }

            if(operation != "join" && operation != "leave"){
                try{
                    var opnd = args[2];
                    if(operation == "put"){
                        String pathname = opnd;
                    }
                    else if(operation == "get" || operation == "delete"){
                        var key = opnd;
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
