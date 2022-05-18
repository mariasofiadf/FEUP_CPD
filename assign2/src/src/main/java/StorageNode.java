import java.rmi.Remote;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;

public class StorageNode implements Functions, Remote {

    StorageNode(){}
    static ScheduledExecutorService scheduledExecutorService;

    // <key, path>
    static ConcurrentHashMap<Integer, String> keyPathMap = new ConcurrentHashMap<>();

    //hashed ids
    static List<Integer> nodeIds = new ArrayList<>();
    

    public static void main(String[] args) {
        try{
            scheduledExecutorService =
                    Executors.newScheduledThreadPool(5);
            scheduledExecutorService.schedule(new UDPMulticastClient(), 0, TimeUnit.SECONDS);
            StorageNode obj = new StorageNode();
            Functions functionsStub = (Functions) UnicastRemoteObject.exportObject(obj,0);

            Registry registry = LocateRegistry.getRegistry();

            //Unbind previous remote object's stub in the registry
            registry.rebind(Constants.REG_FUNC_VAL, functionsStub);
            System.out.println("Storage Node ready");
            Scanner sc=new Scanner(System.in);  
   
        }catch(Exception e){
            System.err.println("Server exception: " + e);
            e.printStackTrace();
        }
    }

    @Override
    public String join() throws RemoteException, InterruptedException, ExecutionException {

        /*
        1. Every time a node joins or leaves the cluster, the node must send via IP multicast a JOIN/LEAVE message, respectively.
        2. Messages:
            - membership counter (initially 0) - increases everytime the node join or leaves the cluster
                if even the node is joining the cluster, if odd the node is leaving
                The membership counter must survive node crashes and therefore should be stored in non-volatile memory.
        3. Add events (join/leave) to the membership log, this must contain:
            - node id
            - membership counter
        4. Whenever a node JOINS initialize the cluster membership aka some nodes will send a Membership message, which includes:
            - list of current cluster members
            - the 32 most recent membership events (put, join, leave, etc) in its log.
            Note: Use TCP to pass membership information to nodes joining the cluster
        5. Since a new member starts accepting connections on a PORT before sending JOIN messages, it sends the port number on these messages.
        6. X time after receiving the join message, a cluster member send the membership information.
            Note: The new member stops accepting connections after receiving 3 MEMBERSHIP messages.
        7.


         */
        ScheduledFuture scheduledFuture = scheduledExecutorService.schedule(new UDPMulticastServer("JOIN"),0, TimeUnit.SECONDS);
        return scheduledFuture.get().toString();
    }


    @Override
    public String leave() throws RemoteException {
        return "leave not implemented yet";
    }


    @Override
    public String put(int key, byte[] value) throws RemoteException {

        return "put not implemented yet";
    }

    @Override
    public String get(int key) throws RemoteException, ExecutionException, InterruptedException {
        //[0, 10, 15, 30] nodeIds
        //key = 12
        // {12:"/12"}
        ScheduledFuture scheduledFuture = scheduledExecutorService.schedule(new Getter(key),0, TimeUnit.SECONDS);
        return scheduledFuture.get().toString();
    }

    @Override
    public String delete(int key) throws RemoteException {
        return "delete not implemented yet";
    }
}
