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
    static ConcurrentHashMap<Integer, String> keyPathMap = new ConcurrentHashMap<Integer, String>();

    //hashed ids
    static List<Integer> nodeIds = new ArrayList<Integer>();
    

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
