import java.rmi.Remote;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.*;

public class StorageNode implements Functions, Remote {

    StorageNode(){}
    static ScheduledExecutorService scheduledExecutorService;

    public static void main(String[] args) {
        try{
            scheduledExecutorService =
                    Executors.newScheduledThreadPool(5);
            StorageNode obj = new StorageNode();
            Functions functionsStub = (Functions) UnicastRemoteObject.exportObject(obj,0);

            Registry registry = LocateRegistry.getRegistry();

            //Unbind previous remote object's stub in the registry
            registry.rebind(Constants.REG_FUNC_VAL, functionsStub);
            System.out.println("Storage Node ready");
        }catch(Exception e){
            System.err.println("Server exception: " + e);
            e.printStackTrace();
        }
    }

    @Override
    public String join() throws RemoteException {
        return "leave not implemented yet";
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
        ScheduledFuture scheduledFuture = scheduledExecutorService.schedule(new Getter(),0, TimeUnit.SECONDS);
        return scheduledFuture.get().toString();
    }

    @Override
    public String delete(int key) throws RemoteException {
        return "delete not implemented yet";
    }
}
