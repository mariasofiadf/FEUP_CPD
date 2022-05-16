import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class StorageNode implements Functions, KeyValue {

    StorageNode(){}

    public static void main(String[] args) {
        try{
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
    public String get(int key) throws RemoteException {
        return "get not implemented yet";
    }

    @Override
    public String delete(int key) throws RemoteException {
        return "delete not implemented yet";
    }
}
