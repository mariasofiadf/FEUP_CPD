import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.concurrent.ExecutionException;

public interface Functions extends Remote {

        String join() throws RemoteException, InterruptedException, ExecutionException;
        String leave() throws RemoteException, ExecutionException, InterruptedException;
        String put(String key, byte[] value) throws RemoteException;

        String get(int key) throws RemoteException, ExecutionException, InterruptedException;
        String delete(int key) throws RemoteException;
}
