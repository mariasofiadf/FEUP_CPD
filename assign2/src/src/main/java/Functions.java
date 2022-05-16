import java.rmi.Remote;
import java.rmi.RemoteException;

public interface Functions extends Remote {

        String join() throws RemoteException;
        String leave() throws RemoteException;
        String put(int key, byte[] value) throws RemoteException;
        String get(int key) throws RemoteException;
        String delete(int key) throws RemoteException;
}
