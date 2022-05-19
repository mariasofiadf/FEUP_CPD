import java.rmi.Remote;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.*;

import static java.lang.Integer.parseInt;
import static java.lang.String.valueOf;

public class StorageNode implements Functions, Remote {
    int counter = 0;
    String id;
    StorageNode(ScheduledExecutorService ses, ConcurrentHashMap<Integer, String> keyPathMap,SortedMap<String, Integer> membershipLog){
        this.ses = ses;
        this.keyPathMap = keyPathMap;
        this.membershipLog = membershipLog;
        Long timestamp = System.currentTimeMillis();
        Hash hash = new Hash();
        try {
            this.id = hash.hash(timestamp.toString());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        addMembershipEntry(id, counter);
    }
    ScheduledExecutorService ses;

    // <key, path>
    ConcurrentHashMap<Integer, String> keyPathMap;

    //hashed ids
    SortedMap<String, Integer> membershipLog;

    public static void main(String[] args) {
        try{
            System.out.println("\n[Main] Node ready");
            StorageNode node = new StorageNode(Executors.newScheduledThreadPool(Constants.MAX_THREADS),
            new ConcurrentHashMap<>(), new TreeMap<>());

            node.ses.schedule(new UDPMulticastReceiver(node), 0, TimeUnit.SECONDS);
            node.ses.schedule(new UDPMulticastSender(node, Constants.MEMBERSHIP), 5, TimeUnit.SECONDS);
            Functions functionsStub = (Functions) UnicastRemoteObject.exportObject(node,0);

            Registry registry = LocateRegistry.getRegistry();

            //Unbind previous remote object's stub in the registry
            registry.rebind(Constants.REG_FUNC_VAL, functionsStub);

            node.join();

        }catch(Exception e){
            System.err.println("\n[Main] Server exception: " + e);
            e.printStackTrace();
        }
    }

    @Override
    public String join() throws RemoteException, InterruptedException, ExecutionException {
        //UDPMulticastSender knows how to assemble join msg
        ScheduledFuture scheduledFuture = ses.schedule(new UDPMulticastSender(this, Constants.JOIN),0, TimeUnit.SECONDS);
        return scheduledFuture.get().toString();
    }


    @Override
    public String leave() throws RemoteException, ExecutionException, InterruptedException {
        //UDPMulticastSender knows how to assemble leave msg
        ScheduledFuture scheduledFuture = ses.schedule(new UDPMulticastSender(this, Constants.LEAVE),0, TimeUnit.SECONDS);
        return scheduledFuture.get().toString();
    }


    @Override
    public String put(int key, byte[] value) throws RemoteException {
        return "put not implemented yet";
    }

    @Override
    public String get(int key) throws RemoteException, ExecutionException, InterruptedException {
        ScheduledFuture scheduledFuture = ses.schedule(new Getter(key),0, TimeUnit.SECONDS);
        return scheduledFuture.get().toString();
    }

    @Override
    public String delete(int key) throws RemoteException {
        return "delete not implemented yet";
    }

    public void addMembershipEntry(String id, Integer counter){
        if(membershipLog.containsKey(id))
            membershipLog.replace(id, counter);
        else
            membershipLog.put(id, counter);
        System.out.println("\n[Msg Processor] Added membership entry. Updated log (" + membershipLog.size() + " members):");
        membershipLog.forEach((k,v)-> System.out.println("[MsgProcessor] Id: " + k.substring(0,6) + " | Counter: " + v));
    }

    public void membership(){

    }
}
