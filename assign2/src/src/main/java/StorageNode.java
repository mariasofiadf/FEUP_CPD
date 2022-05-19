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

    boolean inGroup = false;

    UDPMulticastReceiver mcastReceiver;
    StorageNode(ScheduledExecutorService ses, ConcurrentHashMap<Integer, String> keyPathMap,SortedMap<String, Integer> membershipLog, List<String> members){
        this.ses = ses;
        this.keyPathMap = keyPathMap;
        this.membershipLog = membershipLog;
        this.members = members;
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

    List<String> members;

    public static void main(String[] args) {
        try{
            System.out.println("\n[Main] Node ready");
            StorageNode node = new StorageNode(Executors.newScheduledThreadPool(Constants.MAX_THREADS),
            new ConcurrentHashMap<>(), new TreeMap<>(), new ArrayList<>());

            Functions functionsStub = (Functions) UnicastRemoteObject.exportObject(node,0);

            Registry registry = LocateRegistry.getRegistry();

            //Unbind previous remote object's stub in the registry
            registry.rebind(Constants.REG_FUNC_VAL, functionsStub);

            //For debug purposes:
            Scanner scanner = new Scanner(System.in);
            char cmd; boolean stop = false;
            while(!stop){
                cmd = scanner.next().charAt(0);
                switch (Character.toLowerCase(cmd)){
                    case 'q': stop = true;
                    case 'j': node.join(); break;
                    case 'l': node.leave(); break;
                    case 'm': node.showMembers(); break;
                    case 'g': node.showMembershipLog(); break;
                    default: System.out.println("Invalid key");
                }
            }

        }catch(Exception e){
            System.err.println("\n[Main] Server exception: " + e);
            e.printStackTrace();
        }
    }

    @Override
    public String join() throws RemoteException, InterruptedException, ExecutionException {
        inGroup = true;
        //Start listening to mcast group
        mcastReceiver = new UDPMulticastReceiver(this);
        this.ses.schedule(mcastReceiver, 0, TimeUnit.SECONDS);

        //Start periodic membership messages
        this.ses.schedule(new UDPMulticastSender(this, Constants.MEMBERSHIP), 5, TimeUnit.SECONDS);

        //UDPMulticastSender knows how to assemble join msg
        ScheduledFuture scheduledFuture = ses.schedule(new UDPMulticastSender(this, Constants.JOIN),0, TimeUnit.SECONDS);
        return scheduledFuture.get().toString();
    }


    @Override
    public String leave() throws RemoteException, ExecutionException, InterruptedException {
        inGroup = false;
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
        if(membershipLog.containsKey(id) && membershipLog.get(id) < counter){
            membershipLog.replace(id, counter);
            System.out.println("[Msg Processor] Updated log entry: " + id.substring(0,6) + " " + counter);
        }
        else if (!membershipLog.containsKey(id)){
            membershipLog.put(id, counter);
            System.out.println("[Msg Processor] Added log entry: " + id.substring(0,6) + " " + counter);
        }
        if(!members.contains(id) && counter % 2 == 0){
            members.add(id);
            System.out.println("[Msg Processor] Added member: " + id.substring(0,6));
        }
        if(members.contains(id) && counter % 2 != 0){
            members.remove(id);
            System.out.println("[Msg Processor] Removed member: " + id.substring(0,6));
        }

    }

    public void showMembers(){
        System.out.println("Members");
        for (String member : members) {
            System.out.println("[Main] " + member);
        }

    }

    public void showMembershipLog(){
        System.out.println("Membership Log");
        membershipLog.forEach((k,v)-> System.out.println("[Main] Id: " + k.substring(0,6) + " | Counter: " + v));
    }
}
