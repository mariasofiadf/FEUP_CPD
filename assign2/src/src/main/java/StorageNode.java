import java.io.IOException;
import java.net.*;
import java.rmi.Remote;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.*;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import static java.lang.Integer.parseInt;
import static java.lang.String.valueOf;

public class StorageNode implements Functions, Remote {
    int counter = 0;
    String id;
    boolean inGroup = false;
    String IP_mcast_addr;
    Integer IP_mcast_port;
    Integer port;
    ScheduledExecutorService ses;
    // <key, path>
    ConcurrentHashMap<String, String> keyPathMap;
    //hashed ids
    SortedMap<String, Integer> membershipLog;
    List<String> members;
    Queue<String> q = new LinkedList<>();
    StorageNode(ScheduledExecutorService ses, ConcurrentHashMap<String, String> keyPathMap,
                SortedMap<String, Integer> membershipLog, List<String> members,
                String IP_mcast_addr, String IP_mcast_port, String id, String port) {
        this.ses = ses;
        this.keyPathMap = keyPathMap;
        this.membershipLog = membershipLog; //TODO: Initialize log from disk
        this.members = members; //TODO Initialize members from disk
        this.IP_mcast_addr = IP_mcast_addr;
        this.IP_mcast_port = Integer.valueOf(IP_mcast_port);
        this.port = Integer.valueOf(port);
        Hash hash = new Hash();
        try {
            this.id = hash.hash(id);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        addMembershipEntry(this.id, counter);
    }

    private static boolean available(int port) {
        try (Socket ignored = new Socket("localhost", port)) {
            return false;
        } catch (IOException ignored) {
            return true;
        }
    }
    public static void main(String[] args) {
        if(args.length != 4)
        {
            System.out.println("Wrong number of arguments for Node startup");
            System.out.println("Usage:\njava -Djava.rmi.    .codebase=file:./ Store <IP_mcast_addr> <IP_mcast_port> <node_id>  <Store_port>");
            return;
        }

        try{
            
            StorageNode node = new StorageNode(Executors.newScheduledThreadPool(Constants.MAX_THREADS),
            new ConcurrentHashMap<>(), new TreeMap<>(), new ArrayList<>(), args[0], args[1], args[2], args[3]);

            Selector selector = Selector.open();
            InetAddress group = InetAddress.getByName(node.IP_mcast_addr);
            final InetSocketAddress address = new InetSocketAddress(group, node.IP_mcast_port);

            final NetworkInterface ni;
            try (MulticastSocket temp = new MulticastSocket()) {
                ni = temp.getNetworkInterface();
            }
            DatagramChannel datagramChannel = DatagramChannel.open(StandardProtocolFamily.INET)
                .setOption(StandardSocketOptions.SO_REUSEADDR, true)
                .bind(new InetSocketAddress(node.IP_mcast_port))
                .setOption(StandardSocketOptions.IP_MULTICAST_IF, ni);

            datagramChannel.configureBlocking(false);
            datagramChannel.join(group, ni);

            int ops = datagramChannel.validOps();  
            SelectionKey selectKy = datagramChannel.register(selector, ops);
            
            System.out.printf("[Main] Node initialized with IP_mcast_addr=%s IP_mcast_port=%d node_id=%s Store_port=%d%n",
                    node.IP_mcast_addr, node.IP_mcast_port, node.id.substring(0,6), node.port);

            Functions functionsStub = (Functions) UnicastRemoteObject.exportObject(node,0);
            Registry registry = LocateRegistry.getRegistry();
            //Unbind previous remote object's stub in the registry
            registry.rebind(Constants.REG_FUNC_VAL, functionsStub);
            boolean stop = false;
            node.ses.schedule(new DebugHelper(node),0,TimeUnit.SECONDS); //TODO: Remove this when done (This is for debug only)

            for (;;) {  
                int noOfKeys = selector.select();  
                Set selectedKeys = selector.selectedKeys();  
                Iterator itr = selectedKeys.iterator();
                while (itr.hasNext()) {
                    SelectionKey ky = (SelectionKey) itr.next();
                    if (ky.isReadable()) {
                        node.ses.schedule(new MsgProcessor(node, (DatagramChannel) ky.channel()),0,TimeUnit.SECONDS);
                    }
                    else if (ky.isWritable()) {
                        if(node.q.isEmpty())
                            continue;
                        node.ses.schedule(new UDPMulticastSender(node, node.q.remove(), (DatagramChannel) ky.channel(), address), 0, TimeUnit.SECONDS);
                    }
                    itr.remove();
                    TimeUnit.SECONDS.sleep(1);
                } // end of while loop  
                if(stop)
                    break;
            } // end of for loop  

        }catch(Exception e){
            System.err.println("\n[Main] Server exception: " + e);
            e.printStackTrace();
        }
    }

    @Override
    public String join() throws RemoteException, InterruptedException, ExecutionException {
        if(inGroup) return "Already joined";
        inGroup = true;
        q.add(Constants.MEMBERSHIP);
        q.add(Constants.JOIN);
        return "";
    }


    @Override
    public String leave() throws RemoteException, ExecutionException, InterruptedException {
        if(!inGroup) return "Already left";
        inGroup = false;
        q.add(Constants.LEAVE);
        return "";
    }

    @Override
    public String put(String key, byte[] bs) throws RemoteException {
        ScheduledFuture scheduledFuture = ses.schedule(new Putter(this, key, bs),0, TimeUnit.SECONDS);
        return "put not implemented yet";
    }

    @Override
    public String get(int key) throws RemoteException, ExecutionException, InterruptedException {
        return "get not implemented yet";
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
        Collections.sort(members);
    }

    public void showMembers(){
        System.out.println("Members");
        for (String member : members) {
            System.out.println("[Main] " + member.substring(0,6));
        }
    }

    public void showMembershipLog(){
        System.out.println("Membership Log");
        membershipLog.forEach((k,v)-> System.out.println("[Main] Id: " + k.substring(0,6) + " | Counter: " + v));
    }

    String binarySearch(List<String> arr, int l, int r, String x)
    {
        if(arr.size() == 1) return arr.get(0);
        if (r >= l) {
            int mid = l + (r - l) / 2;
            System.out.println("mid" + mid);

            if(mid >= arr.size())
                return arr.get(0);
            if(mid <= 0)
                return arr.get(1);
            // If the element is present at the
            // middle itself
            if (arr.get(mid).compareTo(x) > 0  && arr.get(mid-1).compareTo(x) < 0){
                return arr.get(mid);
            }
            // If element is smaller than mid, then
            // it can only be present in left subarray
            if (arr.get(mid).compareTo(x) > 0)
                return binarySearch(arr, l, mid - 1, x);

            // Else the element can only be present
            // in right subarray
            return binarySearch(arr, mid + 1, r, x);
        }

        // We reach here when element is not present
        // in array
        return "";
    }

    public String getResponsibleNode(String key){
        return binarySearch(this.members,0, this.members.size(),key);

    }

    public void showKeys(){
        this.keyPathMap.forEach((k,v)-> System.out.println("key: "+ k + "\tpath: " + v));
    }
}
