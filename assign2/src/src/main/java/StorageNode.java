import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
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
    int receivedMembership = 0;
    String localAddress;
    Integer membershipPort;
    String IP_mcast_addr;
    Integer IP_mcast_port;
    Integer port;
    ScheduledExecutorService ses;
    // <key, path>
    ConcurrentHashMap<String, String> keyPathMap;
    //hashed ids
    SortedMap<String, Integer> membershipLog;
    List<String> members;
    Map<String, MemberInfo> memberInfo;
    Queue<Task> qMcast = new LinkedList<>();

    Map<String, Queue<Task>> qUcast = new HashMap<>();
    Integer sentJoins = 0;
    StorageNode(String IP_mcast_addr, String IP_mcast_port, String id, String port) throws SocketException, UnknownHostException {
        this.ses = Executors.newScheduledThreadPool(Constants.MAX_THREADS);
        this.keyPathMap = new ConcurrentHashMap<>();
        this.membershipLog = new TreeMap<>();
        this.members = new ArrayList<>();
        this.memberInfo = new HashMap();
        this.membershipPort = getFreePort();
        this.IP_mcast_addr = IP_mcast_addr;
        this.IP_mcast_port = Integer.valueOf(IP_mcast_port);
        this.port = Integer.valueOf(port);
        Hash hash = new Hash();
        try {
            this.id = hash.hash(id);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        try(final DatagramSocket socket = new DatagramSocket()){
            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
            this.localAddress = socket.getLocalAddress().getHostAddress();
//            this.localAddress = InetAddress.getByName("localhost").getHostAddress();
        }
        this.loadFromDisk();
    }

    private static boolean available(int port) {
        try (Socket ignored = new Socket("localhost", port)) {
            return false;
        } catch (IOException ignored) {
            return true;
        }
    }
    private static int getFreePort(){
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            int port = serverSocket.getLocalPort();
            serverSocket.close();
            return port;
        } catch (IOException e) {
            return -1;
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
            
            StorageNode node = new StorageNode(args[0], args[1], args[2], args[3]);

            System.out.printf("[Main] Node initialized with IP_mcast_addr=%s IP_mcast_port=%d node_id=%s Store_port=%d%n",
                    node.IP_mcast_addr, node.IP_mcast_port, node.id.substring(0,6), node.port);


            Functions functionsStub = (Functions) UnicastRemoteObject.exportObject(node,0);
            Registry registry = LocateRegistry.getRegistry();
            registry.rebind(Constants.REG_FUNC_VAL, functionsStub);
            boolean stop = false;
            if(Constants.DEBUG)
                node.ses.schedule(new DebugHelper(node),0,TimeUnit.SECONDS);

        }catch(Exception e){
            System.err.println("\n[Main] Server exception: " + e);
            e.printStackTrace();
        }
    }

    //Listens for mcast messages
    Callable mcastListener = () -> {
        byte[] buf = new byte[256];
        MulticastSocket socket = new MulticastSocket(IP_mcast_port);
        InetAddress group = InetAddress.getByName(IP_mcast_addr);
        socket.joinGroup(group);
        System.out.println("Started listening to mcast");
        while (true) {
            DatagramPacket packet = new DatagramPacket(buf, buf.length);
            socket.receive(packet);
            String msg = new String(packet.getData(), 0, packet.getLength());
            ses.submit(() -> {process(msg);});
            if ("end".equals(msg)) break;
        }
        System.out.println("closed");
        socket.leaveGroup(group);
        socket.close();
        return null;
    };

    Callable process(String msg){
        Message message = new Message();
        Map<String, String> map;
        try {
            map = message.disassembleMsb(msg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(map.get(Constants.ACTION) == null) return null;
        System.out.println("Received " + map.get(Constants.ACTION));
        switch (map.get(Constants.ACTION)) {
            case Constants.JOIN -> processJoin(map);
            case Constants.LEAVE -> processLeave(map);
            case Constants.LOG -> processLog(map);
            case Constants.MEMBERSHIP -> processMembership(map);
            default -> {}
        }
        return null;
    }

    private void processLog(Map<String, String> map) {
        map.forEach((k, v) -> {
            if(!k.equalsIgnoreCase(Constants.ACTION) && !k.equalsIgnoreCase(Constants.BODY) && !k.equals(id))
                addMembershipEntry(k,parseInt(v));
        });
    }

    private void processMembership(Map<String, String> map) {
        map.forEach((k, v) -> {
            if(!k.equalsIgnoreCase(Constants.ACTION) && !k.equalsIgnoreCase(Constants.BODY) && !k.equals(id))
                if(!members.contains(k)) {
                    System.out.println("Added member: " + k.substring(0,6));
                    members.add(k);
                }
        });
        receivedMembership++;
    }

    Callable processLeave(Map<String, String> map) {
        if(map.get(Constants.ID).equals(id)) return null;
        ses.submit(()->memberInfo.remove(map.get(Constants.ID)));
        ses.submit(()->addMembershipEntry(map.get(Constants.ID), parseInt(map.get(Constants.COUNTER))));
        return null;
    }


    Callable processJoin(Map<String, String> map) {
        if(map.get(Constants.ID).equals(id)) return null;
        if(members.contains(map.get(Constants.ID))) return null;
        ses.submit(() -> memberInfo.put(map.get(Constants.ID), new MemberInfo(map.get(Constants.ADDRESS), Integer.valueOf(map.get(Constants.PORT)))));
        ses.submit(() -> addMembershipEntry(map.get(Constants.ID), parseInt(map.get(Constants.COUNTER))));
        //Address to send membership to
        InetSocketAddress address = new InetSocketAddress(map.get(Constants.ADDRESS), Integer.parseInt(map.get(Constants.PORT)));
        try {
            ses.submit(sendMembership(address));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    Callable<String> sendMembership (InetSocketAddress address) throws IOException {
        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.connect(address);

        Message message = new Message();
        Map<String, String> map = new HashMap<>();
        map.put("action", Constants.MEMBERSHIP);
        for(String key : members){
            MemberInfo memberInfo = this.memberInfo.get(key);
            if(memberInfo != null)  map.put(key, memberInfo.address + ":" + memberInfo.port);
        }

        byte[] buf = message.assembleMsg(map).getBytes();
        socketChannel.write(ByteBuffer.wrap(buf));
        socketChannel.close();
        if (Constants.DEBUG) System.out.println("Sent Membership to " + address.toString());
        return null;
    };
    //Listens for membership messages after join
    Callable<String> membershipListener = () -> {
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.socket().bind(new InetSocketAddress(localAddress,membershipPort));
        if(Constants.DEBUG) System.out.println("Starting to listen for membership messages on " + serverSocketChannel.getLocalAddress());
        while (receivedMembership < Constants.MIN_RECEIVED_MEMBERSHIP){
            SocketChannel sc = serverSocketChannel.accept();
            ses.submit(()->{
                String msg;
                ByteBuffer bb = ByteBuffer.allocate(1000);
                try {
                    sc.read(bb);
                    msg = new String(bb.array()).trim();
                    sc.close();
                    ses.submit(()->process(msg));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        return "Task's execution";
    };

    Callable<String> sendLog() throws IOException {
        DatagramSocket socket = new DatagramSocket();
        InetAddress group = InetAddress.getByName(IP_mcast_addr);

        Message message = new Message();
        Map<String, String> map = new HashMap<>();
        map.put("action", Constants.LOG);
        int i = 0;
        for(String key : membershipLog.keySet()){
            map.put(key, membershipLog.get(key).toString());
            i++;
            if(i >= Constants.MAX_LOG) break;
        }
        String msg = message.assembleMsg(map);
        byte[] buf = message.assembleMsg(map).getBytes();
        DatagramPacket packet = new DatagramPacket(buf, buf.length, group, IP_mcast_port);
        socket.send(packet);
        socket.close();

        if (Constants.DEBUG) System.out.println("Sent Log");
        if(inGroup) ses.schedule(()->sendLog(), 1, TimeUnit.SECONDS);
        else if (Constants.DEBUG) System.out.println("Stopped sending log msg");
        return null;
    };

    Callable<String> sendJoin = () -> {
        DatagramSocket socket = new DatagramSocket();
        InetAddress group = InetAddress.getByName(IP_mcast_addr);

        Message message = new Message();
        Map<String, String> map = new HashMap<>();
        map.put("action", Constants.JOIN);
        map.put("id", id);
        map.put("counter", valueOf(counter));
        map.put("address", localAddress);
        map.put("port", valueOf(membershipPort));

        byte[] buf = message.assembleMsg(map).getBytes();
        DatagramPacket packet = new DatagramPacket(buf, buf.length, group, IP_mcast_port);
        socket.send(packet);
        socket.close();
        sentJoins++;
        if (Constants.DEBUG) System.out.println("Sent Join");
        return "Sent Join";
    };

    Callable<String> join = () -> {
        ses.schedule(mcastListener, 0 ,TimeUnit.SECONDS);
        ses.schedule(membershipListener,0,TimeUnit.SECONDS);
        while (sentJoins < Constants.MAX_JOIN_TRIES && receivedMembership < Constants.MIN_RECEIVED_MEMBERSHIP){
            ses.submit(sendJoin);
            TimeUnit.SECONDS.sleep(1);
        }
        if(sentJoins >= Constants.MAX_JOIN_TRIES)
            System.out.println("Sent 3 joins and didn't get 3 memberships back... Inside cluster");
        if(receivedMembership >= Constants.MIN_RECEIVED_MEMBERSHIP)
            System.out.println("Received 3 memberships back. Inside cluster!");
        ses.submit(() -> addMembershipEntry(id,counter));
        ses.submit(() -> memberInfo.put(id, new MemberInfo(localAddress, port)));
        ses.schedule(() -> sendLog(),1,TimeUnit.SECONDS);
        return "Joined cluster";
    };

    @Override
    public String join() throws RemoteException, InterruptedException, ExecutionException {
        if(inGroup) return "Already in cluster";
        inGroup = true;
        ScheduledFuture<String> future = ses.schedule(join, 0, TimeUnit.SECONDS);
        while (!future.isDone()) TimeUnit.SECONDS.sleep(1);
        return future.get();
    }


    @Override
    public String leave() throws RemoteException, ExecutionException, InterruptedException {
        if(!inGroup) return "Already left";
        inGroup = false;
        //TODO finished leave
        qMcast.add(new Task(Constants.LEAVE));
        return "";
    }


    public void saveKeyVal(String key, byte[] bs){
        String path = id + File.separator + Constants.STORE_FOLDER + File.separator + key;
        File file = new File(path);
        try (FileOutputStream fos = new FileOutputStream(path);
            ObjectOutputStream oos = new ObjectOutputStream(fos)) {
            oos.writeObject(bs);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    
    Callable<String> putCall(String key, byte[] bs){
        String nodeId = getResponsibleNode(key);
        if(nodeId.equals(id)){
            System.out.println("[put] Inserting key " + key);
            keyPathMap.put(key,key);
            saveKeyVal(key, bs);
            
        }
        else System.out.println("[put] Not my key ("+key+")... redirecting it to " + nodeId.substring(0,6));
        return null;
    };

    @Override
    public String put(String key, byte[] bs) throws RemoteException, InterruptedException, ExecutionException {
        Future<Callable<String>> future = ses.submit(()->putCall(key, bs));
        while(!future.isDone()) TimeUnit.SECONDS.sleep(1);
        return future.get().toString();
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
            System.out.println("Updated log entry: " + id.substring(0,6) + " " + counter);
        }
        else if (!membershipLog.containsKey(id)){
            membershipLog.put(id, counter);
            System.out.println("Added log entry: " + id.substring(0,6) + " " + counter);
        }
        if(!members.contains(id) && counter % 2 == 0){
            members.add(id);
            System.out.println("Added member: " + id.substring(0,6));
        }
        if(members.contains(id) && counter % 2 != 0){
            members.remove(id);
            System.out.println("Removed member: " + id.substring(0,6));
        }
        Collections.sort(members);
        saveMembersDisk();
        saveLogDisk();
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

    public void loadFromDisk(){
        String directoryName = this.id;
        File directory = new File(directoryName);
        if (!directory.exists()){
            directory.mkdir();
        }
        File members = new File((directoryName + File.separator + Constants.MEMBERS_FILENAME));
        if(members.exists())
            loadMembersDisk();
        File log = new File((directoryName + File.separator + Constants.LOG_FILENAME));
        if(log.exists())
            loadLogDisk();

        File store = new File(directoryName + File.separator + Constants.STORE_FOLDER);
        if(!store.exists()){
            store.mkdir();
        }
        //TODO load store from disk
    }

    public void saveMembersDisk(){
        try (FileOutputStream fos = new FileOutputStream(this.id + File.separator+ Constants.MEMBERS_FILENAME);
             ObjectOutputStream oos = new ObjectOutputStream(fos)) {
            oos.writeObject(this.members);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public void loadMembersDisk(){
        try (FileInputStream fis = new FileInputStream(this.id + File.separator+ Constants.MEMBERS_FILENAME);
             ObjectInputStream ois = new ObjectInputStream(fis)) {
            this.members = (List<String>) ois.readObject();
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public void saveLogDisk(){
        try (FileOutputStream fos = new FileOutputStream(this.id + File.separator+ Constants.LOG_FILENAME);
             ObjectOutputStream oos = new ObjectOutputStream(fos)) {
            oos.writeObject(this.membershipLog);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public void loadLogDisk(){
        try (FileInputStream fis = new FileInputStream(this.id + File.separator+ Constants.LOG_FILENAME);
             ObjectInputStream ois = new ObjectInputStream(fis)) {
            this.membershipLog = (SortedMap<String, Integer>) ois.readObject();
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

}
