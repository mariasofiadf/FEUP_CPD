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
    int counter = -1;
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
    ArrayList<String> sentMembershipsTo = new ArrayList<>();
    Integer sentJoins = 0;
    Processor processor;
    FileController fileController;
    Sender sender;

    StorageNode(String IP_mcast_addr, String IP_mcast_port, String id) throws SocketException, UnknownHostException {
        this.ses = Executors.newScheduledThreadPool(Constants.MAX_THREADS);
        this.keyPathMap = new ConcurrentHashMap<>();
        this.membershipLog = new TreeMap<>();
        this.members = new ArrayList<>();
        this.memberInfo = new HashMap();
        this.membershipPort = Utils.getFreePort();
        this.IP_mcast_addr = IP_mcast_addr;
        this.IP_mcast_port = Integer.valueOf(IP_mcast_port);
        this.port = Utils.getFreePort();
        this.processor = new Processor(this);
        this.fileController = new FileController(this);
        this.sender = new Sender(this);
        Hash hash = new Hash();
        try {
            this.id = hash.hash(id);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        if(Constants.LOOPBACK){
            this.localAddress = id;
        }else{
            try(final DatagramSocket socket = new DatagramSocket()){
                socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
                this.localAddress = socket.getLocalAddress().getHostAddress();
//            this.localAddress = InetAddress.getByName("localhost").getHostAddress();
            }
        }
        fileController.loadFromDisk();
    }

    public static void main(String[] args) {
        if(args.length != 3)
        {
            System.out.println("Wrong number of arguments for Node startup");
            System.out.println("Usage:\njava -Djava.rmi.    .codebase=file:./ Store <IP_mcast_addr> <IP_mcast_port> <node_id>  <Store_port>");
            return;
        }

        try{

            StorageNode node = new StorageNode(args[0], args[1], args[2]);

            System.out.printf("""
                            Node initialized with: 
                            Identifier=%s
                            Multicast Address=%s
                            Multicast Port=%d
                            Address=%s
                            Membership Port=%d
                            Generic Unicast Port=%d
                            """,
                    node.id.substring(0,6), node.IP_mcast_addr, node.IP_mcast_port, node.localAddress, node.membershipPort, node.port);


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

    @Override
    public String join() throws RemoteException, InterruptedException, ExecutionException {
        if(inGroup) return "Already in cluster";
        inGroup = true;
        ScheduledFuture<String> future = ses.schedule(this::joinCall, 0, TimeUnit.SECONDS);
        while (!future.isDone()) TimeUnit.SECONDS.sleep(1);
        return future.get();
    }

    @Override
    public String leave() throws RemoteException, ExecutionException, InterruptedException {
        if(!inGroup) return "Already left";
        inGroup = false;
        ScheduledFuture<String> future = ses.schedule(this::leaveCall, 0, TimeUnit.SECONDS);
        while (!future.isDone()) TimeUnit.SECONDS.sleep(1);
        return future.get();
    }

    @Override
    public String get(String key) throws RemoteException, ExecutionException, InterruptedException {
        Future<String> future = ses.submit(()-> getCall(key));
        while(!future.isDone()) TimeUnit.SECONDS.sleep(1);
        return future.get();
    }

    @Override
    public String put(String key, byte[] bs) throws RemoteException, InterruptedException, ExecutionException {
        Future<String> future = ses.submit(()-> putCall(key, bs)
        );
        while(!future.isDone()) TimeUnit.MILLISECONDS.sleep(100);
        return future.get();
    }

    @Override
    public String delete(String key) throws RemoteException, ExecutionException, InterruptedException {
        Future<String> future = ses.submit(()-> deleteCall(key));
        while(!future.isDone()) TimeUnit.SECONDS.sleep(1);
        return future.get();
    }

    //Listens for mcast messages
    public String mcastListener() throws IOException {
        byte[] buf = new byte[1000];
        DatagramSocket socket = new DatagramSocket(null); // unbound
        socket.setReuseAddress(true); // set reuse address before binding
        socket.bind(new InetSocketAddress(IP_mcast_port)); // bind

        String addr = IP_mcast_addr;
        InetAddress mcastaddr = InetAddress.getByName(addr);
        InetSocketAddress group = new InetSocketAddress(mcastaddr, 0);
        NetworkInterface netIf = NetworkInterface.getByName(Constants.INTERFACE);
        socket.joinGroup(group, netIf);

        System.out.println("Started to listen for multicast messages");
        while (inGroup) {
            DatagramPacket packet = new DatagramPacket(buf, buf.length);
            socket.receive(packet);
            String msg = new String(packet.getData(), 0, packet.getLength());
            ses.schedule(() -> processor.process(msg, null),0,TimeUnit.SECONDS);
            if ("end".equals(msg)) break;
        }
        System.out.println("Stopped listening to multicast");
        socket.leaveGroup(group,netIf);
        socket.close();
        return "";
    }

    //Listens for membership messages after join
    public String membershipListener() throws IOException {
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.socket().bind(new InetSocketAddress(localAddress,membershipPort));
        System.out.println("Starting to listen for membership messages");
        while (receivedMembership < Constants.MIN_RECEIVED_MEMBERSHIP){
            SocketChannel sc = serverSocketChannel.accept();
            ses.submit(()->{
                String msg;
                ByteBuffer bb = ByteBuffer.allocate(1000);
                try {
                    sc.read(bb);
                    msg = new String(bb.array()).trim();
                    ses.submit(()->processor.process(msg, sc));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        System.out.println("Stopped listening for membership messages");
        return "";
    }


    //Listens for membership messages after join
    public String mainListener() throws IOException {
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.socket().bind(new InetSocketAddress(localAddress,port));
        System.out.println("Starting to listen for generic unicast messages");
        while (inGroup){
            SocketChannel sc = serverSocketChannel.accept();
            String msg;
            ByteBuffer bb = ByteBuffer.allocate(1000);
            try {
                sc.read(bb);
                msg = new String(bb.array()).trim();
                ses.submit(()->processor.process(msg, sc));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        System.out.println("Stopped listening for generic unicast messages");
        return "";
    }


    public String joinCall() throws InterruptedException {
        if(counter%2!=0)
            counter++;
        sentJoins = 0;
        receivedMembership = 0;

        ses.submit(this::mcastListener);
        ses.submit(this::membershipListener);
        ses.submit(this::mainListener);
        ses.submit(() -> addMembershipEntry(id,counter));
        ses.submit(() -> memberInfo.put(id, new MemberInfo(localAddress, membershipPort,port)));
        while (sentJoins < Constants.MAX_JOIN_TRIES && receivedMembership < Constants.MIN_RECEIVED_MEMBERSHIP){
            ses.submit(()->sender.sendJoin());
            TimeUnit.SECONDS.sleep(1);
        }
        if(sentJoins >= Constants.MAX_JOIN_TRIES && receivedMembership < Constants.MIN_RECEIVED_MEMBERSHIP)
            System.out.println("Sent 3 joins and didn't get 3 memberships back... Inside cluster");
        if(receivedMembership >= Constants.MIN_RECEIVED_MEMBERSHIP)
            System.out.println("Received 3 memberships back. Inside cluster!");

        ses.schedule(()->sender.sendLog(),1000+(new Random()).nextInt(0,100),TimeUnit.MILLISECONDS);

        ses.submit(()->fileController.saveCounterDisk());
        return "";
    }


    public String leaveCall(){
        System.out.println("Leaving cluster");

        ses.submit(() -> addMembershipEntry(id,++counter));
        ses.submit(() -> memberInfo.remove(id));
        ses.submit(()-> sender.sendLeave());
        return "Left cluster";
    }


    public String putCall(String key, byte[] bs){
        String nodeId = getResponsibleNode(key);
        ArrayList<String> replicators = getReplicatorNodes(key);
        if(nodeId.equals(id)){
            System.out.println("Inserting key " + key);
            keyPathMap.put(key,key);
            fileController.saveKeyVal(key, bs);
        }
        else {
            ses.submit(()->sender.sendPut(nodeId, key, bs));
            System.out.println("Not my key ("+key+")... redirecting it to " + nodeId.substring(0,6));
        }
        for(String replicator : replicators){
            if(!Objects.equals(replicator, nodeId))ses.submit(()-> sender.sendPut(replicator,key,bs));
        }
        return "Put " + key;
    }


    String getCall(String key){
        String nodeId = getResponsibleNode(key);
        var replicators = getReplicatorNodes(key);
        String value = "";
        if(nodeId.equals(id) || (keyPathMap.contains(key) && replicators.contains(id))){
            System.out.println("Getting key " + key);
            if(keyPathMap.get(key) != null){
                value = fileController.readKeyVal(key);
            }
        }
        if(value.equals("")) {
            try {
                do{
                    if(getResponsibleNode(key).equals(id))
                        System.out.println("I don't have this key ("+key.substring(0,6)+")... getting it from " + nodeId.substring(0,6));
                    else System.out.println("Not my key ("+key.substring(0,6)+")... getting it from " + nodeId.substring(0,6));
                    value = requestValue(nodeId, key);
                    nodeId = replicators.get(0);
                    replicators.remove(0);
                }while (value.equals("") && !replicators.isEmpty());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        if(value.equals("")) return "Key not in store";
        return value;
    }



    private String deleteCall(String key) {
        String nodeId = getResponsibleNode(key);
        if(nodeId.equals(id)){
            System.out.println("Deleting key " + key);
            keyPathMap.remove(key);
            fileController.delKeyVal(key);
        }
        else {
            ses.submit(()-> sender.sendDelete(nodeId, key));
            System.out.println("Not my key ("+key+")... deleting it from " + nodeId.substring(0,6));
        }
        return "Deleted " + key;
    }

    private String requestValue(String nodeId, String key) throws Exception {
        SocketChannel socketChannel = SocketChannel.open();
        InetSocketAddress address = new InetSocketAddress(memberInfo.get(nodeId).address, memberInfo.get(nodeId).port);
        socketChannel.configureBlocking(false);
        boolean connected =  socketChannel.connect(address);
        if(!connected) return "";

        Message message = new Message();
        Map<String, String> map = new HashMap<>();
        map.put(Constants.ACTION, Constants.GET);
        map.put(Constants.KEY, key);

        byte[] buf = message.assembleMsg(map).getBytes();

        socketChannel.write(ByteBuffer.wrap(buf));

        ByteBuffer bb = ByteBuffer.allocate(1000);
        Map<String, String> mapResp;
        socketChannel.configureBlocking(true);
        while (true){
            socketChannel.read(bb);
            String resp = new String(bb.array()).trim();
            mapResp = message.disassembleMsb(resp);
            if(mapResp.get(Constants.ACTION).equals(Constants.GET_RESP)) break;
            TimeUnit.MILLISECONDS.sleep(10);
        }
        socketChannel.close();
        return mapResp.get(Constants.BODY);
    }

    public String redistributeValues() {
        System.out.println("Redistributing values...");
        keyPathMap.forEach((k,v)->{
            if(!getResponsibleNode(k).equals(id)) {
                try {
                    sender.sendPut(getResponsibleNode(k),k,fileController.readKeyVal(k).getBytes());
                    fileController.delKeyVal(k);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        return "";
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
        members = new ArrayList<>(new HashSet<>(members));
        Collections.sort(members);
        fileController.saveMembersDisk();
        fileController.saveLogDisk();
    }

    public void showMembers(){
        System.out.println("\nMembers");
        for (String member : members) {
            System.out.println(member.substring(0,6));
        }
    }

    public void showMembershipLog(){
        System.out.println("\nMembership Log");
        membershipLog.forEach((k,v)-> System.out.println("Id: " + k.substring(0,6) + " | Counter: " + v));
    }

    public String getResponsibleNode(String key){
        return Utils.binarySearch(this.members,0, this.members.size(),key);
    }

    public ArrayList<String> getReplicatorNodes(String key){
        String keyOwner = getResponsibleNode(key);
        ArrayList replicators = new ArrayList();
        int i = members.indexOf(keyOwner);
        replicators.add(members.get(++i%members.size()));
        replicators.add(members.get(++i%members.size()));
        return replicators;
    }

    public void showKeys(){
        System.out.println("\nKeys");
        this.keyPathMap.forEach((k,v)-> System.out.println("key: "+ k + "\tpath: " + v));
    }


}
