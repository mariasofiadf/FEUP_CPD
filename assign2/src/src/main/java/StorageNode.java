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
    ScheduledExecutorService ses = null;
    // <key, path>
    ConcurrentHashMap<String, String> keyPathMap;
    //hashed ids
    SortedMap<String, Integer> membershipLog;
    List<String> members;
    Map<String, MemberInfo> memberInfo;
    ArrayList<String> sentMembershipsTo = new ArrayList<>();
    Integer sentJoins = 0;
    StorageNode(String IP_mcast_addr, String IP_mcast_port, String id) throws SocketException, UnknownHostException {
        this.ses = Executors.newScheduledThreadPool(Constants.MAX_THREADS);
        this.keyPathMap = new ConcurrentHashMap<>();
        this.membershipLog = new TreeMap<>();
        this.members = new ArrayList<>();
        this.memberInfo = new HashMap();
        this.membershipPort = getFreePort();
        this.IP_mcast_addr = IP_mcast_addr;
        this.IP_mcast_port = Integer.valueOf(IP_mcast_port);
        this.port = getFreePort();
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
        if(args.length != 3)
        {
            System.out.println("Wrong number of arguments for Node startup");
            System.out.println("Usage:\njava -Djava.rmi.    .codebase=file:./ Store <IP_mcast_addr> <IP_mcast_port> <node_id>  <Store_port>");
            return;
        }

        try{

            StorageNode node = new StorageNode(args[0], args[1], args[2]);

            System.out.printf("[Main] Node initialized with: Multicast Address=%s\n" +
                            "Multicast Port=%d\n" +
                            "Address=%s\n" +
                            "Identifier=%s",
                    node.IP_mcast_addr, node.IP_mcast_port, node.localAddress, node.id);


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
    final Callable mcastListener = () -> {
        byte[] buf = new byte[1000];
        DatagramSocket socket = new DatagramSocket(null); // unbound
        socket.setReuseAddress(true); // set reuse address before binding
        socket.bind(new InetSocketAddress(IP_mcast_port)); // bind

        String addr = IP_mcast_addr;
        InetAddress mcastaddr = InetAddress.getByName(addr);
        InetSocketAddress group = new InetSocketAddress(mcastaddr, 0);
        NetworkInterface netIf = NetworkInterface.getByName(Constants.INTERFACE);
        socket.joinGroup(group, netIf);

        System.out.println("Started listening to mcast");
        while (inGroup) {
            DatagramPacket packet = new DatagramPacket(buf, buf.length);
            socket.receive(packet);
            String msg = new String(packet.getData(), 0, packet.getLength());
            ses.schedule(() -> process(msg, null),0,TimeUnit.SECONDS);
            if ("end".equals(msg)) break;
        }
        System.out.println("Stopped listening");
        socket.leaveGroup(group,netIf);
        socket.close();
        return null;
    };

    Callable process(String msg, SocketChannel sc){
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
            case Constants.PUT -> processPut(map);
            case Constants.DELETE -> processDelete(map);
            case Constants.GET -> {
                try {
                    processGet(map,sc);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            default -> {}
        }
        try {
            sc.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    private void processDelete(Map<String, String> map) {
        String key = map.get(Constants.KEY);
        System.out.println("Deleting key: " + key);
        keyPathMap.remove(key);
        delKeyVal(key);
    }

    private void processGet(Map<String, String> map, SocketChannel sc) throws IOException {
        Message message = new Message();
        String key = map.get(Constants.KEY);
        String value = "";
        if(keyPathMap.get(key) != null){
            value = readKeyVal(key);
        }
        Map<String, String> mapResp = new HashMap<>();
        mapResp.put(Constants.ACTION,Constants.GET_RESP);
        mapResp.put(Constants.BODY, value);
        byte[] buf = message.assembleMsg(mapResp).getBytes();
        sc.write(ByteBuffer.wrap(buf));
    }

    private void processPut(Map<String, String> map) {
        String key = map.get(Constants.KEY); byte[] val = map.get(Constants.BODY).getBytes();
        System.out.println("[put] Saving key: " + key + " value: " + map.get(Constants.BODY));
        keyPathMap.put(key,key);
        saveKeyVal(key, val);
    }

    private void processLog(Map<String, String> map) {
        map.forEach((k, v) -> {
            if(!k.equalsIgnoreCase(Constants.ACTION) && !k.equalsIgnoreCase(Constants.BODY) && !k.equals(id))
                addMembershipEntry(k,parseInt(v));
        });
    }

    private void processMembership(Map<String, String> map) {
        receivedMembership++;
        map.forEach((k, v) -> {
            if(!k.equalsIgnoreCase(Constants.ACTION) && !k.equalsIgnoreCase(Constants.BODY)){
                if(!members.contains(k)) {
                    System.out.println("Added member: " + k.substring(0,6));
                    members.add(k);
                }
                if(!memberInfo.containsKey(k)){
                    String[] parts = v.split(":");
                    memberInfo.put(k, new MemberInfo(parts[0],Integer.parseInt(parts[1]),Integer.parseInt(parts[2])));
                }
            }
        });
    }

    void processLeave(Map<String, String> map) {
        if(map.get(Constants.ID).equals(id)) return;
        ses.submit(()->memberInfo.remove(map.get(Constants.ID)));
        ses.submit(()->addMembershipEntry(map.get(Constants.ID), parseInt(map.get(Constants.COUNTER))));
    }


    void processJoin(Map<String, String> map) {
        if(map.get(Constants.ID).equals(id)) return;
        var id2 = map.get(Constants.ID);
        if(sentMembershipsTo.contains(id2)) return;
        sentMembershipsTo.add(map.get(Constants.ID));
        ses.schedule(()->sentMembershipsTo.remove(id2),15,TimeUnit.SECONDS);
        System.out.println(map.get(Constants.ID).substring(0,6) + " joined the cluster");

        ses.submit(() -> memberInfo.put(map.get(Constants.ID), new MemberInfo(map.get(Constants.ADDRESS),
                Integer.valueOf(map.get(Constants.MEMBERSHIP_PORT)),Integer.valueOf(map.get(Constants.PORT)))));

        ses.submit(() -> addMembershipEntry(map.get(Constants.ID), parseInt(map.get(Constants.COUNTER))));
        InetSocketAddress address = new InetSocketAddress(map.get(Constants.ADDRESS), Integer.parseInt(map.get(Constants.MEMBERSHIP_PORT)));


        ses.submit(()-> {
            try {
                sendMembership(address);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        ses.schedule(this::redistributeValues,1,TimeUnit.SECONDS);
    }

    private void redistributeValues() {
        System.out.println("Redistributing...");
        keyPathMap.forEach((k,v)->{
            if(!getResponsibleNode(k).equals(id)) {
                try {
                    sendPut(getResponsibleNode(k),k,readKeyVal(k).getBytes());
                    delKeyVal(k);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    public void sendMembership (InetSocketAddress address) throws IOException, InterruptedException {
        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.connect(address);

        Message message = new Message();
        Map<String, String> map = new HashMap<>();
        map.put("action", Constants.MEMBERSHIP);
        for(String key : members){
            MemberInfo memberInfo = this.memberInfo.get(key);
            if(memberInfo != null)  map.put(key, memberInfo.address + ":" + memberInfo.membershipPort+ ":" + memberInfo.port);
        }

        byte[] buf = message.assembleMsg(map).getBytes();
        TimeUnit.MILLISECONDS.sleep((new Random()).nextInt(0,100));
        socketChannel.write(ByteBuffer.wrap(buf));
        socketChannel.close();
        if (Constants.DEBUG) System.out.println("Sent Membership to " + address.toString());
    }

    //Listens for membership messages after join
    final Callable<String> membershipListener = () -> {
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
                    ses.submit(()->process(msg, sc));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        System.out.println("Stopped listening for membership messages");
        return "Task's execution";
    };


    //Listens for membership messages after join
    final Callable<String> mainListener = () -> {
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.socket().bind(new InetSocketAddress(localAddress,port));
        if(Constants.DEBUG) System.out.println("Starting to listen for messages on " + serverSocketChannel.getLocalAddress());
        while (inGroup){
            SocketChannel sc = serverSocketChannel.accept();
            String msg;
            ByteBuffer bb = ByteBuffer.allocate(1000);
            try {
                sc.read(bb);
                msg = new String(bb.array()).trim();
                ses.submit(()->process(msg, sc));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if("lol".equals("end")) break;
        }
        return "Task's execution";
    };




    Callable<String> sendLog() throws IOException {
        DatagramSocket socket = new DatagramSocket(new InetSocketAddress(0));
        NetworkInterface outgoingIf = NetworkInterface.getByName(Constants.INTERFACE);
        socket.setOption(StandardSocketOptions.IP_MULTICAST_IF, outgoingIf);

        InetAddress mcastaddr = InetAddress.getByName(IP_mcast_addr);
        InetSocketAddress dest = new InetSocketAddress(mcastaddr, IP_mcast_port);

        Message message = new Message();
        Map<String, String> map = new HashMap<>();
        map.put("action", Constants.LOG);
        int i = 0;
        for(String key : membershipLog.keySet()){
            map.put(key, membershipLog.get(key).toString());
            i++;
            if(i >= Constants.MAX_LOG) break;
        }

        byte[] buf = message.assembleMsg(map).getBytes();
        DatagramPacket packet = new DatagramPacket(buf, buf.length, dest);
        socket.send(packet);
        socket.close();
        if(Constants.DEBUG) System.out.println("Sent Log");
        if(inGroup) ses.schedule(this::sendLog,Constants.LOG_INTERVAL*1000 + Constants.LOG_INTERVAL*1000*(1/membershipLog.size()),TimeUnit.MILLISECONDS);
        else if (Constants.DEBUG) System.out.println("Stopped sending log msg");
        return null;
    }

    final Callable<String> sendJoin = () -> {
        DatagramSocket socket = new DatagramSocket(new InetSocketAddress(0));
        NetworkInterface outgoingIf = NetworkInterface.getByName(Constants.INTERFACE);
        socket.setOption(StandardSocketOptions.IP_MULTICAST_IF, outgoingIf);

        InetAddress mcastaddr = InetAddress.getByName(IP_mcast_addr);
        InetSocketAddress dest = new InetSocketAddress(mcastaddr, IP_mcast_port);


        Message message = new Message();
        Map<String, String> map = new HashMap<>();
        map.put("action", Constants.JOIN);
        map.put("id", id);
        map.put("counter", valueOf(counter));
        map.put("address", localAddress);
        map.put(Constants.MEMBERSHIP_PORT, valueOf(membershipPort));
        map.put(Constants.PORT, valueOf(port));

        byte[] buf = message.assembleMsg(map).getBytes();
        DatagramPacket packet = new DatagramPacket(buf, buf.length, dest);
        socket.send(packet);
        socket.close();
        sentJoins++;
        if (Constants.DEBUG) System.out.println("Sent Join");
        return "Sent Join";
    };

    final Callable<String> join = () -> {
        if(counter%2!=0)
            counter++;
        sentJoins = 0;
        receivedMembership = 0;

        ses.submit(mcastListener);
        ses.submit(membershipListener);
        ses.submit(mainListener);
        ses.submit(() -> addMembershipEntry(id,counter));
        ses.submit(() -> memberInfo.put(id, new MemberInfo(localAddress, membershipPort,port)));
        while (sentJoins < Constants.MAX_JOIN_TRIES && receivedMembership < Constants.MIN_RECEIVED_MEMBERSHIP){
            ses.submit(sendJoin);
            TimeUnit.SECONDS.sleep(1);
        }
        if(sentJoins >= Constants.MAX_JOIN_TRIES && receivedMembership < Constants.MIN_RECEIVED_MEMBERSHIP)
            System.out.println("Sent 3 joins and didn't get 3 memberships back... Inside cluster");
        if(receivedMembership >= Constants.MIN_RECEIVED_MEMBERSHIP)
            System.out.println("Received 3 memberships back. Inside cluster!");

        ses.schedule(this::sendLog,1000+(new Random()).nextInt(0,100),TimeUnit.MILLISECONDS);

        ses.submit(this::saveCounterDisk);
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


    final Callable<String> sendLeave = () -> {
        DatagramSocket socket = new DatagramSocket();
        InetAddress group = InetAddress.getByName(IP_mcast_addr);

        Message message = new Message();
        Map<String, String> map = new HashMap<>();
        map.put(Constants.ACTION, Constants.LEAVE);
        map.put(Constants.ID, id);
        map.put(Constants.COUNTER, valueOf(counter));

        byte[] buf = message.assembleMsg(map).getBytes();
        DatagramPacket packet = new DatagramPacket(buf, buf.length, group, IP_mcast_port);
        socket.send(packet);
        socket.close();
        members.remove(id);
        ses.schedule(this::redistributeValues,1,TimeUnit.SECONDS);
        if (Constants.DEBUG) System.out.println("Sent Leave");
        return "Sent Leave";
    };

    final Callable<String> leave = () -> {
        System.out.println("Leaving");

        ses.submit(() -> addMembershipEntry(id,++counter));
        ses.submit(() -> memberInfo.remove(id));
        ses.submit(sendLeave);
        return "Left cluster";
    };

    @Override
    public String leave() throws RemoteException, ExecutionException, InterruptedException {
        if(!inGroup) return "Already left";
        inGroup = false;
        ScheduledFuture<String> future = ses.schedule(leave, 0, TimeUnit.SECONDS);
        while (!future.isDone()) TimeUnit.SECONDS.sleep(1);
        return future.get();
    }


    public void saveKeyVal(String key, byte[] bs){
        String path = id + File.separator + Constants.STORE_FOLDER + File.separator + key;
        try (FileOutputStream fos = new FileOutputStream(path);
            ObjectOutputStream oos = new ObjectOutputStream(fos)) {
            oos.write(bs);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    String putCall(String key, byte[] bs){
        String nodeId = getResponsibleNode(key);
        ArrayList<String> replicators = getReplicatorNodes(key);
        if(nodeId.equals(id)){
            System.out.println("Inserting key " + key);
            keyPathMap.put(key,key);
            saveKeyVal(key, bs);
        }
        else {
            ses.submit(()->sendPut(nodeId, key, bs));
            System.out.println("Not my key ("+key+")... redirecting it to " + nodeId.substring(0,6));
        }
        for(String replicator : replicators){
            ses.submit(()->sendPut(replicator,key,bs));
        }
        return "Put " + key;
    }

    Callable<String> sendPut(String nodeId, String key, byte[] bs) throws IOException {
        SocketChannel socketChannel = SocketChannel.open();
        InetSocketAddress address = new InetSocketAddress(memberInfo.get(nodeId).address, memberInfo.get(nodeId).port);
        System.out.println("Send put to " + address);
        socketChannel.connect(address);
        Message message = new Message();
        Map<String, String> map = new HashMap<>();
        map.put("action", Constants.PUT);
        map.put(Constants.KEY, key);
        map.put(Constants.BODY, new String(bs));
        byte[] buf = message.assembleMsg(map).getBytes();
        socketChannel.write(ByteBuffer.wrap(buf));
        socketChannel.close();
        return null;
    }

    @Override
    public String put(String key, byte[] bs) throws RemoteException, InterruptedException, ExecutionException {
        Future<String> future = ses.submit(()-> putCall(key, bs)
        );
        while(!future.isDone()) TimeUnit.MILLISECONDS.sleep(100);
        return future.get();
    }


    public String readKeyVal(String key){
        byte[] bs;
        String value = "";
        String path = id + File.separator + Constants.STORE_FOLDER + File.separator + key;
        File file = new File(path);
        if(!file.exists()){
            System.out.println("Just noticed I lost this file -> " + key);
            keyPathMap.remove(key);
            return value;
        }
        try (FileInputStream fis = new FileInputStream(path);
            ObjectInputStream ois = new ObjectInputStream(fis)) {
            bs = ois.readAllBytes();
            value = new String(bs);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return value;
    }

    String getCall(String key){
        String nodeId = getResponsibleNode(key);
        var replicators = getReplicatorNodes(key);
        String value = "";
        if(nodeId.equals(id) || (keyPathMap.contains(key) && replicators.contains(id))){
            System.out.println("Getting key " + key);
            if(keyPathMap.get(key) != null){
                value = readKeyVal(key);
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

    private String requestValue(String nodeId, String key) throws Exception {
        SocketChannel socketChannel = SocketChannel.open();
        InetSocketAddress address = new InetSocketAddress(memberInfo.get(nodeId).address, memberInfo.get(nodeId).port);
        socketChannel.configureBlocking(false);
        boolean connected =  socketChannel.connect(address);
        if(!connected) return "";

        Message message = new Message();
        Map<String, String> map = new HashMap<>();
        map.put("action", Constants.GET);
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


    @Override
    public String get(String key) throws RemoteException, ExecutionException, InterruptedException {
        Future<String> future = ses.submit(()-> getCall(key)
            );
        while(!future.isDone()) TimeUnit.SECONDS.sleep(1);
        return future.get();
    }

    @Override
    public String delete(String key) throws RemoteException, ExecutionException, InterruptedException {
        Future<String> future = ses.submit(()-> deleteCall(key)
        );
        while(!future.isDone()) TimeUnit.SECONDS.sleep(1);
        return future.get();
    }

    private String deleteCall(String key) {
        String nodeId = getResponsibleNode(key);
        if(nodeId.equals(id)){
            System.out.println("Deleting key " + key);
            keyPathMap.remove(key);
            delKeyVal(key);
        }
        else {
            ses.submit(()-> {
                try {
                    sendDelete(nodeId, key);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            System.out.println("Not my key ("+key+")... deleting it from " + nodeId.substring(0,6));
        }
        return "Deleted " + key;
    }

    private void sendDelete(String nodeId, String key) throws IOException {
        SocketChannel socketChannel = SocketChannel.open();
        InetSocketAddress address = new InetSocketAddress(memberInfo.get(nodeId).address, memberInfo.get(nodeId).port);
        socketChannel.connect(address);
        Message message = new Message();
        Map<String, String> map = new HashMap<>();
        map.put("action", Constants.DELETE);
        map.put(Constants.KEY, key);
        byte[] buf = message.assembleMsg(map).getBytes();
        socketChannel.write(ByteBuffer.wrap(buf));
        socketChannel.close();
    }

    private void delKeyVal(String key) {
        String path = id + File.separator + Constants.STORE_FOLDER + File.separator + key;
        File file = new File(path);
        file.delete();
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
        saveMembersDisk();
        saveLogDisk();
    }

    public void showMembers(){
        System.out.println("\nMembers");
        for (String member : members) {
            System.out.println(member.substring(0,6));
        }
        memberInfo.forEach((k,v)-> System.out.println(k.substring(0,6) + " " + memberInfo.get(k).address + " " + memberInfo.get(k).port));
    }

    public void showMembershipLog(){
        System.out.println("\nMembership Log");
        membershipLog.forEach((k,v)-> System.out.println("Id: " + k.substring(0,6) + " | Counter: " + v));
    }

    String binarySearch(List<String> arr, int l, int r, String x)
    {
        if(arr.size() == 1) return arr.get(0);
        if(arr.size() == 2){
            if(arr.get(0).compareTo(x) < 0 && arr.get(1).compareTo(x) >= 0)
                return arr.get(1);
            else return arr.get(0);
        }
        if (r >= l) {
            int mid = l + (r - l) / 2;

            if(mid >= arr.size())
                return arr.get(0);
            if(mid <= 0)
                return arr.get(0);

            if ((arr.get(mid)).compareTo(x) > 0  && (arr.get(mid-1)).compareTo(x) < 0){
                return arr.get(mid);
            }

            if ((arr.get(mid)).compareTo(x) > 0)
            {
                return binarySearch(arr, l, mid-1, x);
            }

            return binarySearch(arr, mid+1, r, x);
        }

        return "";
    }

    public String getResponsibleNode(String key){
        return binarySearch(this.members,0, this.members.size(),key);
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
        System.out.println("\nKeys:");
        this.keyPathMap.forEach((k,v)-> System.out.println("key: "+ k + "\tpath: " + v));
    }

    public void loadFromDisk(){
        String directoryName = this.id;
        File directory = new File(directoryName);
        if (!directory.exists()){
            directory.mkdir();
        }
        File members = new File((directoryName + File.separator + Constants.MEMBERS_FILENAME));
        File memberinfo = new File((directoryName + File.separator + Constants.MEMBERINFO_FILENAME));
        if(members.exists() && memberinfo.exists())
            loadMembersDisk();
        File log = new File((directoryName + File.separator + Constants.LOG_FILENAME));
        if(log.exists())
            loadLogDisk();

        File store = new File(directoryName + File.separator + Constants.STORE_FOLDER);
        if(!store.exists()){
            store.mkdir();
        }
        loadStoreDisk();

        File counter = new File(directoryName + File.separator + Constants.COUNTER);
        if(counter.exists())
            loadCounterDisk();
    }

    private void loadCounterDisk() {
        try (FileInputStream fis = new FileInputStream(this.id + File.separator+ Constants.COUNTER);
             ObjectInputStream ois = new ObjectInputStream(fis)) {
            this.counter = (Integer) ois.readObject();
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
    private void saveCounterDisk(){
        try (FileOutputStream fos = new FileOutputStream(this.id + File.separator+ Constants.COUNTER);
             ObjectOutputStream oos = new ObjectOutputStream(fos)) {
            oos.writeObject(this.counter);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private void loadStoreDisk() {
        File store = new File(this.id + File.separator + Constants.STORE_FOLDER);
        for(File entry : store.listFiles()){
            keyPathMap.put(entry.getName(),entry.getName());
        }
    }

    public void saveMembersDisk(){//TODO: Save memberinfo
        try (FileOutputStream fos = new FileOutputStream(this.id + File.separator+ Constants.MEMBERS_FILENAME);
             ObjectOutputStream oos = new ObjectOutputStream(fos)) {
            oos.writeObject(this.members);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        try (FileOutputStream fos = new FileOutputStream(this.id + File.separator+ Constants.MEMBERINFO_FILENAME);
             ObjectOutputStream oos = new ObjectOutputStream(fos)) {
            oos.writeObject(this.memberInfo);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public void loadMembersDisk(){//TODO load memberinfo
        try (FileInputStream fis = new FileInputStream(this.id + File.separator+ Constants.MEMBERS_FILENAME);
             ObjectInputStream ois = new ObjectInputStream(fis)) {
            this.members = (List<String>) ois.readObject();
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        try (FileInputStream fis = new FileInputStream(this.id + File.separator+ Constants.MEMBERINFO_FILENAME);
             ObjectInputStream ois = new ObjectInputStream(fis)) {
            this.memberInfo = (Map<String, MemberInfo>) ois.readObject();
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
