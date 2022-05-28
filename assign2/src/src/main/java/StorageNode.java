import java.io.*;
import java.net.*;
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
import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;


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

    Queue<Task> qUcast = new LinkedList<>();

    StorageNode(String IP_mcast_addr, String IP_mcast_port, String id, String port) throws SocketException, UnknownHostException {
        this.ses = Executors.newScheduledThreadPool(Constants.MAX_THREADS);
        this.keyPathMap = new ConcurrentHashMap<>();
        this.membershipLog = new TreeMap<>();
        this.members = new ArrayList<>();
        this.memberInfo = new HashMap();
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
            SelectionKey selectKy = datagramChannel.register(selector, ops, Constants.CHANNEL_MCAST);

            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            InetSocketAddress hostAddress = new InetSocketAddress("localhost",  getFreePort());
            serverSocketChannel.bind(hostAddress);
            serverSocketChannel.configureBlocking(false);
            node.membershipPort = hostAddress.getPort();
            System.out.println(node.membershipPort);
            serverSocketChannel.register(selector, serverSocketChannel.validOps(),Constants.CHANNEL_MEMBERSHIP);

            Pipe pipe = Pipe.open();
            SelectableChannel pipeChannelSource = pipe.source();
            pipeChannelSource.configureBlocking(false);
            pipeChannelSource.register(selector, OP_READ, Constants.CHANNEL_FILES_READ);
            SelectableChannel pipeChannelSink = pipe.sink();
            pipeChannelSink.configureBlocking(false);
            pipeChannelSink.register(selector, OP_WRITE, Constants.CHANNEL_FILES_WRITE);
            FileController fileController = new FileController(pipe);
            fileController.run();

            System.out.printf("[Main] Node initialized with IP_mcast_addr=%s IP_mcast_port=%d node_id=%s Store_port=%d%n",
                    node.IP_mcast_addr, node.IP_mcast_port, node.id.substring(0,6), node.port);


            Functions functionsStub = (Functions) UnicastRemoteObject.exportObject(node,0);
            Registry registry = LocateRegistry.getRegistry();
            registry.rebind(Constants.REG_FUNC_VAL, functionsStub);
            boolean stop = false;
            if(Constants.DEBUG)
                node.ses.schedule(new DebugHelper(node),0,TimeUnit.SECONDS);

            for (;;) {  
                int noOfKeys = selector.select();  
                Set selectedKeys = selector.selectedKeys();  
                Iterator itr = selectedKeys.iterator();
                while (itr.hasNext()) {
                    SelectionKey ky = (SelectionKey) itr.next();
                    String channelType = (String) ky.attachment();
                    if (ky.isAcceptable()) {
                        // The new client connection is accepted
                        SocketChannel socketChannel = serverSocketChannel.accept();
                        socketChannel.configureBlocking(false);
                        // The new connection is added to a selector
                        socketChannel.register(selector, SelectionKey.OP_READ);
                    }
                    if (ky.isReadable()) {
                        switch (channelType){
                            case Constants.CHANNEL_MCAST -> node.ses.schedule(new MsgProcessor(node, (DatagramChannel) ky.channel()),0,TimeUnit.SECONDS);
                            //case Constants.CHANNEL_FILES_READ -> node.ses.schedule(new MsgProcessor(node, (SelectableChannel) ky.channel()),0,TimeUnit.SECONDS);
                            case Constants.CHANNEL_MEMBERSHIP -> node.ses.schedule(new MsgProcessor(node, (SocketChannel) ky.channel()), 0, TimeUnit.SECONDS);
                        }
                    }
                    else if (ky.isWritable()) {
                        switch (channelType){
                            case Constants.CHANNEL_MCAST -> {
                                if(!node.qMcast.isEmpty()) {
                                    Task task = node.qMcast.remove();
                                    node.ses.schedule(new UDPMulticastSender(node, task.getType(), (DatagramChannel) ky.channel(), address), task.getDelay(), TimeUnit.SECONDS);
                                }
                            }
                        }

                    }
                    itr.remove();
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
        qMcast.add(new Task(Constants.LOG,Constants.MEMBERSHIP_INTERVAL));
        qMcast.add(new Task(Constants.JOIN));
        addMembershipEntry(this.id,this.counter);
        return "";
    }


    @Override
    public String leave() throws RemoteException, ExecutionException, InterruptedException {
        if(!inGroup) return "Already left";
        inGroup = false;
        qMcast.add(new Task(Constants.LEAVE));
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
