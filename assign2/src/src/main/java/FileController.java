import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

public class FileController {
    StorageNode node;
    public FileController(StorageNode node) {
        this.node = node;
    }

    public void loadFromDisk(){
        String directoryName = node.id;
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
        try (FileInputStream fis = new FileInputStream(node.id + File.separator+ Constants.COUNTER);
             ObjectInputStream ois = new ObjectInputStream(fis)) {
            node.counter = (Integer) ois.readObject();
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
    public void saveCounterDisk(){
        try (FileOutputStream fos = new FileOutputStream(node.id + File.separator+ Constants.COUNTER);
             ObjectOutputStream oos = new ObjectOutputStream(fos)) {
            oos.writeObject(node.counter);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private void loadStoreDisk() {
        File store = new File(node.id + File.separator + Constants.STORE_FOLDER);
        for(File entry : store.listFiles()){
            node.keyPathMap.put(entry.getName(),entry.getName());
        }
    }

    public void saveMembersDisk(){
        try (FileOutputStream fos = new FileOutputStream(node.id + File.separator+ Constants.MEMBERS_FILENAME);
             ObjectOutputStream oos = new ObjectOutputStream(fos)) {
            oos.writeObject(node.members);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        try (FileOutputStream fos = new FileOutputStream(node.id + File.separator+ Constants.MEMBERINFO_FILENAME);
             ObjectOutputStream oos = new ObjectOutputStream(fos)) {
            oos.writeObject(node.memberInfo);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public void loadMembersDisk(){
        try (FileInputStream fis = new FileInputStream(node.id + File.separator+ Constants.MEMBERS_FILENAME);
             ObjectInputStream ois = new ObjectInputStream(fis)) {
            node.members = (List<String>) ois.readObject();
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        try (FileInputStream fis = new FileInputStream(node.id + File.separator+ Constants.MEMBERINFO_FILENAME);
             ObjectInputStream ois = new ObjectInputStream(fis)) {
            node.memberInfo = (Map<String, MemberInfo>) ois.readObject();
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public void saveLogDisk(){
        try (FileOutputStream fos = new FileOutputStream(node.id + File.separator+ Constants.LOG_FILENAME);
             ObjectOutputStream oos = new ObjectOutputStream(fos)) {
            oos.writeObject(node.membershipLog);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public void loadLogDisk(){
        try (FileInputStream fis = new FileInputStream(node.id + File.separator+ Constants.LOG_FILENAME);
             ObjectInputStream ois = new ObjectInputStream(fis)) {
            node.membershipLog = (SortedMap<String, Integer>) ois.readObject();
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public void saveKeyVal(String key, byte[] bs){
        String path = node.id + File.separator + Constants.STORE_FOLDER + File.separator + key;
        try (FileOutputStream fos = new FileOutputStream(path);
             ObjectOutputStream oos = new ObjectOutputStream(fos)) {
            oos.write(bs);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public String readKeyVal(String key){
        byte[] bs;
        String value = "";
        String path = node.id + File.separator + Constants.STORE_FOLDER + File.separator + key;
        File file = new File(path);
        if(!file.exists()){
            System.out.println("Just noticed I lost this file -> " + key);
            node.keyPathMap.remove(key);
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

    public String delKeyVal(String key) {
        String path = node.id + File.separator + Constants.STORE_FOLDER + File.separator + key;
        File file = new File(path);
        file.delete();
        return "";
    }
}
