import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;


public class Client {
    public static void main(String[] args) {

        try {
            Registry registry = LocateRegistry.getRegistry();
            Functions functionsStub = (Functions) registry.lookup(Constants.REG_FUNC_VAL);
            String response = functionsStub.get(1);
            System.out.println("response: " + response);
        } catch (Exception e) {
            System.err.println("Client exception: " + e);
            e.printStackTrace();
        }
    }
}
