import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

public class Utils {

    public static boolean available(int port) {
        try (Socket ignored = new Socket("localhost", port)) {
            return false;
        } catch (IOException ignored) {
            return true;
        }
    }
    public static int getFreePort(){
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            int port = serverSocket.getLocalPort();
            serverSocket.close();
            return port;
        } catch (IOException e) {
            return -1;
        }
    }

    public static String binarySearch(List<String> arr, int l, int r, String x)
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

}
