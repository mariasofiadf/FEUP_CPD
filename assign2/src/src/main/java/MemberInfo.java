import java.io.Serializable;

public class MemberInfo implements Serializable {
    final String address;
    final Integer membershipPort;
    final Integer port;

    public MemberInfo(String address, Integer membershipPort, Integer port) {
        this.address = address;
        this.membershipPort = membershipPort;
        this.port = port;
    }
}
