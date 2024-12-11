package protocols.statemachine.Utils;

import org.apache.commons.codec.binary.Hex;
import java.io.*;
import java.net.InetAddress;

import pt.unl.fct.di.novasys.network.data.Host;


public class MembershipOp {

    public final static int ID = 0;


    public final static int REMOVE = 0;
    public final static int ADD = 1;
    public final static int CHANGE_LEADER = 2;


    private int type;
    private Host host;

    public MembershipOp(int type, Host host) {
        this.type = type;
        this.host = host;
    }

    public int getType() {
        return type;
    }

    public Host getHost(){
        return host;
    }


    public byte[] toByteArray() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(type);
        InetAddress address = host.getAddress();
        int port = host.getPort();
        dos.writeByte(address.hashCode());
        dos.writeInt(port);
        return baos.toByteArray();
    }

    public static MembershipOp fromByteArray(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);
        int type = dis.readInt();
        InetAddress address = InetAddress.getByName(dis.readUTF());
        int port = dis.readInt();
        Host host = new Host(address, port);

        return new MembershipOp(type, host);
    }

}
