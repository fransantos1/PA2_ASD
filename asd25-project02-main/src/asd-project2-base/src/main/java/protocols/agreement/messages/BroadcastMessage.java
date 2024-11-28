package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

/*************************************************
 * This is here just as an example, your solution
 * probably needs to use different message types
 *************************************************/
public class BroadcastMessage extends ProtoMessage {

    public final static short MSG_ID = 101;

    public final static int PREPARE = 0;
    public final static int ACCEPT = 1;
    public final static int ACCEPT_OK = 2;

    private final UUID opId;
    private final int instance;
    private final byte[] op;
    private final int type;


    public BroadcastMessage(int instance, UUID opId, byte[] op, int type) {
        super(MSG_ID);
        this.instance = instance;
        this.op = op;
        this.opId = opId;
        this.type = type;
    }

    public int getInstance() {
        return instance;
    }

    public UUID getOpId() {
        return opId;
    }

    public byte[] getOp() {
        return op;
    }

    public int getType() {
        return type;
    }

    @Override
    public String toString() {
        return "BroadcastMessage{" +
                "opId=" + opId +
                ", instance=" + instance +
                ", op=" + Hex.encodeHexString(op) +
                '}';
    }

    public static ISerializer<BroadcastMessage> serializer = new ISerializer<BroadcastMessage>() {
        @Override
        public void serialize(BroadcastMessage msg, ByteBuf out) {
            out.writeInt(msg.instance);
            out.writeLong(msg.opId.getMostSignificantBits());
            out.writeLong(msg.opId.getLeastSignificantBits());
            out.writeInt(msg.op.length);
            out.writeBytes(msg.op);
            out.writeInt(msg.type);
        }

        @Override
        public BroadcastMessage deserialize(ByteBuf in) {
            int instance = in.readInt();
            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID opId = new UUID(highBytes, lowBytes);
            byte[] op = new byte[in.readInt()];
            in.readBytes(op);
            int type = in.readInt();
            return new BroadcastMessage(instance, opId, op, type);
        }
    };

}
