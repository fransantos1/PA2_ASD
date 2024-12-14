package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

public class broadcastMessage extends ProtoMessage {

    public final static short MSG_ID = 201;

    public static final short APP_OPERATION = 0;
    public static final short MEMBERSHIP_OP = 1;

    public final static int PREPARE = 0;
    public final static int ACCEPT = 1;
    public final static int ACCEPT_OK = 2;

    private final UUID opId;
    private final int instance;
    private final byte[] op;
    private final int opType;


    private final int type;
    private final int ballot;


    public broadcastMessage(int instance, UUID opId, byte[] op, int type, int ballot, int opType) {
        super(MSG_ID);
        this.instance = instance;
        this.op = op;
        this.opId = opId;
        this.type = type;
        this.ballot = ballot;
        this.opType = opType;
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

    public int getBallot() {
        return ballot;
    }

    public int getOpType() { return opType; }

    @Override
    public String toString() {
        return "BroadcastMessage{" +
                "opId=" + opId +
                ", instance=" + instance +
                ", op=" + Hex.encodeHexString(op) +
                '}';
    }

    public static ISerializer<broadcastMessage> serializer = new ISerializer<broadcastMessage>() {
        @Override
        public void serialize(broadcastMessage msg, ByteBuf out) {
            out.writeInt(msg.instance);
            out.writeLong(msg.opId.getMostSignificantBits());
            out.writeLong(msg.opId.getLeastSignificantBits());
            out.writeInt(msg.op.length);
            out.writeBytes(msg.op);
            out.writeInt(msg.type);
            out.writeInt(msg.ballot);
            out.writeInt(msg.opType);
        }

        @Override
        public broadcastMessage deserialize(ByteBuf in) {
            int instance = in.readInt();
            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID opId = new UUID(highBytes, lowBytes);
            byte[] op = new byte[in.readInt()];
            in.readBytes(op);
            int type = in.readInt();
            int ballot = in.readInt();
            int opType = in.readInt();
            return new broadcastMessage(instance, opId, op, type, ballot, opType);
        }
    };

}
