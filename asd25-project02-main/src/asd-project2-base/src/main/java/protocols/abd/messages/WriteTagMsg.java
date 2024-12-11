package protocols.abd.messages;

import io.netty.buffer.ByteBuf;
import protocols.app.utils.Operation;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class WriteTagMsg extends ProtoMessage {

    public static final short MSG_ID = 109;

    private final long opSeq;
    private final UUID uuid;
    private final long key;
    private final long newTagl;
    private final long newTagr;
    private final long newValue;

    public WriteTagMsg(long opSeq, UUID uuid, long key, long newTagl, long newTagr, long newValue){
        super(MSG_ID);
        this.opSeq = opSeq;
        this.uuid = uuid;
        this.key = key;
        this.newTagl = newTagl;
        this.newTagr = newTagr;
        this.newValue = newValue;
    }

    public UUID getUuid() {
        return uuid;
    }

    public long getOpSeq() {
        return opSeq;
    }

    public long getKey() {
        return key;
    }

    public long getNewTagl() {
        return newTagl;
    }

    public long getNewTagr() {
        return newTagr;
    }

    public long getNewValue() {
        return newValue;
    }

    public static ISerializer<WriteTagMsg> serializer = new ISerializer<>() {
        @Override
        public void serialize(WriteTagMsg sampleMessage, ByteBuf out) throws IOException {
            out.writeLong(sampleMessage.opSeq);

            out.writeLong(sampleMessage.uuid.getMostSignificantBits());
            out.writeLong(sampleMessage.uuid.getLeastSignificantBits());

            out.writeLong(sampleMessage.key);

            out.writeLong(sampleMessage.newTagl);
            out.writeLong(sampleMessage.newTagr);

            out.writeLong(sampleMessage.newValue);
        }

        @Override
        public WriteTagMsg deserialize(ByteBuf in) throws IOException {
            long opSeq = in.readLong();

            UUID uuid = new UUID(in.readLong(), in.readLong());

            long key = in.readLong();

            long newTagl = in.readLong();

            long newTagr = in.readLong();

            long newValue = in.readLong();

            return new WriteTagMsg(opSeq, uuid, key, newTagl, newTagr, newValue);
        }
    };
}
