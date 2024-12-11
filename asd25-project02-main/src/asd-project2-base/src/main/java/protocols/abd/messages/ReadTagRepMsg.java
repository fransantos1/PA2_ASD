package protocols.abd.messages;

import java.io.IOException;
import java.sql.Timestamp;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class ReadTagRepMsg extends ProtoMessage {

    public static final short MSG_ID = 108;

    private final long opSeq;
    private final long tagl;
    private final long tagr;
    private final long key;

    public ReadTagRepMsg(long opSeq, long tagl, long tagr, long key) {
        super(MSG_ID);
        this.opSeq = opSeq;
        this.tagl = tagl;
        this.tagr = tagr;
        this.key = key;
    }

    public long getOpSeq() {
        return opSeq;
    }

    public long getTagl() {
        return tagl;
    }

    public long getTagr() {
        return tagr;
    }

    public long getKey() {
        return key;
    }

    public static ISerializer<ReadTagRepMsg> serializer = new ISerializer<>() {
        @Override
        public void serialize(ReadTagRepMsg sampleMessage, ByteBuf out) throws IOException {
            out.writeLong(sampleMessage.opSeq);
            out.writeLong(sampleMessage.tagl);
            out.writeLong(sampleMessage.tagr);
            out.writeLong(sampleMessage.key);
        }

        @Override
        public ReadTagRepMsg deserialize(ByteBuf in) throws IOException {
            long opSeq = in.readLong();
            long tagl = in.readLong();
            long tagr = in.readLong();
            long key = in.readLong();
            return new ReadTagRepMsg(opSeq, tagl, tagr, key);
        }
    };

}
