package protocols.abd.messages;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class ReadTagRepMsg extends ProtoMessage {

    public static final short MSG_ID = 202;

    private final int opSeq;
    private final String tag;

    public ReadTagRepMsg(int opSeq, String tag) {
        super(MSG_ID);
        this.opSeq = opSeq;
        this.tag = tag;
    }

    public int getOpSeq() {
        return opSeq;
    }

    public String getTag() {
        return tag;
    }

    public static ISerializer<ReadTagRepMsg> serializer = new ISerializer<>() {
        @Override
        public void serialize(ReadTagRepMsg sampleMessage, ByteBuf out) throws IOException {
            out.writeInt(sampleMessage.opSeq);
            out.writeInt(sampleMessage.tag.length());
            out.writeBytes(sampleMessage.tag.getBytes());
        }

        @Override
        public ReadTagRepMsg deserialize(ByteBuf in) throws IOException {
            int opSeq = in.readInt();
            int tagLength = in.readInt();
            byte[] tagBytes = new byte[tagLength];
            in.readBytes(tagBytes);
            String aux = new String(tagBytes);
            return new ReadTagRepMsg(opSeq, aux);
        }
    };

}
