package protocols.abd.messages;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class ReadTagMsg extends ProtoMessage{

    public static final short MSG_ID = 201;

    private final int opSeq;
    private final String key;

    public ReadTagMsg(int opSeq, String key) {
        super(MSG_ID);
        this.opSeq = opSeq;
        this.key = key;
    }

    public int getOpSeq() {
        return opSeq;
    }

    public String getKey() {
        return key;
    }

    public static ISerializer<ReadTagMsg> serializer = new ISerializer<>() {
        @Override
        public void serialize(ReadTagMsg sampleMessage, ByteBuf out) throws IOException {
            out.writeInt(sampleMessage.opSeq);
            out.writeInt(sampleMessage.key.length());
            out.writeBytes(sampleMessage.key.getBytes());
        }

        @Override
        public ReadTagMsg deserialize(ByteBuf in) throws IOException {
            int opSeq = in.readInt();
            int keyLength = in.readInt();
            byte[] keyBytes = new byte[keyLength];
            in.readBytes(keyBytes);
            String aux = new String(keyBytes);
            return new ReadTagMsg(opSeq, aux);
        }
    };



}
