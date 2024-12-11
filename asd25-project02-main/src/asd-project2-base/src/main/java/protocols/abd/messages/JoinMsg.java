package protocols.abd.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class JoinMsg extends ProtoMessage {

    public static final short MSG_ID = 102;

    private final Host hostJoining;

    public JoinMsg(Host hostJoining){
        super(MSG_ID);
        this.hostJoining = hostJoining;
    }

    public Host getHostJoining() {
        return hostJoining;
    }

    public static ISerializer<JoinMsg> serializer = new ISerializer<>() {
        @Override
        public void serialize(JoinMsg sampleMessage, ByteBuf out) throws IOException {
            Host.serializer.serialize(sampleMessage.getHostJoining(), out);
        }

        @Override
        public JoinMsg deserialize(ByteBuf in) throws IOException {
            return new JoinMsg(Host.serializer.deserialize(in));
        }
    };

}
