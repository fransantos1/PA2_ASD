package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class JoinMessage extends ProtoMessage {

    public final static short MSG_ID = 102;

    private final Host host;

    public JoinMessage(Host host) {
        super(MSG_ID);
        this.host = host;
    }

    public Host getHost() {
        return host;
    }

    public static ISerializer<JoinMessage> serializer = new ISerializer<JoinMessage>() {
        @Override
        public void serialize(JoinMessage msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.host, out);
        }

        @Override
        public JoinMessage deserialize(ByteBuf in) throws IOException {
            return new JoinMessage(Host.serializer.deserialize(in));
        }
    };

}
