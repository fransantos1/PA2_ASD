package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class joinMessage extends ProtoMessage {

    public final static short MSG_ID = 202;

    private final Host host;

    public joinMessage(Host host) {
        super(MSG_ID);
        this.host = host;
    }

    public Host getHost() {
        return host;
    }

    public static ISerializer<joinMessage> serializer = new ISerializer<joinMessage>() {
        @Override
        public void serialize(joinMessage msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.host, out);
        }

        @Override
        public joinMessage deserialize(ByteBuf in) throws IOException {
            return new joinMessage(Host.serializer.deserialize(in));
        }
    };

}
