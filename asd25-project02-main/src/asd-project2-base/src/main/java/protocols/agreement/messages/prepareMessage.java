package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import protocols.agreement.IncorrectAgreement;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class prepareMessage extends ProtoMessage {

    public final static short MSG_ID = 103;
    private final Host host;
    private final int ballot;

    public prepareMessage(Host host, int ballot) {
        super(MSG_ID);
        this.host = host;
        this.ballot = ballot;
    }

    public Host getHost() {
        return host;
    }
    public int getBallot() {
        return ballot;
    }

    public static ISerializer<prepareMessage> serializer = new ISerializer<prepareMessage>() {
        @Override
        public void serialize(prepareMessage msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.host, out);
            out.writeInt(msg.ballot);
        }

        @Override
        public prepareMessage deserialize(ByteBuf in) throws IOException {
            Host host = Host.serializer.deserialize(in);
            int ballot = in.readInt();
            return new prepareMessage(host, ballot);
        }
    };

}
