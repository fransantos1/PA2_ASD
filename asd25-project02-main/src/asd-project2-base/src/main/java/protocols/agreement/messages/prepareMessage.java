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
    private final int instance;

    public prepareMessage(Host host, int ballot, int instance) {
        super(MSG_ID);
        this.host = host;
        this.ballot = ballot;
        this.instance = instance;
    }

    public Host getHost() {
        return host;
    }
    public int getBallot() {
        return ballot;
    }

    public int getInstance() {
        return instance;
    }

    public static ISerializer<prepareMessage> serializer = new ISerializer<prepareMessage>() {
        @Override
        public void serialize(prepareMessage msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.host, out);
            out.writeInt(msg.ballot);
            out.writeInt(msg.instance);
        }

        @Override
        public prepareMessage deserialize(ByteBuf in) throws IOException {
            Host host = Host.serializer.deserialize(in);
            int ballot = in.readInt();
            int instance = in.readInt();
            return new prepareMessage(host, ballot, instance);
        }
    };

}
