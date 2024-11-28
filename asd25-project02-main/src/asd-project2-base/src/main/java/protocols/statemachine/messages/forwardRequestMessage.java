package protocols.statemachine.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;
import protocols.app.messages.ResponseMessage;
import protocols.statemachine.requests.OrderRequest;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

public class forwardRequestMessage extends ProtoMessage {

    public final static short MSG_ID = 403;

    private final OrderRequest req;

    public forwardRequestMessage(OrderRequest req) {
        super(MSG_ID);
        this.req = req;
    }

    public OrderRequest getReq() {
        return req;
    }

    public static ISerializer<forwardRequestMessage> serializer = new ISerializer<forwardRequestMessage>() {
        @Override
        public void serialize(forwardRequestMessage responseMsg, ByteBuf out) {

            out.writeInt(responseMsg.req.getOperation().length);
            out.writeBytes(responseMsg.req.getOperation());

            UUID id = responseMsg.req.getOpId();
            long mostSigBits = id.getMostSignificantBits();
            long leastSigBits = id.getLeastSignificantBits();
            out.writeLong(mostSigBits);
            out.writeLong(leastSigBits);
        }

        @Override
        public forwardRequestMessage deserialize(ByteBuf in) {
            byte[] operation = new byte[in.readInt()];
            in.readBytes(operation);
            long mostSigBits = in.readLong();
            long leastSigBits = in.readLong();
            UUID id = new UUID(mostSigBits, leastSigBits);
            return new forwardRequestMessage(new OrderRequest(id, operation));
        }
    };
}
