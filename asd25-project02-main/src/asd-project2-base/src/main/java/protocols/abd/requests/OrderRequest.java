package protocols.abd.requests;

import org.apache.commons.codec.binary.Hex;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

import java.util.UUID;

public class OrderRequest extends ProtoRequest {

    public static final short REQUEST_ID = 201;

    private final UUID opId;
    private final byte[] operation;

    public OrderRequest(UUID opId, byte[] operation) {
        super(REQUEST_ID);
        this.opId = opId;
        this.operation = operation;
    }

    public byte[] getOperation() {
        return operation;
    }

    public UUID getOpId() {
        return opId;
    }

    @Override
    public String toString() {
        return "OrderRequest{" +
                "opId=" + opId +
                ", operation=" + Hex.encodeHexString(operation) +
                '}';
    }
}
