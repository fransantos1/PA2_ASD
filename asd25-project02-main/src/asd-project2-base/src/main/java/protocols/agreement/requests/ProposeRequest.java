package protocols.agreement.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import org.apache.commons.codec.binary.Hex;

import java.util.UUID;

public class ProposeRequest extends ProtoRequest {

    public static final short REQUEST_ID = 208;
    public static final short APP_OPERATION = 0;
    public static final short MEMBERSHIP_OP = 1;

    private final int instance;
    private final UUID opId;
    private final byte[] operation;
    private final int opType;


    public ProposeRequest(int instance, UUID opId, byte[] operation , int opType) {
        super(REQUEST_ID);
        this.instance = instance;
        this.opId = opId;
        this.operation = operation;
        this.opType = opType;
    }

    public int getInstance() {
        return instance;
    }

    public byte[] getOperation() {
        return operation;
    }

    public UUID getOpId() {
        return opId;
    }

    public int getOpType() { return opType; }
    @Override
    public String toString() {
        return "ProposeRequest{" +
                "instance=" + instance +
                ", opId=" + opId +
                ", operation=" + Hex.encodeHexString(operation) +
                '}';
    }
}
