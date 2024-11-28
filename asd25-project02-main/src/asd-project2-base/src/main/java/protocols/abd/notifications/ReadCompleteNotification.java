package protocols.abd.notifications;

import org.apache.commons.codec.binary.Hex;
import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;

import java.nio.ByteBuffer;

public class ReadCompleteNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 203;

    private final long key;
    private final long value;

    public ReadCompleteNotification(long key, long value){
        super(NOTIFICATION_ID);
        this.key = key;
        this.value = value;
    }

    public byte[] getKey() {
        byte [] bytes = ByteBuffer.allocate(8).putLong(key).array();
        return bytes;
    }

    public byte[] getValue() {
        byte [] bytes = ByteBuffer.allocate(8).putLong(value).array();
        return bytes;
    }

    @Override
    public String toString() {
        return "ReadCompleteNotification{" +
                "key=" + key +
                ", value=" + value +
                '}';
    }
}
