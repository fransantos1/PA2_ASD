package protocols.abd;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.Message;
import protocols.abd.messages.*;
import protocols.abd.notifications.ReadCompleteNotification;
import protocols.abd.notifications.UpdateValueNotification;
import protocols.abd.notifications.WriteCompleteNotification;
import protocols.abd.requests.ReadRequest;
import protocols.abd.requests.WriteRequest;
import protocols.agreement.IncorrectAgreement;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.requests.ProposeRequest;
import protocols.app.requests.*;
import protocols.app.utils.Operation;
import protocols.statemachine.notifications.ChannelReadyNotification;
import protocols.statemachine.notifications.ExecuteNotification;
import protocols.statemachine.requests.OrderRequest;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.*;

/**
 *
 */
public class ABD extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(ABD.class);

    private enum State {JOINING, ACTIVE}

    //Protocol information, to register in babel
    public static final String PROTOCOL_NAME = "ABD";
    public static final short PROTOCOL_ID = 100;

    private final Host self;     // My own address/port
    private final int channelId; // Id of the created channel

    private State state;
    private List<Host> membership;

    private Map<Long, Long> pendingOperations;
    private List<ProtoMessage> answers;
    //private Map<Long, Long> tag;
    private Map<Long, ABDInstance> val;

    private List<ProtoRequest> bufferOps;
    private Map<Long, UUID> uuidBuffer;

    private final short APP_ID = 300;

    private int executedOps;

    //private long opSeq;

    public ABD(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        //nextInstance = 0;

        String address = props.getProperty("address");
        String port = props.getProperty("p2p_port");

        logger.info("Listening on {}:{}", address, port);
        this.self = new Host(InetAddress.getByName(address), Integer.parseInt(port));

        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, address);
        channelProps.setProperty(TCPChannel.PORT_KEY, port); //The port to bind to
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000");
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000");
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000");
        channelId = createChannel(TCPChannel.NAME, channelProps);

        /*-------------------- Register Channel Events ------------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(ReadRequest.REQUEST_ID, this::uponReadRequest);
        registerRequestHandler(WriteRequest.REQUEST_ID, this::uponWriteRequest);
        registerRequestHandler(CurrentStateRequest.REQUEST_ID, this::uponStateReq);
        registerReplyHandler(CurrentStateReply.REQUEST_ID, this::uponStateReply);

        /*--------------------- Register Notification Handlers ------------------------ */
        //subscribeNotification(DecidedNotification.NOTIFICATION_ID, this::uponDecidedNotification);

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channelId, ReadTagMsg.MSG_ID, ReadTagMsg.serializer);
        registerMessageHandler(channelId, ReadTagMsg.MSG_ID, this::uponReceiveBroadcastWrite, this::uponMsgFail);

        registerMessageSerializer(channelId, ReadTagRepMsg.MSG_ID, ReadTagRepMsg.serializer);
        registerMessageHandler(channelId, ReadTagRepMsg.MSG_ID, this::uponReceive, this::uponMsgFail);

        registerMessageSerializer(channelId, WriteTagMsg.MSG_ID, WriteTagMsg.serializer);
        registerMessageHandler(channelId, WriteTagMsg.MSG_ID, this::uponBebBcastDeliver, this::uponMsgFail);

        registerMessageSerializer(channelId, AckMsg.MSG_ID, AckMsg.serializer);
        registerMessageHandler(channelId, AckMsg.MSG_ID, this::uponAck, this::uponMsgFail);

        registerMessageSerializer(channelId, ReadMsg.MSG_ID, ReadMsg.serializer);
        registerMessageHandler(channelId, ReadMsg.MSG_ID, this::uponReceiveBroadcastRead, this::uponMsgFail);

        registerMessageSerializer(channelId, ReadReplyMsg.MSG_ID, ReadReplyMsg.serializer);
        registerMessageHandler(channelId, ReadReplyMsg.MSG_ID, this::uponReceiveReadReply, this::uponMsgFail);

        registerMessageSerializer(channelId, JoinMsg.MSG_ID, JoinMsg.serializer);
        registerMessageHandler(channelId, JoinMsg.MSG_ID, this::uponReceiveJoin, this::uponMsgFail);

        registerMessageSerializer(channelId, JoinReplyMsg.MSG_ID, JoinReplyMsg.serializer);
        registerMessageHandler(channelId, JoinReplyMsg.MSG_ID, this::uponReceiveJoinReply, this::uponMsgFail);

        registerMessageSerializer(channelId, LeaveMsg.MSG_ID, LeaveMsg.serializer);
        registerMessageHandler(channelId, LeaveMsg.MSG_ID, this::uponReceiveLeave, this::uponMsgFail);

        pendingOperations = new HashMap<>();
        answers = new LinkedList<>();
        //tag = new HashMap<>();
        val = new HashMap<>();
        bufferOps = new LinkedList<>();
        uuidBuffer = new HashMap<>();
        //opSeq = 0;
        executedOps = 0;
    }

    @Override
    public void init(Properties props) {
        //Inform the state machine protocol about the channel we created in the constructor
        triggerNotification(new ChannelReadyNotification(channelId, self));

        String host = props.getProperty("initial_membership");
        String[] hosts = host.split(",");
        List<Host> initialMembership = new LinkedList<>();
        for (String s : hosts) {
            String[] hostElements = s.split(":");
            Host h;
            try {
                h = new Host(InetAddress.getByName(hostElements[0]), Integer.parseInt(hostElements[1]));
            } catch (UnknownHostException e) {
                throw new AssertionError("Error parsing initial_membership", e);
            }
            initialMembership.add(h);
        }
        logger.info(initialMembership.toString());
        if (initialMembership.contains(self)) {
            state = State.JOINING;
            logger.info("Starting in ACTIVE as I am part of initial membership");
            //I'm part of the initial membership, so I'm assuming the first in the system
            // Ask for the state from the Application
            sendRequest(new CurrentStateRequest(0), APP_ID);
        } else {
            state = State.JOINING;
            logger.info("Starting in JOINING as I am not part of initial membership");
            joinSystem();
            // You have to do something to join the system and know which instance you joined
            // (and copy the state of that instance)
        }
    }

    /* --------------------------------- Requests ---------------------------------------- */
    private void uponReadRequest(ReadRequest request, short sourceProto) {
        logger.debug("Received read request: " + request);
        if (state == State.JOINING) {
            bufferOps.add(request);
            //Do something smart (like buffering the requests)
        } else if (state == State.ACTIVE) {
            // Definir para ver se é r
            uuidBuffer.put(request.getKey(), request.getOpId());
            read(request.getKey());
        }
    }

    private void uponWriteRequest(WriteRequest request, short sourceProto) {
        logger.debug("Received write request: " + request);
        if (state == State.JOINING) {
            bufferOps.add(request);
            //Do something smart (like buffering the requests)
        } else if (state == State.ACTIVE) {
            uuidBuffer.put(request.getKey(), request.getOpId());
            write(request.getKey(), request.getValue());
        }
    }

    /* --------------------------------- Notifications ---------------------------------------- */


    /* --------------------------------- Messages ---------------------------------------- */
    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    /* --------------------------------- TCPChannel Events ---------------------------- */
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        logger.info("Connection to {} is up", event.getNode());
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        logger.debug("Connection to {} is down, cause {}", event.getNode(), event.getCause());
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        logger.debug("Connection to {} failed, cause: {}", event.getNode(), event.getCause());
        //Maybe we don't want to do this forever. At some point we assume he is no longer there.
        //Also, maybe wait a little bit before retrying, or else you'll be trying 1000s of times per second
        if(membership.contains(event.getNode()))
            openConnection(event.getNode());
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.trace("Connection from {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.trace("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
    }

    /* --------------------------------- Functions ---------------------------- */

    private void write(long key, long v) {
        //You should write the operation to the state machine
        val.get(key).addOneOpSeq();
        pendingOperations.put(key, v);
        // Trigger broadcast
        for (Host h : membership) {
            if (!h.equals(self)) {
                // Send write request
                //sendRequest(new WriteRequest(opSeq, operation), AGREEMENT_PROTOCOL_ID);
                ProtoMessage msg = new ReadTagMsg(val.get(key).getOpSeq(), key);
                openConnection(h);
                sendMessage(msg, h);
            }
        }
    }

    // Upon bebBcastDeliver ( (READ_Tag, id, k), p)
    private void uponReceiveBroadcastWrite(ProtoMessage msg, Host from, short sourceProto, int channelId){

        long key = ((ReadTagMsg) msg).getKey();

        long tag1l = val.get(key).getTagLeft();
        long tag1r = val.get(key).getTagRight();

        long oSeq = ((ReadTagMsg) msg).getOpSeq();

        ProtoMessage msgToSend = new ReadTagRepMsg(oSeq, tag1l, tag1r, key);

        openConnection(from);
        sendMessage(msgToSend, from);
    }

    private void uponReceive (ProtoMessage msg, Host from, short sourceProto, int channelId) {
        long id = (( ReadTagRepMsg ) msg).getOpSeq();

        if (val.get(((ReadTagRepMsg ) msg).getKey()).getOpSeq() == id){
            answers.add((ReadTagRepMsg) msg);
            if(answers.size() == membership.size() + 1){
                long newTagl = 0;
                long newTagr = 0;

                for (ProtoMessage msgAux : answers) {
                    if (((ReadTagRepMsg ) msgAux).getTagl() > newTagl || ( ((ReadTagRepMsg ) msgAux).getTagl() == newTagl) && ((ReadTagRepMsg ) msgAux).getTagr() > newTagr){
                        newTagl = ((ReadTagRepMsg ) msgAux).getTagl();
                        newTagr = ((ReadTagRepMsg ) msgAux).getTagr();
                    }
                }

                val.get(((ReadTagRepMsg ) msg)).addOneOpSeq();
                answers.clear();
                long key = ((ReadTagRepMsg ) msg).getKey();
                // Send write request
                for (Host h : membership) {
                    if (!h.equals(self)) {
                        WriteTagMsg msgToSend = new WriteTagMsg(val.get(((ReadTagRepMsg ) msg).getKey()).getOpSeq(), uuidBuffer.get(key), key, newTagl, newTagr, pendingOperations.get(key));
                        openConnection(h);
                        sendMessage(msgToSend, h);
                    }
                }
                pendingOperations.clear();
            }
        }
    }

    private void uponBebBcastDeliver(ProtoMessage msg, Host from, short sourceProto, int channelId){
        long newTagl = ((WriteTagMsg ) msg).getNewTagl();
        long newTagr = ((WriteTagMsg ) msg).getNewTagr();
        long key = ((WriteTagMsg ) msg).getKey();
        if(val.get(key).getTagLeft() < newTagl ||  ( val.get(key).getTagLeft() == newTagl && val.get(key).getTagRight() < newTagr) ){
            val.get(key).setTag(newTagl, newTagr);
            val.get(key).setVal(((WriteTagMsg ) msg).getNewValue());
            triggerNotification(new UpdateValueNotification(((WriteTagMsg ) msg).getUuid(), key, ((WriteTagMsg ) msg).getNewValue()));
            executedOps++;
        }

        long oSeq = ((WriteTagMsg ) msg).getOpSeq();
        AckMsg ackMsg = new AckMsg(oSeq, key);
        openConnection(from);
        sendMessage(ackMsg, from);
    }

    private void uponAck(ProtoMessage msg, Host from, short sourceProto, int channelId){
        long id = ((AckMsg ) msg).getOpSeq();
        if(val.get(((AckMsg ) msg).getKey()).getOpSeq() == id){
            answers.add(msg);
            if(answers.size() == membership.size() + 1) {
                answers.clear();
                // TODO: When testing, check if this is good or bad
                val.get(((AckMsg ) msg).getKey()).addOneOpSeq();
                executedOps++;
                if (pendingOperations.isEmpty()){
                    long key = ((AckMsg ) msg).getKey();
                    long value = val.get(key).getVal();
                    triggerNotification(new WriteCompleteNotification(uuidBuffer.get(key), key, value));
                    uuidBuffer.remove(key);
                }
                else{
                    long key = ((AckMsg ) msg).getKey();
                    long value = val.get(key).getVal();
                    triggerNotification(new ReadCompleteNotification(uuidBuffer.get(key), key, value));
                    uuidBuffer.remove(key);
                }

            }
            // TODO: Ver dps se faz sentido dar clear ao pending!!
        }

    }

    private void read(long key){
        val.get(key).addOneOpSeq();
        answers.clear();
        for (Host h : membership) {
            if (!h.equals(self)) {
                ReadMsg msgToSend = new ReadMsg(val.get(key).getOpSeq(), key);
                openConnection(h);
                sendMessage(msgToSend, h);
            }
        }
    }

    private void uponReceiveBroadcastRead(ProtoMessage msg, Host from, short sourceProto, int channelId){

        Long key = ((ReadMsg) msg).getKey();

        long tagl1 = val.get(key).getTagLeft();
        long tagr1 = val.get(key).getTagRight();

        long val1 = val.get(key).getVal();

        long oSeq = ((ReadMsg) msg).getOpSeq();

        ProtoMessage msgToSend = new ReadReplyMsg(oSeq, key, tagl1, tagr1, val1);

        openConnection(from);
        sendMessage(msgToSend, from);
    }

    private void uponReceiveReadReply(ProtoMessage msg, Host from, short sourceProto, int channelId){
        long id = ((ReadReplyMsg ) msg).getOpSeq();
        if(val.get(((ReadReplyMsg ) msg).getKey()).getOpSeq() == id){
            answers.add(msg);
            if (answers.size() > membership.size() +1){
                long newTagl = 0;
                long newTagr = 0;
                long val1 = Long.MIN_VALUE; // = 0 ?

                for (ProtoMessage msgAux : answers) {
                    if (((ReadReplyMsg ) msgAux).getTagl() > newTagl || ( ((ReadReplyMsg ) msgAux).getTagl() == newTagl) && ((ReadReplyMsg ) msgAux).getTagr() > newTagr){
                        newTagl = ((ReadReplyMsg ) msgAux).getTagl();
                        newTagr = ((ReadReplyMsg ) msgAux).getTagr();
                        val1 = ((ReadReplyMsg ) msgAux).getVal();
                    }
                }
                long key = ((ReadReplyMsg) msg).getKey();
                // TODO: CHECK THIS LINE
                pendingOperations.put(key, val1);
                val.get(((ReadReplyMsg ) msg).getKey()).addOneOpSeq();
                answers.clear();
                for (Host h : membership) {
                    if (!h.equals(self)) {
                        WriteTagMsg msgToSend = new WriteTagMsg(val.get(((ReadReplyMsg) msg).getKey()).getOpSeq(), uuidBuffer.get(key), key, newTagl, newTagr, pendingOperations.get(key));
                        openConnection(h);
                        sendMessage(msgToSend, h);
                    }
                }
            }
        }
    }

    // Join the system and exit it
    // and
    // Add and remove a replica

    private void joinSystem() {
        for (Host h : membership) {
            if (!h.equals(self)) {
                // Send join request
                ProtoMessage msg = new JoinMsg(self);
                openConnection(h);
                sendMessage(msg, h);
            }
        }
    }

    private void uponReceiveJoin(ProtoMessage msg, Host from, short sourceProto, int channelId) {
        Host hostJoining = ((JoinMsg) msg).getHostJoining();
        membership.add(hostJoining);
        ProtoMessage msgToSend = new JoinReplyMsg(val);
        openConnection(hostJoining);
        sendMessage(msg, hostJoining);
    }

    private void uponReceiveJoinReply(ProtoMessage msg, Host from, short sourceProto, int channelId) {
        if(State.JOINING == state){
            Map<Long, ABDInstance> val = ((JoinReplyMsg) msg).getVal();
            for(int i = 0; i < val.size(); i++){

            }
            this.val.clear();
            this.val.putAll(val);
            state = State.ACTIVE;
        }
    }

    private void leave(){
        for (Host h : membership) {
            if (!h.equals(self)) {
                ProtoMessage msg = new LeaveMsg(self);
                openConnection(h);
                sendMessage(msg, h);
            }
        }
    }

    private void uponReceiveLeave(ProtoMessage msg, Host from, short sourceProto, int channelId) {
        Host hostLeaving = ((LeaveMsg) msg).getLeaving();
        membership.remove(hostLeaving);
    }

    private void uponStateReq(CurrentStateRequest request, short sourceProto){
        try{
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    DataOutputStream dos = new DataOutputStream(baos);
                    dos.writeInt(executedOps);
                    // TODO: NOT SURE
                    dos.writeInt(5);
                    dos.write(new byte[]{1, 2, 3, 4, 5});
                    dos.writeInt(val.size());
                    for (Map.Entry<Long, ABDInstance> entry : val.entrySet()) {
                        dos.writeUTF(entry.getKey().toString());
                        byte [] bytes = ByteBuffer.allocate(8).putLong(entry.getValue().getVal()).array();
                        dos.writeInt(bytes.length);
                        dos.write(bytes);
                    }
                    sendRequest(new InstallStateRequest(baos.toByteArray()), sourceProto);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    //private void askForState(){
    //    sendRequest(new CurrentStateRequest());
    //}

    private void uponStateReply(CurrentStateReply reply, short sourceProto) {
        try {
            val.clear();
            ByteArrayInputStream bais = new ByteArrayInputStream(reply.getState());
            DataInputStream dis = new DataInputStream(bais);
            executedOps = dis.readInt();
            byte [] cumulativeHash = new byte[dis.readInt()];
            dis.read(cumulativeHash);
            int mapSize = dis.readInt();
            for (int i = 0; i < mapSize; i++) {
                String key = dis.readUTF();
                byte[] value = new byte[dis.readInt()];
                dis.read(value);
                val.put(Long.parseLong(key), new ABDInstance(i, Long.parseLong(key), convertToLong(value)));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static long convertToLong(byte[] bytes) {
        long value = 0l;
        // Iterating through for loop
        for (byte b : bytes) {
            // Shifting previous value 8 bits to right and
            // add it with next value
            value = (value << 8) + (b & 255);
        }
        return value;
    }
}