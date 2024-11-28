package protocols.abd;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.Message;
import protocols.abd.messages.*;
import protocols.abd.notifications.ReadCompleteNotification;
import protocols.abd.requests.ReadRequest;
import protocols.abd.requests.WriteRequest;
import protocols.agreement.IncorrectAgreement;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.requests.ProposeRequest;
import protocols.app.requests.CurrentStateRequest;
import protocols.app.requests.InstallStateRequest;
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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
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
    public static final short PROTOCOL_ID = 201;

    private final Host self;     // My own address/port
    private final int channelId; // Id of the created channel

    private State state;
    private List<Host> membership;
    private int nextInstance;

    private Map<Long, Long> pendingOperations;
    private List<ProtoMessage> answers;
    private Map<Long, Long> tag;
    private Map<Long, Long> val;

    private List<ProtoRequest> bufferOps;

    private final short APP_ID = 300;

    // TODO: Mudar para timestamp
    private long opSeq;

    public ABD(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        nextInstance = 0;

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
        tag = new HashMap<>();
        val = new HashMap<>();
        bufferOps = new LinkedList<>();
        opSeq = 0;
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

        if (initialMembership.contains(self)) {
            state = State.JOINING;
            logger.info("Starting in ACTIVE as I am part of initial membership");
            //I'm part of the initial membership, so I'm assuming the first in the system
            // Ask for the state from the Application
            // TODO: Ver instance
            sendRequest(new CurrentStateRequest(0), APP_ID);
        } else {
            state = State.JOINING;
            logger.info("Starting in JOINING as I am not part of initial membership");
            joinSystem();
            //You have to do something to join the system and know which instance you joined
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
            read(request.getKey());
        }
    }

    private void uponWriteRequest(WriteRequest request, short sourceProto) {
        logger.debug("Received write request: " + request);
        if (state == State.JOINING) {
            bufferOps.add(request);
            //Do something smart (like buffering the requests)
        } else if (state == State.ACTIVE) {
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
        pendingOperations.put(key, v);
        opSeq++;

        // Trigger broadcast
        for (Host h : membership) {
            if (!h.equals(self)) {
                // Send write request
                //sendRequest(new WriteRequest(opSeq, operation), AGREEMENT_PROTOCOL_ID);
                ProtoMessage msg = new ReadTagMsg(opSeq, key);
                openConnection(h);
                sendMessage(msg, h);
            }
        }
    }

    // Upon bebBcastDeliver ( (READ_Tag, id, k), p)
    private void uponReceiveBroadcastWrite(ProtoMessage msg, Host from, short sourceProto, int channelId){

        Long key = ((ReadTagMsg) msg).getKey();

        long tag1 = tag.getOrDefault(key, opSeq);

        long oSeq = ((ReadTagMsg) msg).getOpSeq();

        ProtoMessage msgToSend = new ReadTagRepMsg(oSeq, tag1, key);

        openConnection(from);
        sendMessage(msgToSend, from);
    }

    private void uponReceive (ProtoMessage msg, Host from, short sourceProto, int channelId) {
        long id = (( ReadTagRepMsg ) msg).getOpSeq();

        if (opSeq == id){
            answers.add((ReadTagRepMsg) msg);
            if(answers.size() == membership.size() + 1){
                long newTag = 0;

                for (ProtoMessage msgAux : answers) {
                    if (((ReadTagRepMsg ) msgAux).getTag() > newTag)
                        newTag = ((ReadTagRepMsg ) msgAux).getTag();
                }

                opSeq++;
                answers.clear();

                long key = ((ReadTagRepMsg ) msg).getKey();
                // Send write request
                for (Host h : membership) {
                    if (!h.equals(self)) {
                        WriteTagMsg msgToSend = new WriteTagMsg(opSeq, key, newTag, pendingOperations.get(key));
                        openConnection(h);
                        sendMessage(msgToSend, h);
                    }
                }
                pendingOperations.clear();
            }
        }
    }

    private void uponBebBcastDeliver(ProtoMessage msg, Host from, short sourceProto, int channelId){
        long newTag = ((WriteTagMsg ) msg).getNewTag();
        long key = ((WriteTagMsg ) msg).getKey();
        if(newTag > tag.get(key)){
            tag.remove(key);
            tag.put(key, newTag);
            val.remove(key);
            val.put(key, ((WriteTagMsg ) msg).getNewValue());
        }

        long oSeq = ((WriteTagMsg ) msg).getOpSeq();
        AckMsg ackMsg = new AckMsg(oSeq, key);
        openConnection(from);
        sendMessage(ackMsg, from);

        //TODO: Send Notification to the APP
        triggerNotification(new ExecuteNotification(new UUID(1, 1), new byte[]{1, 2, 3}));
        sendRequest(new InstallStateRequest(new byte[]{1, 2, 3}), APP_ID);
    }

    private void uponAck(ProtoMessage msg, Host from, short sourceProto, int channelId){
        long id = ((AckMsg ) msg).getOpSeq();
        if(opSeq == id){
            answers.add(msg);
            if(answers.size() == membership.size() + 1) {
                answers.clear();
                // TODO: Ver o q tem de ser returnado!!!
                if (pendingOperations.isEmpty())
                    // writeOK
                    //sendReply(new );
                    return;
                else{
                    long key = ((AckMsg ) msg).getKey();
                    long value = val.get(key);
                    triggerNotification(new ReadCompleteNotification(key, value));
                }

            }
            // TODO: Ver dps se faz sentido dar clear ao pending!!
        }

    }

    private void read(long key){
        opSeq++;
        answers.clear();
        for (Host h : membership) {
            if (!h.equals(self)) {
                ReadMsg msgToSend = new ReadMsg(opSeq, key);
                openConnection(h);
                sendMessage(msgToSend, h);
            }
        }
    }

    private void uponReceiveBroadcastRead(ProtoMessage msg, Host from, short sourceProto, int channelId){

        Long key = ((ReadMsg) msg).getKey();

        long tag1 = tag.getOrDefault(key, opSeq);

        long val1 = val.get(key);

        long oSeq = ((ReadMsg) msg).getOpSeq();

        ProtoMessage msgToSend = new ReadReplyMsg(oSeq, key, tag1, val1);

        openConnection(from);
        sendMessage(msgToSend, from);
    }

    private void uponReceiveReadReply(ProtoMessage msg, Host from, short sourceProto, int channelId){
        long id = ((ReadReplyMsg ) msg).getOpSeq();
        if(opSeq == id){
            answers.add(msg);
            if (answers.size() > membership.size() +1){
                long newTag = 0;
                long val1 = Long.MIN_VALUE; // = 0 ?

                for (ProtoMessage msgAux : answers) {
                    // TODO: Maybe a Check related with the type of msg

                    if (((ReadReplyMsg ) msgAux).getTag() > newTag){
                        newTag = ((ReadReplyMsg ) msgAux).getTag();
                        val1 = ((ReadReplyMsg ) msgAux).getVal();
                    }
                }
                long key = ((ReadReplyMsg) msg).getKey();
                pendingOperations.put(key, val1);
                opSeq++;
                answers.clear();
                for (Host h : membership) {
                    if (!h.equals(self)) {
                        WriteTagMsg msgToSend = new WriteTagMsg(opSeq, key, newTag, pendingOperations.get(key));
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
        ProtoMessage msgToSend = new JoinReplyMsg(val, tag);
        openConnection(hostJoining);
        sendMessage(msg, hostJoining);
    }

    private void uponReceiveJoinReply(ProtoMessage msg, Host from, short sourceProto, int channelId) {
        if(State.JOINING == state){
            Map<Long, Long> val = ((JoinReplyMsg) msg).getVal();
            Map<Long, Long> tag = ((JoinReplyMsg) msg).getTag();
            this.val.putAll(val);
            this.tag.putAll(tag);
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
        // EXIT
    }

    private void uponReceiveLeave(ProtoMessage msg, Host from, short sourceProto, int channelId) {
        Host hostLeaving = ((LeaveMsg) msg).getLeaving();
        membership.remove(hostLeaving);
    }

}