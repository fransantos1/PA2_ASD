package protocols.abd;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.Message;
import protocols.abd.messages.ReadTagMsg;
import protocols.abd.messages.ReadTagRepMsg;
import protocols.agreement.IncorrectAgreement;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.requests.ProposeRequest;
import protocols.app.utils.Operation;
import protocols.statemachine.notifications.ChannelReadyNotification;
import protocols.statemachine.notifications.ExecuteNotification;
import protocols.statemachine.requests.OrderRequest;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * This is NOT fully functional StateMachine implementation.
 * This is simply an example of things you can do, and can be used as a starting point.
 *
 * You are free to change/delete anything in this class, including its fields.
 * The only thing that you cannot change are the notifications/requests between the StateMachine and the APPLICATION
 * You can change the requests/notification between the StateMachine and AGREEMENT protocol, however make sure it is
 * coherent with the specification shown in the project description.
 *
 * Do not assume that any logic implemented here is correct, think for yourself!
 */
public class ABD extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(ABD.class);

    private enum State {JOINING, ACTIVE}

    //Protocol information, to register in babel
    public static final String PROTOCOL_NAME = "ABD";
    public static final short PROTOCOL_ID = 201;

    private final Host self;     //My own address/port
    private final int channelId; //Id of the created channel

    private State state;
    private List<Host> membership;
    private int nextInstance;

    // Op Type:
    // 0 - Read
    // 1 - Write
    private List<Operation> pendingOperations;
    private List<ReadTagRepMsg> answers;
    private Map<Integer, String> tag;
    private Map<Integer, byte[]> val;

    // TODO: Mudar para timestamp
    private int opSeq;


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
        registerRequestHandler(OrderRequest.REQUEST_ID, this::uponOrderRequest);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(DecidedNotification.NOTIFICATION_ID, this::uponDecidedNotification);


        pendingOperations = new LinkedList<>();
        answers = new LinkedList<>();
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
            state = State.ACTIVE;
            logger.info("Starting in ACTIVE as I am part of initial membership");
            //I'm part of the initial membership, so I'm assuming the system is bootstrapping
            membership = new LinkedList<>(initialMembership);
            membership.forEach(this::openConnection);
            triggerNotification(new JoinedNotification(membership, 0));
        } else {
            state = State.JOINING;
            logger.info("Starting in JOINING as I am not part of initial membership");
            //You have to do something to join the system and know which instance you joined
            // (and copy the state of that instance)
        }

    }

    /*--------------------------------- Requests ---------------------------------------- */
    private void uponOrderRequest(OrderRequest request, short sourceProto) {
        logger.debug("Received request: " + request);
        if (state == State.JOINING) {
            //Do something smart (like buffering the requests)
        } else if (state == State.ACTIVE) {
            //Also do something starter, we don't want an infinite number of instances active
        	//Maybe you should modify what is it that you are proposing so that you remember that this
        	//operation was issued by the application (and not an internal operation, check the uponDecidedNotification)
            sendRequest(new ProposeRequest(nextInstance++, request.getOpId(), request.getOperation()),
                    IncorrectAgreement.PROTOCOL_ID);
        }
    }

    /*--------------------------------- Notifications ---------------------------------------- */
    private void uponDecidedNotification(DecidedNotification notification, short sourceProto) {
        logger.debug("Received notification: " + notification);
        //Maybe we should make sure operations are executed in order?
        //You should be careful and check if this operation if an application operation (and send it up)
        //or if this is an operations that was executed by the state machine itself (in which case you should execute)
        triggerNotification(new ExecuteNotification(notification.getOpId(), notification.getOperation()));
    }

    /*--------------------------------- Messages ---------------------------------------- */
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

    private void writeOperation(Operation operation) {
        //You should write the operation to the state machine
        pendingOperations.add(operation);
        opSeq++;

        // Trigger broadcast
        for (Host h : membership) {
            if (!h.equals(self)) {
                // Send write request
                //sendRequest(new WriteRequest(opSeq, operation), AGREEMENT_PROTOCOL_ID);
                ProtoMessage msg = new ReadTagMsg(opSeq, operation.getKey());
                openConnection(h);
                sendMessage(msg, h);
            }
        }
    }

    private void readOperation(Operation operation) {
        //You should read the operation from the state machine
        // TODO: DO LATER
    }

    // Upon bebBcastDeliver (READ_Tag, id, k ,     p)
    private void uponReceiveBroadcast(ProtoMessage msg, Host from, short sourceProto, int channelId){

        String aux = ((ReadTagMsg) msg).getKey();
        String tsSend;
        if (!tag.containsKey(aux))
            tsSend = "0";
        else
            tsSend = tag.get(aux);

        ProtoMessage msgToSend = new ReadTagRepMsg(((ReadTagMsg) msg).getOpSeq(), tsSend);

        openConnection(from);
        sendMessage(msgToSend, from);
    }

    private void uponReceive(ProtoMessage msg, Host from, short sourceProto, int channelId){
        int opSeqMsg = ((ReadTagRepMsg) msg).getOpSeq();
        if (opSeqMsg == opSeq) {
            answers.add((ReadTagRepMsg) msg);
            if (answers.size() == membership.size() + 1) {
                // Get the tag with the highest timestamp
                int newTag = 0;

                for (ReadTagRepMsg msgAux : answers) {
                    if (Integer.parseInt(msgAux.getTag()) > newTag)
                        newTag = Integer.parseInt(msgAux.getTag());
                }
                opSeq++;
                answers.clear();
                // Send write request
                for (Host h : membership) {
                    if (!h.equals(self)) {
                        //ProtoMessage msgToSend = new ReadTagRepMsg(opSeq, maxTag);
                        openConnection(h);
                        //sendMessage(msgToSend, h);
                    }
                }
                pendingOperations.clear();
            }
        }
    }




}
