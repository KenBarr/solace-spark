package events;



import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.Context;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.InvalidPropertiesException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;

import lombok.extern.slf4j.Slf4j;

import payment.Debit;

@Slf4j
public class Broker {
    private XMLMessageProducer prod;
    private JCSMPSession session;
    private Topic topic;
	String queueName = "payment/card";


    public Broker() throws Exception {
    	log.info("Broker");
        // Create a JCSMP Session
        String uri = "tcp://vmr-mr3e5sq7dacxp.messaging.solace.cloud:20480";
        String username = "solace-cloud-client";
        String password = "cge4fi7lj67ms6mnn2b4pe76g2";
        String vpn = "msgvpn-8ksiwsp0mtv";

        final JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, uri);     // host:port
        properties.setProperty(JCSMPProperties.USERNAME, username); // client-username
        properties.setProperty(JCSMPProperties.VPN_NAME,  vpn); // message-vpn
        properties.setProperty(JCSMPProperties.PASSWORD, password); // client-password
        session =  JCSMPFactory.onlyInstance().createSession(properties);
        session.connect();
        
        this.setReceiver();
        
//        topic = JCSMPFactory.onlyInstance().createTopic("a/b");
//
//        /** Anonymous inner-class for handling publishing events */
//        prod = session.getMessageProducer(new JCSMPStreamingPublishEventHandler() {
//            @Override
//            public void responseReceived(String messageID) {
//                System.out.println("Producer received response for msg: " + messageID);
//            }
//            @Override
//            public void handleError(String messageID, JCSMPException e, long timestamp) {
//                System.out.printf("Producer received error for msg: %s@%s - %s%n",
//                        messageID,timestamp,e);
//            }
//        });

    }

    private void setReceiver() throws Exception {
    	    	
    	ConsumerFlowProperties flow_prop = new ConsumerFlowProperties();
		Queue listenQueue = JCSMPFactory.onlyInstance().createQueue(this.queueName);

    	flow_prop.setEndpoint(listenQueue);
    	flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
    	
    	EndpointProperties endpoint_props = new EndpointProperties();
    	endpoint_props.setAccessType(EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);

    	Context defaultContext = JCSMPFactory.onlyInstance().getDefaultContext();
    	
    	Context context = JCSMPFactory.onlyInstance().createContext(null);
    	
    	
    	FlowReceiver cons = this.session.createFlow(
    			new XMLMessageListener() {
    	            @Override
    	            public void onReceive(BytesXMLMessage msg) {
	                    System.out.println("Message received.");
	                    byte[] bytes = msg.getBytes();
	                    try {
		                    Debit d = helper.deserialize(b);
		                    System.out.println("====+++++====");
		                    System.out.println(d);
		                    System.out.println("====+++++====");
		                    
		                    
	    	                // When the ack mode is set to SUPPORTED_MESSAGE_ACK_CLIENT,
	    	                // guaranteed delivery messages are acknowledged after
	    	                // processing
	    	                msg.ackMessage();
	                    	
	                    }
	                    catch (Exception ex) {
	                    	log.error("Exception in desetalizing: " + ex.getMessage());
	                    }
	                    
    	            }

    	            @Override
    	            public void onException(JCSMPException e) {
    	                System.out.printf("Consumer received exception: %s%n", e);
    	            }
    	        }
    			, flow_prop, endpoint_props);  

    	cons.start();
    	log.info("Listening for messages: "+ this.queueName);    	
    }
    public void sendMessage() throws JCSMPException {
        // Publish-only session is now hooked up and running!

        TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
        final String text = "Hello world!";
        msg.setText(text);
        System.out.printf("Connected. About to send message '%s' to topic '%s'...%n",text,topic.getName());
        prod.send(msg,topic);
        log.info("Message sent to topic " + topic.getName());

    }

    public void sendMessage(byte[] bytes) throws JCSMPException {
        // Publish-only session is now hooked up and running!

        BytesMessage msg = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
        msg.setData(bytes);
        System.out.printf("Connected. About to send message to topic '%s'...%n",topic.getName());
        prod.send(msg,topic);
        System.out.println("Message sent. Exiting.");

    }


    @Override
    protected void finalize() throws Throwable {
        session.closeSession();
        super.finalize();
    }


}