package aggregator;

import com.rabbitmq.client.AMQP.*;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import connector.IRabbitMQConnector;
import connector.RabbitMQConnector;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.namespace.QName;
import models.LoanResponse;
import utilities.MessageUtility;

public class Aggregator {

    private final IRabbitMQConnector connector ;
    private Channel channel;
    private String queueName;
    private final String EXCHANGENAME = "whatAggrigator";
    private final String LOANBROKEREXCHANGE = "whatLoanBroker";
    private final MessageUtility util = new MessageUtility();
    private Map<String, Map<Integer, String>> replyMap;//<corrId,<messageNo,response>>
    private List<String> sentCorrIDs;
private Consumer consumer;

  public Aggregator(IRabbitMQConnector con, Consumer cons){
  connector=con;
  consumer=cons;
  }
    public Aggregator(){
     connector= new RabbitMQConnector();
    }
    //initialize Aggregator
    public void init() throws IOException {
  
        channel = connector.getChannel();
        channel.exchangeDeclare(EXCHANGENAME, "direct");
        queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGENAME, "");
        replyMap = new HashMap();
        receive();
        sentCorrIDs = new ArrayList();
    }

    private LoanResponse getBestResult(Map<Integer, String> map) throws JAXBException {
        LoanResponse bestRes = null;

        for (Map.Entry<Integer, String> entry : map.entrySet()) {
            String bodyString = entry.getValue();
            LoanResponse res = unmarchal(bodyString);
            if (bestRes == null || bestRes.getInterestRate() > res.getInterestRate()) {
                bestRes = res;
            }
        }
        return bestRes;
    }

    private Map<String, Map<Integer, String>> handleMessage(String bodyString, BasicProperties prop) throws JAXBException, IOException {
        String corrId = prop.getCorrelationId();
        int total = (int) prop.getHeaders().get("total");
        int messageNo = (int) prop.getHeaders().get("messageNo");
        if (replyMap.containsKey(corrId)) {
            Map<Integer, String> map = replyMap.get(corrId);
            map.put(messageNo, bodyString);
            replyMap.put(corrId, map);
        }
        else {
            Map<Integer, String> contentMap = new HashMap();
            contentMap.put(messageNo, bodyString);
            replyMap.put(corrId, contentMap);
        }
        if (total == replyMap.get(corrId).size() && !sentCorrIDs.contains(corrId)) {
            LoanResponse bestResult = getBestResult(replyMap.get(corrId));
            send(prop, bestResult);
            System.out.println("sending corrId: " + corrId + " ssn: " + bestResult.getSsn() + " interestRate:" + bestResult.getInterestRate());
            sentCorrIDs.add(corrId);
        }
        return replyMap;
    }

    //Waiting asynchronously for messages
    public boolean receive() throws IOException {

        System.out.println(" [*] Waiting for messages.");
          consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) throws IOException {
                try {
                    String bodyString = removeBom(new String(body));
                    handleMessage(bodyString, properties);
                }
                catch (JAXBException ex) {
                    Logger.getLogger(Aggregator.class.getName()).log(Level.SEVERE, null, ex);
                }
                finally {
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            }
        };
        channel.basicConsume(queueName, false, consumer);
        return true;
    }

    //unmarshal from string to Object
    private LoanResponse unmarchal(String bodyString) throws JAXBException {
        JAXBContext jc = JAXBContext.newInstance(LoanResponse.class);
        Unmarshaller unmarshaller = jc.createUnmarshaller();
        StringReader reader = new StringReader(bodyString);
        return (LoanResponse) unmarshaller.unmarshal(reader);
    }

    //marshal from pbkect to xml string
    private String marchal(LoanResponse d) throws JAXBException {
        JAXBContext jc2 = JAXBContext.newInstance(LoanResponse.class);
        Marshaller marshaller = jc2.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        JAXBElement<LoanResponse> je2 = new JAXBElement(new QName("LoanResponse"), LoanResponse.class, d);
        StringWriter sw = new StringWriter();
        marshaller.marshal(je2, sw);

        return removeBom(sw.toString());
    }

    //remove unnecessary charactors before xml declaration 
    private String removeBom(String bodyString) {
        String res = bodyString.trim();
        int substringIndex = res.indexOf("<?xml");
        if (substringIndex < 0) {
            return res;
        }
        return res.substring(res.indexOf("<?xml"));
    }

    //build a new property for messaging
    //send message to exchange
    public boolean send(BasicProperties prop, LoanResponse data) throws IOException, JAXBException {

        //creating data for sending
        //send message to each bank in the banklist. 
        String xmlString = marchal(data);
        byte[] body = util.serializeBody(xmlString);

        System.out.println("sending from Aggregator to " + LOANBROKEREXCHANGE + " : " + xmlString);
        channel.basicPublish(LOANBROKEREXCHANGE, "", prop, body);
        return true;
    }

}
