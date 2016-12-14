
import aggregator.Aggregator;
import com.rabbitmq.client.AMQP.*;
import com.rabbitmq.client.AMQP.Queue.DeclareOk;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import connector.IRabbitMQConnector;
import connector.RabbitMQConnector;
import java.io.IOException;
import javax.xml.bind.JAXBException;
import models.LoanResponse;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;

/**
 *
 * @author Tomoe
 * This is a trial to test using Mockito to mock out RabbitMQ part.
 * 
 */
public class AggregatorTest {

    private IRabbitMQConnector mockCon;
    private Channel mockChan;
    private DeclareOk mockDeclareOk;
    private Aggregator agg;
    private Consumer mockConsumer;

    @Before
    public void setUp() throws IOException {

        mockCon = mock(RabbitMQConnector.class);
        mockConsumer = mock(com.rabbitmq.client.Consumer.class);
        agg = new Aggregator(mockCon, mockConsumer);
        mockChan = mock(com.rabbitmq.client.Channel.class);
        mockDeclareOk = mock(com.rabbitmq.client.AMQP.Queue.DeclareOk.class);

        when(mockCon.getChannel()).thenReturn(mockChan);
        when(mockChan.queueDeclare()).thenReturn(mockDeclareOk);
        when(mockDeclareOk.getQueue()).thenReturn("testQuename");

        agg.init();
    }

    @Test
    public void testInit() throws IOException {
        
        verify(mockCon, atLeast(1)).getChannel();
        verify(mockChan, atLeast(1)).exchangeDeclare(anyString(), anyString());
        verify(mockChan, atLeast(1)).basicConsume(anyString(), anyBoolean(), (Consumer) anyObject());
    }

    @Test
    public void testSend() throws IOException, JAXBException {
        BasicProperties prop = null;
        LoanResponse data = new LoanResponse("123456-4444", 1.4);
        agg.send(prop, data);
        verify(mockChan, atLeast(1)).basicPublish(anyString(), anyString(), anyObject(), anyObject());

    }
}
