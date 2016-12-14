/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package connector;

import com.rabbitmq.client.Channel;
import java.io.IOException;

/**
 *
 * @author Tomoe
 */
public interface IRabbitMQConnector {
    Channel getChannel() throws IOException;
    void close(Channel channel)throws IOException ;
}
