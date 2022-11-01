import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class ActiveMQProduction {
    public static void main(String[] args) throws JMSException{
        //Create ConnectionFactory
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");

        //Create connection
        Connection connection = connectionFactory.createConnection();
        connection.start();

        //Create session from our object connection
        Session session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);

        // Create destination (Topic or Queue)
        Destination destination = session.createQueue("MyQueue");

        //Create Message Producer from session to teh topic
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT); 

        //Create message
        String text = "have a good day !";
        TextMessage message = session.createTextMessage(text);

        //Send message
        producer.send(message);
        System.out.println("200 OK");
        //Clean up
        session.close();
        connection.close();
    }
}
