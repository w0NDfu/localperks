import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/** Sends one exact previously persisted payload into the isolated broker. */
public class ReplayOrder {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("validation-replay-" + args[3]);
        producer.setNamesrvAddr(args[0]);
        producer.setSendMsgTimeout(10000);
        producer.setRetryTimesWhenSendFailed(0);
        producer.start();
        try {
            Message message = new Message(args[1], args[2].getBytes(StandardCharsets.UTF_8));
            SendResult result = producer.send(message);
            System.out.println("VALIDATION_SEND " + result.getSendStatus() + " " + result.getMsgId());
        } finally {
            producer.shutdown();
        }
    }
}
