package be.ccaak.kafkatoy.commands;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

@ShellComponent
@Slf4j
public class ShellCommands {

    private AdminClient admin;

    @PostConstruct
    public void init() {
        log.info("Initializing adminClient");
        admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"));
    }
    @PreDestroy
    public void shutdown() {
        log.info("Closing adminClient");
        admin.close();
    }

    @ShellMethod
    public void publish(String msg, @ShellOption(valueProvider = TopicValueProvider.class) String topic) {
        System.out.println("Should publish :" + msg + " on topic " + topic);
        KafkaProducer producer = new KafkaProducer(Map.of("bootstrap.servers", "localhost:9092",
                "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "value.serializer", "org.apache.kafka.common.serialization.StringSerializer"));
        producer.send(new org.apache.kafka.clients.producer.ProducerRecord(topic, msg));
    }

    @ShellMethod
    public void topics() throws ExecutionException, InterruptedException {
        var topics = admin.listTopics();
        topics.names().get()
                .stream().filter(s->!s.startsWith("_"))
                .forEach(System.out::println);
    }
    @ShellMethod
    public void createTopic(String name) throws ExecutionException, InterruptedException {
        NewTopic newTopic = new NewTopic(name, Optional.empty(),Optional.empty());
        var createTopicsResult = admin.createTopics(List.of(newTopic));
        int partitions = createTopicsResult.numPartitions(name).get();
        log.info("Topic created {} with partitions {}!", name, partitions);
    }

}
