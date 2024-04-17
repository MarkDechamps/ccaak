package be.ccaak.kafkatoy.commands;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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
    public void publish(String msg, @ShellOption(valueProvider = TopicValueProvider.class) String topic) throws ExecutionException, InterruptedException {
        System.out.println("Should publish :" + msg + " on topic " + topic);
        var producer = new KafkaProducer<String, String>(Map.of("bootstrap.servers", "localhost:9092",
                "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "value.serializer", "org.apache.kafka.common.serialization.StringSerializer"));
        var result = producer.send(new ProducerRecord<>(topic, msg)).get();
        log.info("Message published: {} ", result);
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
        var configFuture = createTopicsResult.config(name);
        int partitions = createTopicsResult.numPartitions(name).get();
        log.info("Topic created {} with partitions {}!", name, partitions);
        var config = configFuture.get();
        config.entries().forEach(e->{
            log.info("{} - {}",e.type(),e.value());
        });
    }

}
