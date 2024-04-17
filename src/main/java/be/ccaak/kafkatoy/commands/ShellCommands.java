package be.ccaak.kafkatoy.commands;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.util.Map;
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
        KafkaProducer producer;
    }

    @ShellMethod
    public void topics() throws ExecutionException, InterruptedException {
        var topics = admin.listTopics();
        topics.names().get()
                .stream().filter(s->!s.startsWith("_"))
                .forEach(System.out::println);
    }
}
