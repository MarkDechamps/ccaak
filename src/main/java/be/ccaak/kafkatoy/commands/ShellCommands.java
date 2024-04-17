package be.ccaak.kafkatoy.commands;

import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import be.kafka.helloworld;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

@ShellComponent
@Slf4j
public class ShellCommands {

    private AdminClient admin;

    @PostConstruct
    public void init() {
        log.info("Initializing adminClient");
        admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, "localhost:9093"));
    }

    @PreDestroy
    public void shutdown() {
        log.info("Closing adminClient");
        admin.close();
    }

    @ShellMethod
    public void publish(String msg, @ShellOption(valueProvider = TopicValueProvider.class) String topic, @ShellOption(defaultValue = "1") Integer amount) {
        System.out.println("Should publish :" + msg + " on topic " + topic);
        try (var producer = new KafkaProducer<String, String>(producerConfig())) {
            IntStream.rangeClosed(0, amount).forEach(i -> {
                try {
                    var result = producer.send(new ProducerRecord<>(topic, msg)).get();
                    log.info("Message published: {} ", result);
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }

            });
        }
    }

    private static Map<String, Object> producerConfig() {
        return Map.of("bootstrap.servers", "localhost:9092",
                "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "value.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "batch.size", "100"
        );
    }

    private static Map<String, Object> avroProducerConfig() {
        return Map.of(
                "bootstrap.servers", "localhost:9092",
                "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer",
                "schema.registry.url", "http://localhost:8081"
        );
    }

    private static Map<String, Object> consumerConfig(String groupId) {
        return Map.of("bootstrap.servers", "localhost:9092",
                "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                //    "auto.offset.reset","earliest",
                "group.id", groupId);
    }

    private static Map<String, Object> avroConsumerConfig(String groupId) {
        return Map.of("bootstrap.servers", "localhost:9092",
                "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer",
                "schema.registry.url", "http://localhost:8081",
                "specific.avro.reader", "true",
                "auto.offset.reset","earliest",
                "group.id", groupId);
    }

    @ShellMethod
    public void topics() throws ExecutionException, InterruptedException {
        var topics = admin.listTopics();
        topics.names().get()
                .stream().filter(s -> !s.startsWith("_"))
                .forEach(System.out::println);
    }

    @ShellMethod
    public void createTopic(String name, @ShellOption(defaultValue = "__NULL__") Integer partitionCount) throws ExecutionException, InterruptedException {
        NewTopic newTopic = new NewTopic(name, Optional.ofNullable(partitionCount), Optional.empty());
        var createTopicsResult = admin.createTopics(List.of(newTopic));
        int partitions = createTopicsResult.numPartitions(name).get();
        log.info("Topic created {} with partitions {}!", name, partitions);
    }

    @ShellMethod
    public void consume(String topic, @ShellOption(defaultValue = "my-group") String groupId) {
        try (var consumer = new KafkaConsumer<String, String>(consumerConfig(groupId))) {
            consumer.subscribe(List.of(topic));
            int count = 0;
            while (count < 30) {
                var result = consumer.poll(Duration.of(10, ChronoUnit.SECONDS));
                log.info("Read:{}", result.count());

                result.forEach((ConsumerRecord<String, String> record) -> {
                    log.info("=>{}", record.toString());
                });
                count++;
            }
        }
    }

    @ShellMethod
    public void publishAvro() {
        try (var producer = new KafkaProducer<String, GenericRecord>(avroProducerConfig())) {
            IntStream.rangeClosed(0, 10).forEach(i -> {
                var value = helloworld.newBuilder().setName("Mark").setAge(42).build();

                try {
                    var result = producer.send(new ProducerRecord<>("avro", value)).get();
                    log.info("Message published: {} ", result);
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }

            });
        }
    }

    @ShellMethod
    public void consumeAvro(@ShellOption(defaultValue = "my-group") String groupId) {
        try (var consumer = new KafkaConsumer<String, helloworld>(avroConsumerConfig(groupId))) {
            consumer.subscribe(List.of("avro"));
            int count = 0;
            while (count < 30) {
                var result = consumer.poll(Duration.of(10, ChronoUnit.SECONDS));
                log.info("AVRO Read:{}", result.count());

                result.forEach((ConsumerRecord<String, helloworld> record) -> {
                    log.info("=> {} - {}", record.value().getName(), record.value().getAge());
                });
                count++;
            }
        }
    }
}
