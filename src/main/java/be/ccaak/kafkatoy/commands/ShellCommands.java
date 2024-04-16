package be.ccaak.kafkatoy.commands;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

@ShellComponent
public class ShellCommands {

    @ShellMethod(value = "publish", key = "publish")
    public void publish(String msg, @ShellOption(valueProvider = TopicValueProvider.class) String topic) {
        System.out.println("Should publish :"+msg+" on topic "+topic);
        KafkaProducer producer;
    }
}
