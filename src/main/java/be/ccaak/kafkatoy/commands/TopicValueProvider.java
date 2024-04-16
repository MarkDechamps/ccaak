package be.ccaak.kafkatoy.commands;

import org.springframework.shell.CompletionContext;
import org.springframework.shell.CompletionProposal;
import org.springframework.shell.standard.ValueProvider;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TopicValueProvider implements ValueProvider {

    @Override
    public List<CompletionProposal> complete(CompletionContext completionContext) {
        return Stream.of("topic1", "topic2")
                .map(CompletionProposal::new)
                .collect(Collectors.toList());
    }
}