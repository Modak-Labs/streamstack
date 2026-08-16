package io.streamstack.cli.ss;

import io.streamstack.cli.Io;
import io.streamstack.cli.Main;
import io.streamstack.client.StreamStack;
import io.streamstack.model.RecordEnvelope;
import io.streamstack.model.SequencedRecord;
import io.streamstack.model.request.AppendRequest;
import io.streamstack.model.response.AppendResponse;
import io.streamstack.model.response.HeadResponse;
import io.streamstack.model.response.ListResponse;
import io.streamstack.model.response.ReadResponse;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

@Command(
    name = "native",
    mixinStandardHelpOptions = true,
    subcommands = {
        SsCommand.Create.class,
        SsCommand.Head.class,
        SsCommand.Append.class,
        SsCommand.Read.class,
        SsCommand.Tail.class,
        SsCommand.ListStreams.class,
        SsCommand.Close.class,
        SsCommand.Delete.class,
        SsBench.class
    })
public final class SsCommand {

    @ParentCommand
    Main main;

    StreamStack open() {
        return StreamStack.builder().baseUrl(main.endpoint()).build();
    }

    @Command(name = "create")
    static final class Create implements Callable<Integer> {

        @ParentCommand
        SsCommand parent;

        @Parameters(paramLabel = "STREAM")
        String stream;

        @Option(names = "--content-type", defaultValue = "application/octet-stream")
        String contentType;

        @Override
        public Integer call() {
            try (StreamStack client = parent.open()) {
                Io.err("created=" + client.create(stream, contentType));
            }

            return 0;
        }
    }

    @Command(name = "head")
    static final class Head implements Callable<Integer> {

        @ParentCommand
        SsCommand parent;

        @Parameters(paramLabel = "STREAM")
        String stream;

        @Override
        public Integer call() {
            try (StreamStack client = parent.open()) {
                HeadResponse head = client.head(stream).orElse(null);

                if (head == null) {
                    Io.err("not found");
                    return 1;
                }

                Io.out("start=" + head.startSeq()
                    + " next=" + head.nextSeq()
                    + " closed=" + head.closed()
                    + " type=" + head.contentType());
            }

            return 0;
        }
    }

    @Command(name = "append", description = "Append newline-delimited records from stdin")
    static final class Append implements Callable<Integer> {

        @ParentCommand
        SsCommand parent;

        @Parameters(paramLabel = "STREAM")
        String stream;

        @Override
        public Integer call() throws Exception {
            try (StreamStack client = parent.open()) {
                Io.eachStdinLine(line -> {
                    AppendResponse ack = client.append(stream, new AppendRequest(List.of(
                        new RecordEnvelope(0, Map.of(), line.getBytes(StandardCharsets.UTF_8)))));

                    Io.err("start=" + ack.startSeq() + " next=" + ack.nextSeq());
                });
            }

            return 0;
        }
    }

    @Command(name = "read", description = "Read records from a sequence number")
    static final class Read implements Callable<Integer> {

        @ParentCommand
        SsCommand parent;

        @Parameters(paramLabel = "STREAM")
        String stream;

        @Option(names = "--seq", defaultValue = "0")
        long seq;

        @Override
        public Integer call() {
            try (StreamStack client = parent.open()) {
                long next = seq;

                while (true) {
                    ReadResponse page = client.read(stream, next, 0, 0);

                    print(page.records());
                    next = page.nextSeq();

                    if (page.upToDate() || page.records().isEmpty()) {
                        return 0;
                    }
                }
            }
        }
    }

    @Command(name = "tail", description = "Follow from now; polls for new records")
    static final class Tail implements Callable<Integer> {

        @ParentCommand
        SsCommand parent;

        @Parameters(paramLabel = "STREAM")
        String stream;

        @Override
        public Integer call() throws Exception {
            try (StreamStack client = parent.open()) {
                long next = client.head(stream).map(HeadResponse::nextSeq).orElse(0L);

                while (!Thread.currentThread().isInterrupted()) {
                    ReadResponse page = client.read(stream, next, 0, 0);

                    print(page.records());
                    next = page.nextSeq();

                    if (page.closed() && page.upToDate()) {
                        return 0;
                    }

                    if (page.records().isEmpty()) {
                        Thread.sleep(500);
                    }
                }
            }

            return 0;
        }
    }

    @Command(name = "list")
    static final class ListStreams implements Callable<Integer> {

        @ParentCommand
        SsCommand parent;

        @Option(names = "--prefix", defaultValue = "/")
        String prefix;

        @Option(names = "--limit", defaultValue = "0")
        int limit;

        @Override
        public Integer call() {
            try (StreamStack client = parent.open()) {
                ListResponse listing = client.list(prefix, null, limit);

                for (HeadResponse stream : listing.streams()) {
                    Io.out(stream.name()
                        + " start=" + stream.startSeq()
                        + " next=" + stream.nextSeq()
                        + " closed=" + stream.closed());
                }

                if (listing.hasMore()) {
                    Io.err("more streams available, raise --limit or page with start_after");
                }
            }

            return 0;
        }
    }

    @Command(name = "close")
    static final class Close implements Callable<Integer> {

        @ParentCommand
        SsCommand parent;

        @Parameters(paramLabel = "STREAM")
        String stream;

        @Override
        public Integer call() {
            try (StreamStack client = parent.open()) {
                Io.err("closed next=" + client.closeStream(stream));
            }

            return 0;
        }
    }

    @Command(name = "delete")
    static final class Delete implements Callable<Integer> {

        @ParentCommand
        SsCommand parent;

        @Parameters(paramLabel = "STREAM")
        String stream;

        @Override
        public Integer call() {
            try (StreamStack client = parent.open()) {
                Io.err("deleted=" + client.delete(stream));
            }

            return 0;
        }
    }

    private static void print(List<SequencedRecord> records) {
        for (SequencedRecord record : records) {
            Io.out(record.seq() + "\t" + new String(record.envelope().body(), StandardCharsets.UTF_8));
        }
    }
}
