package io.streamstack.cli.s2;

import io.streamstack.cli.Io;
import io.streamstack.cli.Main;
import io.streamstack.cli.Urls;
import io.streamstack.s2.client.Producer;
import io.streamstack.s2.client.ReadSession;
import io.streamstack.s2.client.S2;
import io.streamstack.s2.client.Stream;
import io.streamstack.s2.model.request.AppendRecord;
import io.streamstack.s2.model.request.ReadRequest;
import io.streamstack.s2.model.response.AppendResponse;
import io.streamstack.s2.model.response.BasinResponse;
import io.streamstack.s2.model.response.SequencedRecord;
import io.streamstack.s2.model.response.StreamResponse;
import io.streamstack.s2.model.response.TailResponse;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

import java.util.Objects;
import java.util.concurrent.Callable;

@Command(
    name = "s2",
    mixinStandardHelpOptions = true,
    subcommands = {
        S2Command.Ls.class,
        S2Command.CreateBasin.class,
        S2Command.DeleteBasin.class,
        S2Command.CreateStream.class,
        S2Command.DeleteStream.class,
        S2Command.Append.class,
        S2Command.Read.class,
        S2Command.Tail.class,
        S2Command.CheckTail.class,
        S2Bench.class
    })
public final class S2Command {

    @ParentCommand
    Main main;

    S2 open() {
        return S2.builder(main.endpoint()).build();
    }

    @Command(name = "ls", description = "List basins, or streams in a basin")
    static final class Ls implements Callable<Integer> {

        @ParentCommand
        S2Command parent;

        @Parameters(arity = "0..1", paramLabel = "BASIN")
        String basin;

        @Option(names = {"-p", "--prefix"})
        String prefix;

        @Option(names = {"-n", "--limit"})
        Integer limit;

        @Override
        public Integer call() {
            try (S2 s2 = parent.open()) {
                if (Objects.isNull(basin) || basin.isBlank()) {
                    for (BasinResponse basinInfo : s2.listBasins(prefix, null, limit).basins()) {
                        Io.out(basinInfo.name());
                    }
                } else {
                    String name = Urls.basin(basin);

                    for (StreamResponse stream : s2.basin(name).listStreams(prefix, null, limit).streams()) {
                        Io.out(name + "/" + stream.name());
                    }
                }
            }

            return 0;
        }
    }

    @Command(name = "create-basin")
    static final class CreateBasin implements Callable<Integer> {

        @ParentCommand
        S2Command parent;

        @Parameters(paramLabel = "BASIN")
        String basin;

        @Override
        public Integer call() {
            try (S2 s2 = parent.open()) {
                s2.createBasin(Urls.basin(basin));
                Io.err("basin created: " + Urls.basin(basin));
            }

            return 0;
        }
    }

    @Command(name = "delete-basin")
    static final class DeleteBasin implements Callable<Integer> {

        @ParentCommand
        S2Command parent;

        @Parameters(paramLabel = "BASIN")
        String basin;

        @Override
        public Integer call() {
            try (S2 s2 = parent.open()) {
                s2.deleteBasin(Urls.basin(basin));
                Io.err("basin deleted: " + Urls.basin(basin));
            }

            return 0;
        }
    }

    @Command(name = "create-stream")
    static final class CreateStream implements Callable<Integer> {

        @ParentCommand
        S2Command parent;

        @Parameters(paramLabel = "BASIN/STREAM")
        String uri;

        @Override
        public Integer call() {
            Urls.Resource resource = Urls.s2(uri);

            try (S2 s2 = parent.open()) {
                s2.basin(resource.basin()).createStream(resource.stream());
                Io.err("stream created: " + resource.basin() + "/" + resource.stream());
            }

            return 0;
        }
    }

    @Command(name = "delete-stream")
    static final class DeleteStream implements Callable<Integer> {

        @ParentCommand
        S2Command parent;

        @Parameters(paramLabel = "BASIN/STREAM")
        String uri;

        @Override
        public Integer call() {
            Urls.Resource resource = Urls.s2(uri);

            try (S2 s2 = parent.open()) {
                s2.basin(resource.basin()).deleteStream(resource.stream());
                Io.err("stream deleted: " + resource.basin() + "/" + resource.stream());
            }

            return 0;
        }
    }

    @Command(name = "append", description = "Append newline-delimited records from stdin")
    static final class Append implements Callable<Integer> {

        @ParentCommand
        S2Command parent;

        @Parameters(paramLabel = "BASIN/STREAM")
        String uri;

        @Override
        public Integer call() throws Exception {
            Urls.Resource resource = Urls.s2(uri);

            try (S2 s2 = parent.open();
                 Producer producer = s2.basin(resource.basin()).stream(resource.stream()).producer()) {
                Io.eachStdinLine(line -> {
                    producer.submit(new AppendRecord(null, null, Urls.utf8(line)));
                    AppendResponse ack = producer.flush();

                    Io.err("appended seq=" + ack.start().seqNum() + " tail=" + ack.tail().seqNum());
                });
            }

            return 0;
        }
    }

    @Command(name = "read", description = "Read records; --live keeps the session open")
    static final class Read implements Callable<Integer> {

        @ParentCommand
        S2Command parent;

        @Parameters(paramLabel = "BASIN/STREAM")
        String uri;

        @Option(names = {"-s", "--seq-num"})
        Long seqNum;

        @Option(names = "--live")
        boolean live;

        @Option(names = {"-n", "--count"})
        Long count;

        @Override
        public Integer call() {
            Urls.Resource resource = Urls.s2(uri);

            try (S2 s2 = parent.open()) {
                Stream stream = s2.basin(resource.basin()).stream(resource.stream());

                if (!live) {
                    var batch = stream.read(
                        new ReadRequest(seqNum, null, null, false, count, null, null, null));

                    for (SequencedRecord record : batch.records()) {
                        Io.out(Urls.utf8(record.body()));
                    }

                    return 0;
                }

                Long next = seqNum;

                while (true) {
                    ReadRequest request = new ReadRequest(next, null, null, false, null, null, null, 25L);

                    try (ReadSession session = stream.readSession(request)) {
                        for (SequencedRecord record : session) {
                            next = record.seqNum() + 1;
                            Io.out(Urls.utf8(record.body()));
                        }
                    }
                }
            }
        }
    }

    @Command(name = "tail", description = "Show the last N records; --follow waits for new ones")
    static final class Tail implements Callable<Integer> {

        @ParentCommand
        S2Command parent;

        @Parameters(paramLabel = "BASIN/STREAM")
        String uri;

        @Option(names = {"-n", "--lines"}, defaultValue = "10")
        long lines;

        @Option(names = {"-f", "--follow"})
        boolean follow;

        @Override
        public Integer call() {
            Urls.Resource resource = Urls.s2(uri);

            try (S2 s2 = parent.open()) {
                Stream stream = s2.basin(resource.basin()).stream(resource.stream());
                ReadRequest request = new ReadRequest(null, null, lines, false, follow ? null : lines, null, null,
                    follow ? 25L : null);

                while (true) {
                    try (ReadSession session = stream.readSession(request)) {
                        Long last = null;

                        for (SequencedRecord record : session) {
                            last = record.seqNum();
                            Io.out(Urls.utf8(record.body()));
                        }

                        if (!follow) {
                            return 0;
                        }

                        request = new ReadRequest(
                            Objects.nonNull(last) ? last + 1 : null,
                            null, null, false, null, null, null, 25L);
                    }
                }
            }
        }
    }

    @Command(name = "check-tail")
    static final class CheckTail implements Callable<Integer> {

        @ParentCommand
        S2Command parent;

        @Parameters(paramLabel = "BASIN/STREAM")
        String uri;

        @Override
        public Integer call() {
            Urls.Resource resource = Urls.s2(uri);

            try (S2 s2 = parent.open()) {
                TailResponse tail = s2.basin(resource.basin()).stream(resource.stream()).checkTail();

                Io.out(tail.tail().seqNum() + " @ " + tail.tail().timestamp());
            }

            return 0;
        }
    }
}
