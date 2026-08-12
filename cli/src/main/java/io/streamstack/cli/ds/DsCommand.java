package io.streamstack.cli.ds;

import io.streamstack.cli.Io;
import io.streamstack.cli.Main;
import io.streamstack.cli.Urls;
import io.streamstack.client.ChunkIterator;
import io.streamstack.client.DurableStream;
import io.streamstack.client.model.Chunk;
import io.streamstack.model.LiveMode;
import io.streamstack.model.Offset;
import io.streamstack.model.request.ReadRequest;
import io.streamstack.model.response.AppendResponse;
import io.streamstack.model.response.CreateResponse;
import io.streamstack.model.response.HeadResponse;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

import java.util.Objects;
import java.util.concurrent.Callable;

@Command(
    name = "ds",
    mixinStandardHelpOptions = true,
    subcommands = {
        DsCommand.Create.class,
        DsCommand.Head.class,
        DsCommand.Append.class,
        DsCommand.Read.class,
        DsCommand.Tail.class,
        DsCommand.Close.class,
        DsCommand.Delete.class,
        DsBench.class
    })
public final class DsCommand {

    @ParentCommand
    Main main;

    DurableStream open() {
        return DurableStream.create();
    }

    String url(String stream) {
        return Urls.ds(main.endpoint(), stream);
    }

    @Command(name = "create")
    static final class Create implements Callable<Integer> {

        @ParentCommand
        DsCommand parent;

        @Parameters(paramLabel = "STREAM")
        String stream;

        @Override
        public Integer call() {
            try (DurableStream client = parent.open()) {
                CreateResponse created = client.create(parent.url(stream));

                Io.err("created=" + created.created() + " next=" + created.nextOffset());
            }

            return 0;
        }
    }

    @Command(name = "head")
    static final class Head implements Callable<Integer> {

        @ParentCommand
        DsCommand parent;

        @Parameters(paramLabel = "STREAM")
        String stream;

        @Override
        public Integer call() {
            try (DurableStream client = parent.open()) {
                HeadResponse head = client.head(parent.url(stream));

                Io.out("next=" + head.nextOffset()
                    + " closed=" + head.closed()
                    + " type=" + head.contentType());
            }

            return 0;
        }
    }

    @Command(name = "append", description = "Append newline-delimited chunks from stdin")
    static final class Append implements Callable<Integer> {

        @ParentCommand
        DsCommand parent;

        @Parameters(paramLabel = "STREAM")
        String stream;

        @Override
        public Integer call() throws Exception {
            try (DurableStream client = parent.open()) {
                String url = parent.url(stream);

                Io.eachStdinLine(line -> {
                    AppendResponse ack = client.append(url, Urls.utf8(line));

                    Io.err("appended=" + ack.appended() + " next=" + ack.nextOffset());
                });
            }

            return 0;
        }
    }

    @Command(name = "read", description = "Read chunks; --live follows via SSE")
    static final class Read implements Callable<Integer> {

        @ParentCommand
        DsCommand parent;

        @Parameters(paramLabel = "STREAM")
        String stream;

        @Option(names = "--offset")
        String offset;

        @Option(names = "--live")
        boolean live;

        @Override
        public Integer call() {
            Offset start = Objects.nonNull(offset) ? Offset.of(offset) : Offset.beginning();
            ReadRequest request = new ReadRequest(start, live ? LiveMode.SSE : null, null);

            try (DurableStream client = parent.open();
                 ChunkIterator chunks = client.read(parent.url(stream), request)) {
                for (Chunk chunk : chunks) {
                    if (chunk.data().length > 0) {
                        Io.out(chunk.dataAsString());
                    }
                }
            }

            return 0;
        }
    }

    @Command(name = "tail", description = "Follow from now; --follow keeps SSE open")
    static final class Tail implements Callable<Integer> {

        @ParentCommand
        DsCommand parent;

        @Parameters(paramLabel = "STREAM")
        String stream;

        @Option(names = {"-f", "--follow"})
        boolean follow;

        @Override
        public Integer call() {
            LiveMode live = follow ? LiveMode.SSE : null;
            ReadRequest request = new ReadRequest(Offset.of(Offset.NOW), live, null);

            try (DurableStream client = parent.open();
                 ChunkIterator chunks = client.read(parent.url(stream), request)) {
                for (Chunk chunk : chunks) {
                    if (chunk.data().length > 0) {
                        Io.out(chunk.dataAsString());
                    }
                }
            }

            return 0;
        }
    }

    @Command(name = "close")
    static final class Close implements Callable<Integer> {

        @ParentCommand
        DsCommand parent;

        @Parameters(paramLabel = "STREAM")
        String stream;

        @Override
        public Integer call() {
            try (DurableStream client = parent.open()) {
                var result = client.close(parent.url(stream));

                Io.err("closed next=" + result.finalOffset());
            }

            return 0;
        }
    }

    @Command(name = "delete")
    static final class Delete implements Callable<Integer> {

        @ParentCommand
        DsCommand parent;

        @Parameters(paramLabel = "STREAM")
        String stream;

        @Override
        public Integer call() {
            try (DurableStream client = parent.open()) {
                client.delete(parent.url(stream));
                Io.err("deleted " + stream);
            }

            return 0;
        }
    }
}
