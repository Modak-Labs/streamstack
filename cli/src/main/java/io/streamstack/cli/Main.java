package io.streamstack.cli;

import io.streamstack.cli.ds.DsCommand;
import io.streamstack.cli.ss.SsCommand;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ScopeType;

import java.util.Objects;

@Command(
    name = "streamstack",
    mixinStandardHelpOptions = true,
    subcommands = {SsCommand.class, DsCommand.class})
public final class Main implements Runnable {

    @Option(
        names = "--endpoint",
        description = "Server base URL",
        scope = ScopeType.INHERIT)
    String endpoint;

    public static void main(String[] args) {
        int code = new CommandLine(new Main())
            .setExecutionExceptionHandler((ex, cmd, parse) -> {
                String message = ex.getMessage();

                cmd.getErr().println(Objects.isNull(message) ? ex.getClass().getSimpleName() : message);
                return 1;
            })
            .execute(args);

        System.exit(code);
    }

    public String endpoint() {
        if (Objects.nonNull(endpoint) && !endpoint.isBlank()) {
            return trimSlash(endpoint);
        }

        String env = System.getenv("STREAMSTACK_ENDPOINT");

        if (Objects.nonNull(env) && !env.isBlank()) {
            return trimSlash(env);
        }

        return "http://127.0.0.1:4437";
    }

    private static String trimSlash(String value) {
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }

    @Override
    public void run() {
        CommandLine.usage(this, System.out);
    }
}
