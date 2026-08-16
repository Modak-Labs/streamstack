package io.streamstack.cli;

import org.junit.jupiter.api.Test;

import picocli.CommandLine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MainTest {

    @Test
    void helpListsProtocols() {
        String help = new CommandLine(new Main()).getUsageMessage();

        assertTrue(help.contains("ds"));
        assertTrue(help.contains("native"));
    }

    @Test
    void nativeHelpListsRecordCommands() {
        CommandLine cmd = new CommandLine(new Main());
        String help = cmd.getSubcommands().get("native").getUsageMessage();

        assertTrue(help.contains("append"));
        assertTrue(help.contains("read"));
        assertTrue(help.contains("tail"));
        assertTrue(help.contains("bench"));
    }

    @Test
    void dsHelpListsRecordCommands() {
        CommandLine cmd = new CommandLine(new Main());
        String help = cmd.getSubcommands().get("ds").getUsageMessage();

        assertTrue(help.contains("append"));
        assertTrue(help.contains("read"));
        assertTrue(help.contains("tail"));
        assertTrue(help.contains("bench"));
    }

    @Test
    void missingSubcommandIsUsage() {
        int code = new CommandLine(new Main()).execute();

        assertEquals(0, code);
    }

    @Test
    void dsUrlBuild() {
        assertEquals("http://127.0.0.1:4437/events", Urls.ds("http://127.0.0.1:4437", "events"));
    }
}
