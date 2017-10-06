package net.corda.node

import joptsimple.OptionParser
import joptsimple.util.EnumConverter
import net.corda.core.internal.div
import net.corda.node.services.config.ConfigHelper
import net.corda.node.services.config.FullNodeConfiguration
import net.corda.nodeapi.config.parseAs
import org.slf4j.event.Level
import java.io.PrintStream
import java.nio.file.Path
import java.nio.file.Paths

// NOTE: Do not use any logger in this class as args parsing is done before the logger is setup.
class ArgsParser {
    private val optionParser = OptionParser()
    // The intent of allowing a command line configurable directory and config path is to allow deployment flexibility.
    // Other general configuration should live inside the config file unless we regularly need temporary overrides on the command line
    private val baseDirectoryArg = optionParser
            .accepts("base-directory", "The node working directory where all the files are kept")
            .withRequiredArg()
            .defaultsTo(".")
    private val configFileArg = optionParser
            .accepts("config-file", "The path to the config file")
            .withRequiredArg()
            .defaultsTo("node.conf")
    private val loggerLevel = optionParser
            .accepts("logging-level", "Enable logging at this level and higher")
            .withRequiredArg()
            .withValuesConvertedBy(object : EnumConverter<Level>(Level::class.java) {})
            .defaultsTo(Level.INFO)
    private val logToConsoleArg = optionParser.accepts("log-to-console", "If set, prints logging to the console as well as to a file.")
    private val sshdServerArg = optionParser.accepts("sshd", "Enables SSHD server for node administration.")
    private val noLocalShellArg = optionParser.accepts("no-local-shell", "Do not start the embedded shell locally.")
    private val isRegistrationArg = optionParser.accepts("initial-registration", "Start initial node registration with Corda network to obtain certificate from the permissioning server.")
    private val isVersionArg = optionParser.accepts("version", "Print the version and exit")
    private val justGenerateNodeInfoArg = optionParser.accepts("just-generate-node-info",
            "Perform the node start-up task necessary to generate its nodeInfo, save it to disk, then quit")
    private val helpArg = optionParser.accepts("help").forHelp()

    fun parse(vararg args: String): CmdLineOptions {
        val optionSet = optionParser.parse(*args)
        require(!optionSet.has(baseDirectoryArg) || !optionSet.has(configFileArg)) {
            "${baseDirectoryArg.options()[0]} and ${configFileArg.options()[0]} cannot be specified together"
        }
        val baseDirectory = Paths.get(optionSet.valueOf(baseDirectoryArg)).normalize().toAbsolutePath()
        val configFile = baseDirectory / optionSet.valueOf(configFileArg)
        val help = optionSet.has(helpArg)
        val loggingLevel = optionSet.valueOf(loggerLevel)
        val logToConsole = optionSet.has(logToConsoleArg)
        val isRegistration = optionSet.has(isRegistrationArg)
        val isVersion = optionSet.has(isVersionArg)
        val noLocalShell = optionSet.has(noLocalShellArg)
        val sshdServer = optionSet.has(sshdServerArg)
        val justGenerateNodeInfo = optionSet.has(justGenerateNodeInfoArg)
        return CmdLineOptions(baseDirectory, configFile, help, loggingLevel, logToConsole, isRegistration, isVersion,
                noLocalShell, sshdServer, justGenerateNodeInfo)
    }

    fun printHelp(sink: PrintStream) = optionParser.printHelpOn(sink)
}

data class CmdLineOptions(val baseDirectory: Path,
                          val configFile: Path,
                          val help: Boolean,
                          val loggingLevel: Level,
                          val logToConsole: Boolean,
                          val isRegistration: Boolean,
                          val isVersion: Boolean,
                          val noLocalShell: Boolean,
                          val sshdServer: Boolean,
                          val justGenerateNodeInfo: Boolean) {
    fun loadConfig() = ConfigHelper
            .loadConfig(baseDirectory, configFile)
            .parseAs<FullNodeConfiguration>()
}
