package net.corda.demobench.model

import net.corda.cordform.CordformNode
import net.corda.core.internal.createDirectories
import net.corda.core.internal.isDirectory
import net.corda.core.internal.isRegularFile
import net.corda.core.utilities.loggerFor
import rx.Observable
import rx.Scheduler
import rx.schedulers.Schedulers
import tornadofx.*
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardWatchEventKinds
import java.nio.file.WatchEvent
import java.nio.file.WatchKey
import java.nio.file.WatchService
import java.util.concurrent.TimeUnit

/**
 * Utility class which copies nodeInfo files across a set of running nodes.
 *
 * This class will create paths that it needs to poll and to where it needs to copy files in case those
 * don't exist yet.
 */
class NodeInfoFilesCopier(scheduler: Scheduler = Schedulers.io()): Controller() {

    class NodeWasNeverRegisteredException: IllegalStateException()

    companion object {
        private val logger = loggerFor<NodeInfoFilesCopier>()
    }

    private val nodeDataMap = mutableMapOf<Path, NodeData>()

    init {
        Observable.interval(5, TimeUnit.SECONDS, scheduler)
                .subscribe { poll() }
    }

    /**
     * @param nodeConfig the configuration to be added.
     * Add a [NodeConfig] for a node which is about to be started.
     * Its nodeInfo file will be copied to other nodes' additional-node-infos directory, and conversely,
     * other nodes' nodeInfo files will be copied to this node additional-node-infos directory.
     */
    @Synchronized
    fun addConfig(nodeConfig: NodeConfig) {
        val newNodeFile = NodeData(nodeConfig.nodeDir)
        nodeDataMap.put(nodeConfig.nodeDir, newNodeFile)

        for (previouslySeenFile in allPreviouslySeenFiles()) {
            copy(previouslySeenFile, newNodeFile.destination.resolve(previouslySeenFile.fileName))
        }
        logger.info("Now watching: ${nodeConfig.nodeDir}")
    }

    /**
     * @param nodeConfig the configuration to be removed.
     * Remove the configuration of a node which is about to be stopped or already stopped.
     * No files written by that node will be copied to other nodes, nor files from other nodes will be copied to this
     * one.
     *
     * @throws NodeWasNeverRegisteredException if [addConfig] was not called before with the same [nodeConfig]
     */
    @Synchronized
    fun removeConfig(nodeConfig: NodeConfig) {
        val nodeData = nodeDataMap.get(nodeConfig.nodeDir) ?: throw NodeWasNeverRegisteredException()
        nodeData.watchService.close()
        nodeDataMap.remove(nodeConfig.nodeDir)
        logger.info("Stopped watching: ${nodeConfig.nodeDir}")
    }

    private fun allPreviouslySeenFiles() = nodeDataMap.values.map { it.previouslySeenFiles }.flatten()

    @Synchronized
    private fun poll() {
        for (nodeFile in nodeDataMap.values) {
            val path = nodeFile.nodeDir
            val watchService = nodeFile.watchService
            val watchKey: WatchKey = watchService.poll() ?: continue

            for (event in watchKey.pollEvents()) {
                val kind = event.kind()
                if (kind == StandardWatchEventKinds.OVERFLOW) continue

                @Suppress("UNCHECKED_CAST")
                val fileName: Path = (event as WatchEvent<Path>).context()
                val fullSourcePath = path.resolve(fileName)
                if (fullSourcePath.isRegularFile() && fileName.toString().startsWith("nodeInfo-")) {
                    nodeFile.previouslySeenFiles.add(fullSourcePath)
                    for (destination in nodeDataMap.values.map { it.destination }) {
                        val fullDestinationPath = destination.resolve(fileName)
                        copy(fullSourcePath, fullDestinationPath)
                    }
                }
            }
            if (!watchKey.reset()) {
                logger.warn("Couldn't reset watchKey for path $path, it was probably deleted.")
            }
        }
    }

    private fun copy(source: Path, destination: Path) {
        try {
            // REPLACE_EXISTING is needed in case we copy a file being written and we need to overwrite it with the
            // "full" file.
            Files.copy(source, destination, StandardCopyOption.REPLACE_EXISTING)
        } catch (exception: Exception) {
            logger.warn("Couldn't copy $source to $destination.", exception)
        }
    }

    /**
     * Convenience holder for all the paths and files relative to a single node.
     */
    private class NodeData(val nodeDir: Path) {
        companion object {
            private fun initWatch(path: Path): WatchService {
                if (!path.isDirectory()) {
                    logger.info("Creating $path which doesn't exist.")
                    path.createDirectories()
                }
                val watchService = path.fileSystem.newWatchService()
                path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY)
                logger.info("Now watching $path")
                return watchService
            }
        }

        val watchService: WatchService
        val destination: Path
        val previouslySeenFiles = mutableSetOf<Path>()

        init {
            this.watchService = initWatch(nodeDir)
            this.destination = nodeDir.resolve(CordformNode.NODE_INFO_DIRECTORY)
            destination.createDirectories()
        }
    }
}
