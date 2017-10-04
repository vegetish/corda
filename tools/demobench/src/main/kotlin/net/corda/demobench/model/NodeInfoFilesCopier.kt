package net.corda.demobench.model

import net.corda.cordform.CordformNode
import net.corda.core.internal.createDirectories
import net.corda.core.internal.isDirectory
import net.corda.core.internal.isRegularFile
import net.corda.core.utilities.loggerFor
import rx.Observable
import rx.Scheduler
import rx.Subscription
import rx.schedulers.Schedulers
import java.nio.file.*
import java.util.concurrent.TimeUnit


/**
 * Utility class which copies nodeInfo files across a set of running nodes.
 *
 * This class will create paths that it needs to poll and to where it needs to copy files in case those
 * don't exist yet.
 */
class NodeInfoFilesCopier(scheduler: Scheduler = Schedulers.io()) {

    companion object {
        private val logger = loggerFor<NodeInfoFilesCopier>()
    }

    // Paths we are copying into.
    private val destinations = mutableListOf<Path>()
    // WatchServices watching paths we are watching.
    private val watchTargets = mutableListOf<WatchTarget>()
    // Set of paths of files which have been copied from already..
    private val previouslySeenFiles = mutableSetOf<Path>()

    init {
        Observable.interval(5, TimeUnit.SECONDS, scheduler)
                .subscribe { poll() }
    }

    /**
     * Add a [NodeConfig] for a node which is about to be started.
     * Its nodeInfo file will be copied to other nodes' additional-node-infos directory, and conversely,
     * other nodes' nodeInfo files will be copied to this node additional-node-infos directory.
     */
    fun addConfig(nodeConfig: NodeConfig) {
        addDestination(nodeConfig.nodeDir.resolve(CordformNode.NODE_INFO_DIRECTORY))
        watchTargets.add(WatchTarget(nodeConfig.nodeDir))
    }

    private fun poll() {
        for (watchTarget in watchTargets) {
            val watchKey: WatchKey = watchTarget.watchService.poll() ?: continue

            for (event in watchKey.pollEvents()) {
                val kind = event.kind()
                if (kind == StandardWatchEventKinds.OVERFLOW) continue

                @Suppress("UNCHECKED_CAST")
                val fileName : Path = (event as WatchEvent<Path>).context()
                val fullSourcePath = watchTarget.path.resolve(fileName)
                if (fullSourcePath.isRegularFile() && fileName.toString().startsWith("nodeInfo-")) {
                    previouslySeenFiles.add(fullSourcePath)
                    for (destination in destinations) {
                        val fullDestinationPath = destination.resolve(fileName)
                        copy(fullSourcePath, fullDestinationPath)
                    }
                }
            }
            if (!watchKey.reset()) {
                logger.warn("Couldn't reset watchKey for path ${watchTarget.path}, it was probably deleted.")
            }
        }
    }

    private fun copy(source : Path, destination: Path) {
        try {
            // REPLACE_EXISTING is needed in case we copy a file being written and we need to overwrite it with the
            // "full" file.
            Files.copy(source, destination, StandardCopyOption.REPLACE_EXISTING)
        } catch (exception : Exception) {
            logger.warn("Couldn't copy $source to $destination. Exception: $exception")
        }
    }

    private fun addDestination(destination : Path) {
        destination.createDirectories()
        destinations.add(destination)

        for (previouslySeenFile in previouslySeenFiles) {
            copy(previouslySeenFile, destination.resolve(previouslySeenFile.fileName))
        }
    }

    // Utility class which holds a path and a WatchService watching that path.
    // If path doesn't exist, it is created.
    private class WatchTarget(val path : Path) {
        val watchService : WatchService

        init {
            this.watchService = initWatch(path)
        }

        companion object {
            private fun initWatch(path: Path): WatchService {
                if (!path.isDirectory()) {
                    logger.info("Creating $path which doesn't exist.")
                    path.toFile().mkdirs()
                }
                val watchService = path.fileSystem.newWatchService()
                path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY)
                logger.info("Now watching $path")
                return watchService
            }
        }
    }
}
