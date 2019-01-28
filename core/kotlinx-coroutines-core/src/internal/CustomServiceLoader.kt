package kotlinx.coroutines.internal

import java.io.*
import java.lang.ClassCastException
import java.net.URL
import java.util.*
import java.util.jar.JarFile
import java.util.zip.ZipEntry
import kotlin.NoSuchElementException
import java.io.IOException
import java.io.BufferedReader

private const val PREFIX: String = "META-INF/services/"

public fun <S> loadProviders(service: Class<S>, loader: ClassLoader) = CustomServiceLoader(service, loader)

public class CustomServiceLoader<S> internal constructor (private val service: Class<S>, private val loader: ClassLoader?) : Iterable<S> {

    private fun parse(service: Class<S>, url: URL): Iterator<String> {
        val names = mutableListOf<String>()
        try {
            val string = url.toString()
            val separatorIndex = string.indexOf('!')
            val pathToJar = string.substring(0, separatorIndex)
            val entry = string.substring(separatorIndex + 2, string.length)
            (JarFile(pathToJar.substring("jar:file:/".length), false) as Closeable).use { file ->
                BufferedReader(InputStreamReader((file as JarFile).getInputStream(ZipEntry(entry)), "UTF-8")).use { r ->
                    var lineCounter = 1
                    while (lineCounter >= 0) {
                        lineCounter = parseLine(service, url, r, lineCounter, names)
                    }
                }
            }
        } catch (e: Throwable) {
            error(service, "Error reading configuration file", e)
        }
        return names.iterator()
    }

    private fun parseLine(service: Class<*>, u: URL, r: BufferedReader, lc: Int, names: MutableList<String>): Int {
        var line = r.readLine() ?: return -1
        val commentPos = line.indexOf('#')
        if (commentPos >= 0) line = line.substring(0, commentPos)
        line = line.trim()
        val n = line.length
        if (n != 0) {
            if (line.indexOf(' ') >= 0 || line.indexOf('\t') >= 0)
                error(service, "$u : $lc : Illegal configuration-file syntax")
            var cp = line.codePointAt(0)
            if (!Character.isJavaIdentifierStart(cp))
                error(service, "$u : $lc : Illegal provider-class name: $line")
            var i = Character.charCount(cp)
            while (i < n) {
                cp = line.codePointAt(i)
                if (!Character.isJavaIdentifierPart(cp) && cp != '.'.toInt())
                    error(service, "$u : $lc : Illegal provider-class name: $line")
                i += Character.charCount(cp)
            }
            if (!names.contains(line))
                names.add(line)
        }
        return lc + 1
    }


    override fun iterator(): Iterator<S> {
        return object : Iterator<S> {
            private var urls: Enumeration<URL>? = null
            private var providersToLoad: Iterator<String>? = null
            private var nextProviderName: String? = null

            override fun hasNext(): Boolean {
                nextProviderName?.let { return true }
                urls = urls ?: try {
                    val fullServiceName = PREFIX + service.name
                    if (loader == null) {
                        ClassLoader.getSystemResources(fullServiceName)
                    } else {
                        loader.getResources(fullServiceName)
                    }
                } catch (e: IOException) {
                    error(service, "Error locating configuration files", e)
                }
                if (!urls!!.hasMoreElements()) return false
                while ((providersToLoad == null || providersToLoad!!.hasNext()) && urls!!.hasMoreElements()) {
                    providersToLoad = parse(service, urls!!.nextElement())
                }
                nextProviderName = providersToLoad!!.next()
                return true
            }

            override fun next(): S {
                if (!hasNext()) throw NoSuchElementException()
                val nextName = nextProviderName
                nextProviderName = null
                val cl = try {
                    Class.forName(nextName, false, loader)
                } catch(e: ClassNotFoundException) {
                    error(service, "Provider $nextName not found", e)
                }
                if (!service.isAssignableFrom(cl)) {
                    error(service, "Provider $nextName  not a subtype",
                        ClassCastException("${service.canonicalName} is not assignable from ${cl.canonicalName}"))
                }
                return try {
                    service.cast(cl.getDeclaredConstructor().newInstance())
                } catch (e: Throwable) {
                    error(service, "Provider $nextName could not be instantiated", e)
                }
            }
        }
    }

    private fun error(service: Class<*>, msg: String, e: Throwable): Nothing = throw ServiceConfigurationError(service.name + ": " + msg, e)
    private fun error(service: Class<*>, msg: String): Nothing = throw ServiceConfigurationError("${service.name}: $msg")
}