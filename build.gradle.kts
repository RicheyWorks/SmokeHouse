plugins {
    `java-library`
    application
    alias(libs.plugins.jmh)
}

// The shop window (Phase 4.4): `./gradlew run` starts the store dashboard on 127.0.0.1:8079.
application {
    mainClass.set("io.github.richeyworks.smokehouse.demo.StoreDashboard")
}

group = "io.github.richeyworks"
version = "0.1.0"

java {
    withSourcesJar()
}

// Mirror the siblings: 17-target bytecode from whatever JDK runs Gradle (Gradle 9 needs 17+).
tasks.withType<JavaCompile>().configureEach {
    options.release = 17
}

dependencies {
    // Both resolved to live sibling sources via the composite build in settings.gradle.kts.
    api("io.github.richeyworks:csrbt-core:0.1.0")          // the index engine
    api("io.github.richeyworks:superbeefsort:0.1.0")       // SpillSerializer + recovery sort

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.test {
    useJUnitPlatform()
    // csrbt-core logs via log4j-api with no backend on the classpath; keep tests quiet.
    systemProperty("log4j2.loggerContextFactory",
            "org.apache.logging.log4j.simple.SimpleLoggerContextFactory")
    systemProperty("org.apache.logging.log4j.simplelog.StatusLogger.level", "OFF")
}

// Phase 5 (measure): the SmokeHouse JMH rig, mirroring csrbt-benchmarks. A src/jmh/java source set
// (not a separate module — SmokeHouse is single-module). Never published.
//   ./gradlew compileJmhJava   compile the benchmarks only (fast check)
//   ./gradlew jmh              run them all (results at build/reports/jmh/results.json)
// Narrow a run by uncommenting `includes` below (regex over fully-qualified benchmark names).
val jmhVer = libs.versions.jmh.asProvider().get()

jmh {
    jmhVersion = jmhVer
    fork = 1
    warmupIterations = 3
    iterations = 5
    resultFormat = "JSON"
    resultsFile = layout.buildDirectory.file("reports/jmh/results.json")
    // includes = listOf("IndexUpsertBenchmark")   // e.g. the fast D1 seam run (~2 min, skips the store single-shots)
}
