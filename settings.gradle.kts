rootProject.name = "smokehouse"

// Composite build: SmokeHouse is the third engine of the ecosystem — it stores what CSRBT
// indexes and what SuperBeefSort feeds. Including SuperBeefSort's build transitively includes
// CSRBT (nested composite), and Gradle substitutes both published coordinates
// (`io.github.richeyworks:csrbt-core`, `io.github.richeyworks:superbeefsort`) with the live
// sibling sources — no publish step, always builds against reality.
includeBuild("../SuperBeefSort")

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}
