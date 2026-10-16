plugins {
    id("org.jetbrains.kotlin.jvm")
    id("org.jetbrains.kotlin.plugin.serialization")
    id("org.jetbrains.kotlinx.kover")
}
kotlin {
    jvmToolchain(17)
    sourceSets.main { kotlin.srcDir("../src/main/kotlin") }
}
dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.1")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.8.0")
    implementation("com.squareup.okhttp3:okhttp:4.12.0")
    implementation("com.squareup.okhttp3:okhttp-sse:4.12.0")
    testImplementation(kotlin("test-junit"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.1")
    testImplementation("com.squareup.okhttp3:mockwebserver:4.12.0")
}
tasks.test {
    useJUnit()
    exclude("**/IntegrationTest*")
}
val integrationTest by tasks.registering(Test::class) {
    description = "Runs against the local AdonisJS integration and chat servers (npm ci required)."
    group = "verification"
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    include("**/IntegrationTest*")
    useJUnit()
    inputs.files(rootProject.file("integration-server/server.mjs"), rootProject.file("Examples/Backend/server.mjs"))
    systemProperty("transmit.server", rootProject.file("integration-server/server.mjs").absolutePath)
    systemProperty("transmit.example.server", rootProject.file("Examples/Backend/server.mjs").absolutePath)
    shouldRunAfter(tasks.test)
}
kover.reports.verify.rule { minBound(100) }
tasks.check { dependsOn("koverVerify", integrationTest) }
