clean:
	rm -rf .nextflow*
	rm -rf work
	rm -rf build
	./gradlew clean

# Build the plugin
assemble:
	./gradlew assemble

# Run plugin unit tests
test:
	./gradlew test

# Install the plugin into local nextflow plugins dir
install:
	./gradlew install

# Publish the plugin
release:
	./gradlew releasePlugin