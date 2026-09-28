# Use the Gradle wrapper by default; override with e.g. `make GRADLE=gradle ...`
# or `export GRADLE=gradle` (useful in pixi/conda environments).
GRADLE ?= ./gradlew

# Build the plugin
assemble:
	$(GRADLE) assemble

clean:
	rm -rf .nextflow*
	rm -rf work
	rm -rf build
	$(GRADLE) clean

# Run plugin unit tests
test:
	$(GRADLE) test

# Unit tests plus the dependency check and the memory bound (DESIGN.md section 0)
check:
	$(GRADLE) check

# Build, install into a temp NXF_PLUGINS_DIR and publish through cas:// for real.
# Needs Nextflow 26.04.6; override with NEXTFLOW=/path/to/nextflow.
smoke:
	./gate/smoke.sh

# Install the plugin into the real ~/.nextflow/plugins. This writes outside the
# project, so it is guarded: run `make install FORCE=1` to confirm. The Gate and
# the smoke test never need this -- they install into a throwaway NXF_PLUGINS_DIR.
install:
ifndef FORCE
	@echo "Refusing to write into ~/.nextflow/plugins without FORCE=1."
	@echo "Use 'make smoke' or 'make gate' for a sandboxed install, or 'make install FORCE=1' to proceed."
	@exit 1
endif
	$(GRADLE) installPlugin

# Plugin commands (`nextflow plugin nf-blocks:<verb>`) start the plugin unpinned,
# so an unpublished build is found only through NXF_PLUGINS_TEST_REPOSITORY naming
# a plugins.json for the built zip, with NXF_OFFLINE unset (DESIGN.md section 15,
# "Plugin verbs"). `make plugins-json` writes it and prints the export line;
# `make explore` uses it. Both assume `make install FORCE=1` has run.
NEXTFLOW ?= nextflow

.PHONY: plugins-json
plugins-json:
	@mkdir -p build
	@echo "export NXF_PLUGINS_TEST_REPOSITORY=file://$$(./gate/browser/plugin-repo.sh "$(CURDIR)" "$(CURDIR)/build")"

# Serve the explorer over the stores a config names:
#   make explore CONFIG=/path/to/nextflow.config ARGS='--port 8123'
# Without CONFIG, Nextflow reads the nextflow.config in the current directory.
.PHONY: explore
explore:
	@mkdir -p build
	unset NXF_OFFLINE; \
	NXF_PLUGINS_TEST_REPOSITORY="file://$$(./gate/browser/plugin-repo.sh "$(CURDIR)" "$(CURDIR)/build")" \
	$(NEXTFLOW) $(if $(CONFIG),-c $(CONFIG)) plugin nf-blocks:explore $(ARGS)

# Publish the plugin
release:
	$(GRADLE) releasePlugin

# The Gate (gate/README.md): build, run the Test Pipeline six ways against a
# throwaway cas:// store, then assert over it from outside with gate/assert.py.
# Set GATE_ROOT to reuse a root; NEXTFLOW to pick the 26.04.6 binary.
# .PHONY because a directory named gate/ already exists.
.PHONY: gate
gate:
	python3 -m unittest discover -s gate
	./gate/gate.sh

# The Gate's cloud browser tier (gate/README.md): throwaway buckets in the scidev
# account, a human's AWS SSO session. Run after `make gate` with the same GATE_ROOT.
.PHONY: gate-cloud
gate-cloud:
	./gate/cloud/cloud.sh "$(GATE_ROOT)"

# Gate tier two (gate/tier2/README.md): the scidev Batch queue, a throwaway bucket,
# Rob's SSO session. Run after `make gate` with the same GATE_ROOT.
.PHONY: gate-tier2
gate-tier2:
	./gate/tier2/tier2.sh "$(GATE_ROOT)"
