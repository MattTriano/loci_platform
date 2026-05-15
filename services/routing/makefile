# Canonical build for the bike-map routing service.
#
# Produces two artifacts under build-output/:
#   - routing-lambda.zip : arm64 Lambda deployment package, ready to
#     hand to lambda:UpdateFunctionCode.
#   - routing-cli        : native x86_64 binary used by Airflow's
#     run_route_tests task to validate the routing implementation
#     against the just-built graph before deploying.
#
# Usage:
#   make build            # build both artifacts
#   make image            # (re)build the builder container image
#   make clean            # remove build-output/ contents
#
# The host directory build-output/ is mounted into the Airflow worker
# read-only via docker_compose.yml.

BUILDER_IMAGE := loci-routing-builder:latest
SOURCE_DIR    := $(CURDIR)
OUTPUT_DIR    := $(CURDIR)/build-output
CARGO_CACHE   := routing-cargo-cache

BUILD_CMD := \
	cargo lambda build --release --arm64 --bin routing-lambda && \
	cargo build --release --bin routing-cli

.PHONY: build image clean test

# Build is the default target. Depends on image so the builder is
# present, then runs both cargo invocations in a single container.
build: image
	mkdir -p $(OUTPUT_DIR)
	podman run --rm \
		-v $(SOURCE_DIR):/workspace \
		-v $(CARGO_CACHE):/var/cache/cargo \
		-v $(OUTPUT_DIR):/build-output \
		$(BUILDER_IMAGE) \
		sh -c "$(BUILD_CMD)"
	@# Repack the cargo-lambda bootstrap into a fresh zip with an
	@# explicit executable mode bit, since podman's user namespace
	@# can drop perms when writing into the host-mounted output dir.
	cd $(OUTPUT_DIR)/target/lambda/routing-lambda && \
		chmod 0755 bootstrap && \
		zip -j $(OUTPUT_DIR)/routing-lambda.zip bootstrap
	cp $(OUTPUT_DIR)/target/release/routing-cli $(OUTPUT_DIR)/routing-cli
	@echo
	@echo "Built artifacts:"
	@ls -lh $(OUTPUT_DIR)/routing-lambda.zip $(OUTPUT_DIR)/routing-cli

image:
	podman build -t $(BUILDER_IMAGE) -f Containerfile.build .

clean:
	rm -rf $(OUTPUT_DIR)

test: image
	podman run --rm \
		-v $(SOURCE_DIR):/workspace \
		-v $(CARGO_CACHE):/var/cache/cargo \
		-v $(OUTPUT_DIR):/build-output \
		$(BUILDER_IMAGE) \
		cargo test --workspace
