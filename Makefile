# Binary name
BINARY_NAME=myexplainer

# Build directory
BUILD_DIR=bin

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get

# Platforms
WINDOWS=windows
LINUX=linux
DARWIN=darwin

# Architectures
AMD64=amd64
ARM64=arm64
ARM=arm

# Make all builds
all: clean build-all

# Build for all platforms and architectures
build-all: windows-amd64 windows-arm64 linux-amd64 linux-arm64 linux-arm darwin-amd64 darwin-arm64

# Windows builds
windows-amd64:
	GOOS=$(WINDOWS) GOARCH=$(AMD64) $(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME)-$(WINDOWS)-$(AMD64) ./cmd

windows-arm64:
	GOOS=$(WINDOWS) GOARCH=$(ARM64) $(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME)-$(WINDOWS)-$(ARM64) ./cmd

# Linux builds
linux-amd64:
	GOOS=$(LINUX) GOARCH=$(AMD64) $(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME)-$(LINUX)-$(AMD64) ./cmd

linux-arm64:
	GOOS=$(LINUX) GOARCH=$(ARM64) $(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME)-$(LINUX)-$(ARM64) ./cmd

linux-arm:
	GOOS=$(LINUX) GOARCH=$(ARM) $(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME)-$(LINUX)-$(ARM) ./cmd

# Darwin (macOS) builds
darwin-amd64:
	GOOS=$(DARWIN) GOARCH=$(AMD64) $(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME)-$(DARWIN)-$(AMD64) ./cmd

darwin-arm64:
	GOOS=$(DARWIN) GOARCH=$(ARM64) $(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME)-$(DARWIN)-$(ARM64) ./cmd

# Clean build files
clean:
	$(GOCLEAN)
	rm -rf $(BUILD_DIR)

# Run tests
test:
	$(GOTEST) -v ./...

# Build for the current platform
build:
	$(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd

# Run the application
run: build
	./$(BUILD_DIR)/$(BINARY_NAME)

.PHONY: all build-all windows-amd64 windows-arm64 linux-amd64 linux-arm64 linux-arm darwin-amd64 darwin-arm64 clean test build run