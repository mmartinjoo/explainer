analyze:
	go vet ./...

build: analyze
	go build -o bin/explainer-macos-arm64 cmd/main.go

build-all: build
	GOOS=windows GOARCH=amd64 go build -o bin/explainer-win-x86-64.exe cmd/main.go
	GOOS=linux GOARCH=amd64 go build -o bin/explainer-linux-x86-64 cmd/main.go

run: build
	bin/explainer_mac_arm