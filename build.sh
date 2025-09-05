#!/bin/bash

# Set environment variables
export GOOS="linux"
export GOARCH="amd64"
export CGO_ENABLED="0"

# Define the handlers base path relative to the current directory
handlersBasePath="./handlers"

# Function to compile a single handler
compile_handler() {
    local handlerPath="$1"
    local handlerName=$(basename "$handlerPath")
    
    # Build the Go program
    go build -o "$handlerPath/bootstrap" "$handlerPath/$handlerName.go"
    
    # Create a zip file with bootstrap at the root
    zip -j "$handlersBasePath/$handlerName/$handlerName.zip" "$handlerPath/bootstrap"
    
    # Remove the bootstrap file
    rm "$handlerPath/bootstrap"
}

# Loop through each handler in the handlers directory
for handlerPath in "$handlersBasePath"/*; do
    # Check if it's a directory
    if [ -d "$handlerPath" ]; then
        compile_handler $handlerPath &
    fi
done

wait