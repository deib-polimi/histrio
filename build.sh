#!/bin/bash

# Set environment variables
export GOOS="linux"
export GOARCH="amd64"
export CGO_ENABLED="0"

# Define the handlers base path relative to the current directory
handlersBasePath="./handlers"

# Loop through each handler in the handlers directory
for handlerName in "$handlersBasePath"/*; do
    # Check if it's a directory
    if [ -d "$handlerName" ]; then
        # Build the Go program
        go build -o "$handlerName/bootstrap" "$handlerName/$(basename "$handlerName").go"

        # Create a zip file with bootstrap at the root
        zip -j "$handlersBasePath/$(basename "$handlerName").zip" "$handlerName/bootstrap"

        # Remove the bootstrap file
        rm "$handlerName/bootstrap"
    fi
done