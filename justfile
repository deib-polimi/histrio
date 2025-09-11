
[working-directory: "binaries"]
loader:
    go build loader.go
    rsync -az --progress -e "ssh -i ~/.ssh/histrio-2-aws.pem" loader histrio-aws:loader

serverless:
    ./build.sh
    npx serverless@3.39.0 deploy



default: serverless loader