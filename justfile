
[working-directory: "binaries"]
loader:
    go build loader.go
    rsync -az --progress -e "ssh -i ~/.ssh/histrio-2-aws.pem" loader histrio-aws:loader

serverless:
    ./build.sh
    npx serverless@3.39.0 deploy

logs:
    rsync -avz --progress histrio-aws:time-logs/ ~/dev/phd/histrio/data/

plot output *args:
    uv run --script benchmark/compute_metrics.py -o {{output}} -e {{args}}

hotel-plot:
    just plot hotel_latency h100-noq-0 h100-mq-0 h250-noq-0 h250-mq-0 h1000-noq-0 h1000-mq-0

bank-plot:
    just plot bank_latency b100-noq-0 b100-mq-0 b250-noq-0 b250-mq-0 b1000-noq-0 b1000-mq-0


default: serverless loader