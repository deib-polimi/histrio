
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

hotel-lat:
    just plot hotel_latency base-h-0 h100-noq-0  h250-noq-0 h1000-noq-0 h1000-mq-0

bank-lat:
    just plot bank_latency base-b-0 b100-noq-0  b250-noq-0 b1000-noq-0 b1000-mq-0

hotel-thr:
    just plot hotel_thr th1000-noq-w16-0 th1000-noq-w8-0 th1000-noq-w4-0 th1000-noq-w2-0 th1000-noq-w1-0 base-thr-h-0 -t throughput

bank-thr:
    just plot bank_thr tb1000-noq-w16-0 tb1000-noq-w8-0 tb1000-noq-w4-0 tb1000-noq-w2-0 tb1000-noq-w1-0 base-thr-b-0 -t throughput


default: serverless loader