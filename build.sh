#!/bin/bash


mkdir -p bin


platforms=("windows/amd64" "darwin/amd64" "linux/amd64")

for platform in "${platforms[@]}"
do
    platform_split=(${platform//\// })
    GOOS=${platform_split[0]}
    GOARCH=${platform_split[1]}
    echo $GOOS / $GOARCH
    env GOOS=$GOOS GOARCH=$GOARCH go build -o ./bin/brev_${GOOS}_$GOARCH brev.go
#    echo "env GOOS=$GOOS GOARCH=$GOARCH go build -o ./bin/brev_${GOOS}_$GOARCH brev.go"
#    env GOOS=$GOOS GOARCH=$GOARCH go build -o ./bin/brev_$GOOS_$GOARCH brev.go

done


