FROM golang:1.18.0-alpine3.15 AS builder
RUN apk add --no-cache build-base
WORKDIR /src
COPY . /src
RUN go build -o /brev /src/cmd/brev/brev.go
RUN go build -o /brevd /src/cmd/brevd/brevd.go

FROM alpine:3.15
EXPOSE 8080
RUN apk add --no-cache tzdata ca-certificates
COPY --from=builder /brev /brev
COPY --from=builder /brevd /brevd
ENTRYPOINT /brevd
