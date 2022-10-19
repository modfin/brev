FROM golang:1.19.2-alpine3.16 AS builder
RUN apk add --no-cache build-base
WORKDIR /src
COPY . /src
RUN go build -o /brev /src/cmd/brev/brev.go
RUN go build -o /brevd /src/cmd/brevd/brevd.go

FROM alpine:3.16
EXPOSE 8080
RUN apk add --no-cache tzdata ca-certificates
COPY --from=builder /brev /brev
COPY --from=builder /brevd /brevd
VOLUME /var/lib/brev
ENTRYPOINT /brevd
