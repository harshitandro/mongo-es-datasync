FROM golang:1.12.6-alpine3.9 as builder
MAINTAINER harshitandro@gmail.com

RUN mkdir /project \
 && apk add git

COPY src /project/src
COPY app_conf.json /project/app_conf.json

COPY go.mod /project/go.mod
COPY go.sum /project/go.sum
COPY main.go /project/main.go

RUN cd /project \
 && go mod download \
 && go mod verify \
 && go install -i



FROM golang:1.12.6-alpine3.9
MAINTAINER harshitandro@gmail.com

COPY --from=builder /go/bin/mongo-es-datasync /go/bin/mongo-es-datasync
COPY --from=builder /project/app_conf.json /etc/mongo-es-sync/app_conf.json

CMD ["/go/bin/mongo-es-datasync"]