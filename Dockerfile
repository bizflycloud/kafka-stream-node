FROM hub.paas.vn/public/golang:1.21.1 AS build_base

RUN apt-get update && apt-get install -y git pkg-config

# stage 2
FROM build_base AS build_go

ENV GO111MODULE=on

WORKDIR $GOPATH/kafka-stream-node
COPY go.mod .
COPY go.sum .
RUN go mod download

# stage 3
FROM build_go AS server_builder

ENV GO111MODULE=on

COPY . .
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -gcflags="-N -l" -o /bin/kafka-stream-node ./main.go

# Stage 4
FROM hub.paas.vn/public/golang:1.21.1 AS kafka-stream-node

ENV TZ 'Asia/Ho_Chi_Minh'
RUN echo $TZ > /etc/timezone && \
    apt-get update && apt-get install -y tzdata && \
    rm /etc/localtime && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata && \
    apt-get clean

COPY --from=server_builder /bin/kafka-stream-node /bin/kafka-stream-node

# copy env
COPY --from=server_builder $GOPATH/kafka-stream-node/.env /go/.env

CMD ["/bin/kafka-stream-node"]