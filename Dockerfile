FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -o /shudhi .

FROM gcr.io/distroless/static-debian12
COPY --from=build /shudhi /shudhi
ENTRYPOINT ["/shudhi"]
