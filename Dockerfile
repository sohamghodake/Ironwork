# syntax=docker/dockerfile:1
# One Dockerfile for every Ironwork service; SERVICE selects the cmd/ binary.
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG SERVICE
RUN CGO_ENABLED=0 go build -trimpath -o /out/app ./cmd/${SERVICE}

FROM alpine:3.20
COPY --from=build /out/app /usr/local/bin/app
ENTRYPOINT ["/usr/local/bin/app"]
