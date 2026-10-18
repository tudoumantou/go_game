# syntax=docker/dockerfile:1
# deploy/docker/{svc}.Dockerfile —— 多阶段构建（静态二进制，distroless 运行）
# 配置与密钥不打进镜像：经 compose/k8s 以只读卷挂载（19.4/19.6）
FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG SERVICE=gateway
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath \
      -ldflags "-s -w -X main.version=${VERSION:-dev}" \
      -o /out/server ./cmd/${SERVICE}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/server /app/server
# 配置/密钥由外部挂载到 /app/configs、/app/keys（只读）
USER nonroot:nonroot
ENTRYPOINT ["/app/server"]
