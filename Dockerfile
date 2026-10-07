# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/sisu .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/sisu /sisu
USER 65532:65532
ENTRYPOINT ["/sisu"]

