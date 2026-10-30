# build stage
FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY cmd/ cmd/
COPY internal/ internal/
RUN CGO_ENABLED=0 go build -trimpath -o /out/andamio ./cmd/andamio

# runtime stage: no shell, no libc, minimal attack surface
FROM scratch
COPY --from=build /out/andamio /andamio
EXPOSE 7946/udp 7373
ENTRYPOINT ["/andamio"]
