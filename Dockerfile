FROM rust:bookworm as build

WORKDIR /opt/build

COPY . .

RUN cargo build --release


FROM debian:bookworm

WORKDIR /app

COPY --from=build /opt/build/target/release/rode .

EXPOSE 80/tcp

CMD [ "./rode" ]
