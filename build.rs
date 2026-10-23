// Компиляция raiden_binary.proto в Rust-биндинги для Pulse gRPC.
// protox компилит на чистом Rust, так что protoc в системе не нужен.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let fds = protox::compile(["proto/raiden_binary.proto"], ["proto"])?;
    tonic_build::configure()
        .build_server(false)
        .compile_fds(fds)?;
    println!("cargo:rerun-if-changed=proto/raiden_binary.proto");
    Ok(())
}
