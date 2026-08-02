// The engine is a library (`celld`) with a thin binary entry; all the logic
// lives in the lib, and this only launches it.
fn main() -> anyhow::Result<()> {
    celld::run()
}
