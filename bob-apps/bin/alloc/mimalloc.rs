#[cfg(all(
    any(feature = "mimalloc", feature = "mimalloc-secure"),
    target_arch = "x86_64",
    target_env = "musl",
    target_pointer_width = "64"
))]
use mimalloc::MiMalloc;

#[cfg(all(
    any(feature = "mimalloc", feature = "mimalloc-secure"),
    target_arch = "x86_64",
    target_env = "musl",
    target_pointer_width = "64"
))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;
