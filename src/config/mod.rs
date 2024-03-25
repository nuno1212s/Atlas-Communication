use getset::CopyGetters;

#[derive(CopyGetters)]
pub struct ClientPoolConfig {
    #[get_copy = "pub"]
    batch_limit: usize,
    #[get_copy = "pub"]
    per_client_bound: usize,
    #[get_copy = "pub"]
    clients_per_pool: usize,
    #[get_copy = "pub"]
    batch_timeout_micros: u64,
    #[get_copy = "pub"]
    batch_sleep_micros: u64,
    #[get_copy = "pub"]
    channel_size: usize,
}

impl ClientPoolConfig {
    pub fn new(
        batch_limit: usize,
        per_client_bound: usize,
        clients_per_pool: usize,
        batch_timeout_micros: u64,
        batch_sleep_micros: u64,
        channel_size: usize,
    ) -> Self {
        Self {
            batch_limit,
            per_client_bound,
            clients_per_pool,
            batch_timeout_micros,
            batch_sleep_micros,
            channel_size,
        }
    }
}

impl Default for ClientPoolConfig {
    fn default() -> Self {
        Self {
            batch_limit: 100,
            per_client_bound: 100,
            clients_per_pool: 100,
            batch_timeout_micros: 1000,
            batch_sleep_micros: 1000,
            channel_size: 128,
        }
    }
}

#[derive(CopyGetters)]
pub struct UnpooledConnection {
    #[get_copy = "pub"]
    channel_size: usize,
}

impl Default for UnpooledConnection {
    fn default() -> Self {
        Self { channel_size: 1024 }
    }
}
