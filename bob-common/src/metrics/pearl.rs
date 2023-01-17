pub const PEARL_PUT_COUNTER: &str = "pearl.put_count";
pub const PEARL_PUT_ERROR_COUNTER: &str = "pearl.put_error_count";
pub const PEARL_PUT_TIMER: &str = "pearl.put_timer";
pub const PEARL_PUT_BYTES_COUNTER: &str = "pearl.put_bytes_count";

pub const PEARL_GET_COUNTER: &str = "pearl.get_count";
pub const PEARL_GET_ERROR_COUNTER: &str = "pearl.get_error_count";
pub const PEARL_GET_TIMER: &str = "pearl.get_timer";
pub const PEARL_GET_BYTES_COUNTER: &str = "pearl.get_bytes_count";

pub const PEARL_DELETE_COUNTER: &str = "pearl.delete_count";
pub const PEARL_DELETE_ERROR_COUNTER: &str = "pearl.delete_error_count";
pub const PEARL_DELETE_TIMER: &str = "pearl.delete_timer";

pub const PEARL_EXIST_COUNTER: &str = "pearl.exist_count";
pub const PEARL_EXIST_ERROR_COUNTER: &str = "pearl.exist_error_count";
pub const PEARL_EXIST_TIMER: &str = "pearl.exist_timer";

pub fn init_pearl() {
    counter!(PEARL_GET_COUNTER, 0);
    counter!(PEARL_PUT_COUNTER, 0);
    counter!(PEARL_GET_ERROR_COUNTER, 0);
    counter!(PEARL_PUT_ERROR_COUNTER, 0);
    counter!(PEARL_DELETE_COUNTER, 0);
    counter!(PEARL_DELETE_ERROR_COUNTER, 0);
    counter!(PEARL_EXIST_COUNTER, 0);
    counter!(PEARL_EXIST_ERROR_COUNTER, 0);
}
