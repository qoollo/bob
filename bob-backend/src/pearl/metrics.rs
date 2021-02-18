use super::prelude::*;

pub const PEARL_PUT_COUNTER: &str = "pearl.put_count";
pub const PEARL_PUT_ERROR_COUNTER: &str = "pearl.put_error_count";
pub const PEARL_PUT_TIMER: &str = "pearl.put_timer";

pub const PEARL_GET_COUNTER: &str = "pearl.get_count";
pub const PEARL_GET_ERROR_COUNTER: &str = "pearl.get_error_count";
pub const PEARL_GET_TIMER: &str = "pearl.get_timer";

pub fn init_pearl() {
    counter!(PEARL_GET_COUNTER, 0);
    counter!(PEARL_PUT_COUNTER, 0);
    counter!(PEARL_GET_ERROR_COUNTER, 0);
    counter!(PEARL_PUT_ERROR_COUNTER, 0);
}
