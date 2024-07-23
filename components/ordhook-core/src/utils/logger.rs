#[macro_export]
macro_rules! try_info {
    ($a:expr, $tag:expr, $($args:tt)*) => {
        $a.try_log(|l| info!(l, $tag, $($args)*));
    };
    ($a:expr, $tag:expr) => {
        $a.try_log(|l| info!(l, $tag));
    };
}

#[macro_export]
macro_rules! try_debug {
    ($a:expr, $tag:expr, $($args:tt)*) => {
        $a.try_log(|l| debug!(l, $tag, $($args)*));
    };
    ($a:expr, $tag:expr) => {
        $a.try_log(|l| debug!(l, $tag));
    };
}

#[macro_export]
macro_rules! try_warn {
    ($a:expr, $tag:expr, $($args:tt)*) => {
        $a.try_log(|l| warn!(l, $tag, $($args)*));
    };
    ($a:expr, $tag:expr) => {
        $a.try_log(|l| warn!(l, $tag));
    };
}

#[macro_export]
macro_rules! try_error {
    ($a:expr, $tag:expr, $($args:tt)*) => {
        $a.try_log(|l| error!(l, $tag, $($args)*));
    };
    ($a:expr, $tag:expr) => {
        $a.try_log(|l| error!(l, $tag));
    };
}
