pub trait InspectErr<T, E> {
    fn inspect_err<F>(self, f: F) -> Result<T, E>
    where
        F: FnOnce(&E);
}

impl<T, E> InspectErr<T, E> for Result<T, E> {
    fn inspect_err<F>(self, f: F) -> Result<T, E>
    where
        F: FnOnce(&E),
    {
        if let Err(ref e) = self {
            f(e);
        }

        self
    }
}
