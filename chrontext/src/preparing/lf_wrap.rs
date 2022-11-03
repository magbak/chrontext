use polars::prelude::LazyFrame;

pub struct WrapLF {
    pub lf: LazyFrame,
}

impl WrapLF {
    pub fn new(lf:LazyFrame)->WrapLF {
        WrapLF{lf}
    }
}