use crate::stream::Stream;

pub struct Pair {
    stream_a: Option<Stream>,
    stream_b: Option<Stream>,
}

/*
impl Pair {
    pub fn swap(&mut self) {
        let temp = self.stream_a;
        self.stream_a = self.stream_b;
        self.stream_b = temp;
    }
}
*/

