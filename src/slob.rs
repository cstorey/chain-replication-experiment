use std::collections::HashMap;
use std::ops::{Index, IndexMut};
use std::iter;
use std::usize;
use rand::{XorShiftRng, weak_rng};
use rand::distributions::{IndependentSample, Range};
use mio;

pub struct Slab<T> {
    range: Range<usize>,
    rng: XorShiftRng,
    contents: HashMap<mio::Token, T>,
}

struct SlabIterMut<'a, T: 'a>(::std::collections::hash_map::IterMut<'a, mio::Token, T>);

impl<T> Slab<T> {
    pub fn new() -> Self {
        Self::new_range(usize::MIN, usize::MAX)
    }
    pub fn new_range(min: usize, max: usize) -> Self {
        let rng = weak_rng();
        Slab {
            range: Range::new(min, max),
            rng: rng,
            contents: HashMap::new(),
        }
    }

    pub fn insert_with<F: FnOnce(mio::Token) -> T>(&mut self, f: F) -> Option<mio::Token> {
        let &mut Slab { ref range, ref mut rng, ref mut contents } = self;
        let idx = iter::repeat(())
                      .map(|()| range.ind_sample(rng))
                      .skip_while(|&idx| contents.contains_key(&mio::Token(idx)))
                      .map(mio::Token)
                      .next()
                      .expect("random index");

        let val = f(idx);
        contents.insert(idx, val);
        Some(idx)
    }

    pub fn remove(&mut self, token: mio::Token) -> Option<T> {
        self.contents.remove(&token)
    }
    pub fn get_mut(&mut self, token: mio::Token) -> Option<&mut T> {
        self.contents.get_mut(&token)
    }

    pub fn iter_mut(&mut self) -> SlabIterMut<T> {
        SlabIterMut(self.contents.iter_mut())
    }
}

impl<'a, T> iter::Iterator for SlabIterMut<'a, T> {
    type Item = &'a mut T;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(_, v)| v)
    }
}

impl<T> Index<mio::Token> for Slab<T> {
    type Output = T;
    fn index(&self, index: mio::Token) -> &Self::Output {
        Index::index(&self.contents, &index)
    }
}

impl<T> IndexMut<mio::Token> for Slab<T> {
    fn index_mut(&mut self, index: mio::Token) -> &mut Self::Output {
        self.contents.get_mut(&index).expect("index_mut")
    }
}
