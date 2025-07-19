use std::{collections::HashMap, fmt, num::NonZeroUsize};

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Token(Option<NonZeroUsize>);

impl Token {
    pub fn zero() -> Self {
        Token(None)
    }

    #[allow(dead_code)]
    pub fn is_zero(&self) -> bool {
        self.0.is_none()
    }
}

impl fmt::Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(value) => write!(f, "Token({value})"),
            None => write!(f, "Token(0)"),
        }
    }
}

pub(crate) struct TokenMap<K> {
    counter: NonZeroUsize,
    map: HashMap<K, Token>,
}

impl<K> fmt::Debug for TokenMap<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenMap")
            .field("counter", &self.counter)
            .finish()
    }
}

impl<K> Default for TokenMap<K> {
    fn default() -> Self {
        Self {
            counter: NonZeroUsize::new(1).unwrap(),
            map: HashMap::new(),
        }
    }
}

impl<K> TokenMap<K>
where
    K: Eq + std::hash::Hash,
{
    pub fn insert(&mut self, key: K) -> Token {
        *self.map.entry(key).or_insert_with(|| {
            let token = Token(Some(self.counter));
            self.counter = self
                .counter
                .checked_add(1)
                .or(NonZeroUsize::new(1))
                .unwrap();
            token
        })
    }
}

#[cfg(test)]
pub(crate) mod test_key {

    use super::*;

    #[test]
    fn token_wrap() {
        let mut map = TokenMap {
            counter: NonZeroUsize::new(usize::MAX).unwrap(),
            ..Default::default()
        };
        let foo = map.insert("key");
        assert_eq!(foo.0, Some(NonZeroUsize::new(usize::MAX).unwrap()));

        let bar = map.insert("bar");
        assert_eq!(bar.0, Some(NonZeroUsize::new(1).unwrap()));

        assert_eq!(map.insert("bar"), bar);
        assert_ne!(map.insert("key"), bar);
    }
}
