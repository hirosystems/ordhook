use std::ops::{Add, AddAssign};

use super::{epoch::Epoch, height::Height, *};

#[derive(Copy, Clone, Eq, PartialEq, Debug, Ord, PartialOrd, Deserialize, Serialize)]
#[serde(transparent)]
pub struct Sat(pub u64);

impl Sat {
    pub(crate) const LAST: Self = Self(Self::SUPPLY - 1);
    pub(crate) const SUPPLY: u64 = 2099999997690000;

    pub(crate) fn n(self) -> u64 {
        self.0
    }

    pub(crate) fn height(self) -> Height {
        self.epoch().starting_height() + self.epoch_position() / self.epoch().subsidy()
    }

    pub(crate) fn cycle(self) -> u64 {
        Epoch::from(self).0 / CYCLE_EPOCHS
    }

    pub(crate) fn percentile(self) -> String {
        format!("{}%", (self.0 as f64 / Self::LAST.0 as f64) * 100.0)
    }

    pub(crate) fn epoch(self) -> Epoch {
        self.into()
    }

    pub(crate) fn third(self) -> u64 {
        self.epoch_position() % self.epoch().subsidy()
    }

    pub(crate) fn epoch_position(self) -> u64 {
        self.0 - self.epoch().starting_sat().0
    }

    /// `Sat::rarity` is expensive and is called frequently when indexing.
    /// Sat::is_common only checks if self is `Rarity::Common` but is
    /// much faster.
    pub(crate) fn is_common(self) -> bool {
        let epoch = self.epoch();
        (self.0 - epoch.starting_sat().0) % epoch.subsidy() != 0
    }

    pub(crate) fn name(self) -> String {
        let mut x = Self::SUPPLY - self.0;
        let mut name = String::new();
        while x > 0 {
            name.push(
                "abcdefghijklmnopqrstuvwxyz"
                    .chars()
                    .nth(((x - 1) % 26) as usize)
                    .unwrap(),
            );
            x = (x - 1) / 26;
        }
        name.chars().rev().collect()
    }
}

impl PartialEq<u64> for Sat {
    fn eq(&self, other: &u64) -> bool {
        self.0 == *other
    }
}

impl PartialOrd<u64> for Sat {
    fn partial_cmp(&self, other: &u64) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(other)
    }
}

impl Add<u64> for Sat {
    type Output = Self;

    fn add(self, other: u64) -> Sat {
        Sat(self.0 + other)
    }
}

impl AddAssign<u64> for Sat {
    fn add_assign(&mut self, other: u64) {
        *self = Sat(self.0 + other);
    }
}

#[cfg(test)]
mod tests {
    use super::COIN_VALUE;

    use super::*;

    #[test]
    fn n() {
        assert_eq!(Sat(1).n(), 1);
        assert_eq!(Sat(100).n(), 100);
    }

    #[test]
    fn name() {
        assert_eq!(Sat(0).name(), "nvtdijuwxlp");
        assert_eq!(Sat(1).name(), "nvtdijuwxlo");
        assert_eq!(Sat(26).name(), "nvtdijuwxkp");
        assert_eq!(Sat(27).name(), "nvtdijuwxko");
        assert_eq!(Sat(2099999997689999).name(), "a");
        assert_eq!(Sat(2099999997689999 - 1).name(), "b");
        assert_eq!(Sat(2099999997689999 - 25).name(), "z");
        assert_eq!(Sat(2099999997689999 - 26).name(), "aa");
    }

    #[test]
    fn number() {
        assert_eq!(Sat(2099999997689999).n(), 2099999997689999);
    }

    #[test]
    fn epoch_position() {
        assert_eq!(Epoch(0).starting_sat().epoch_position(), 0);
        assert_eq!((Epoch(0).starting_sat() + 100).epoch_position(), 100);
        assert_eq!(Epoch(1).starting_sat().epoch_position(), 0);
        assert_eq!(Epoch(2).starting_sat().epoch_position(), 0);
    }

    #[test]
    fn subsidy_position() {
        assert_eq!(Sat(0).third(), 0);
        assert_eq!(Sat(1).third(), 1);
        assert_eq!(
            Sat(Height(0).subsidy() - 1).third(),
            Height(0).subsidy() - 1
        );
        assert_eq!(Sat(Height(0).subsidy()).third(), 0);
        assert_eq!(Sat(Height(0).subsidy() + 1).third(), 1);
        assert_eq!(
            Sat(Epoch(1).starting_sat().n() + Epoch(1).subsidy()).third(),
            0
        );
        assert_eq!(Sat::LAST.third(), 0);
    }

    #[test]
    fn supply() {
        let mut mined = 0;

        for height in 0.. {
            let subsidy = Height(height).subsidy();

            if subsidy == 0 {
                break;
            }

            mined += subsidy;
        }

        assert_eq!(Sat::SUPPLY, mined);
    }

    #[test]
    fn last() {
        assert_eq!(Sat::LAST, Sat::SUPPLY - 1);
    }

    #[test]
    fn eq() {
        assert_eq!(Sat(0), 0);
        assert_eq!(Sat(1), 1);
    }

    #[test]
    fn partial_ord() {
        assert!(Sat(1) > 0);
        assert!(Sat(0) < 1);
    }

    #[test]
    fn add() {
        assert_eq!(Sat(0) + 1, 1);
        assert_eq!(Sat(1) + 100, 101);
    }

    #[test]
    fn add_assign() {
        let mut sat = Sat(0);
        sat += 1;
        assert_eq!(sat, 1);
        sat += 100;
        assert_eq!(sat, 101);
    }

    #[test]
    fn third() {
        assert_eq!(Sat(0).third(), 0);
        assert_eq!(Sat(50 * COIN_VALUE - 1).third(), 4999999999);
        assert_eq!(Sat(50 * COIN_VALUE).third(), 0);
        assert_eq!(Sat(50 * COIN_VALUE + 1).third(), 1);
    }

    #[test]
    fn percentile() {
        assert_eq!(Sat(0).percentile(), "0%");
        assert_eq!(Sat(Sat::LAST.n() / 2).percentile(), "49.99999999999998%");
        assert_eq!(Sat::LAST.percentile(), "100%");
    }
}
