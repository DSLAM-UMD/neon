use bitmaps;

const PER_LAYER_SPECIFICITY_BITS: usize = 10;
const N_BITS_PER_PAGE: usize = 2usize.pow(PER_LAYER_SPECIFICITY_BITS as u32);
const LAYER_BITMASK: usize = N_BITS_PER_PAGE - 1usize;

fn le_popcnt(it: bitmaps::Bitmap<N_BITS_PER_PAGE>, len: usize) -> usize {
    (bitmaps::Bitmap::<N_BITS_PER_PAGE>::mask(len) & it).len()
}

fn destruct(layer: usize, index: usize) -> (usize, usize) {
    let local = index
        .checked_shr((layer * PER_LAYER_SPECIFICITY_BITS) as u32)
        .unwrap_or(0)
        & LAYER_BITMASK;
    let lower = index % (N_BITS_PER_PAGE.pow(layer as u32));
    return (local, lower);
}

fn construct(layer: usize, local: usize, lower: usize) -> usize {
    return lower + (local << (layer * PER_LAYER_SPECIFICITY_BITS));
}

#[derive(Clone)]
struct LayerPageData<T: Sized + Clone> {
    map: bitmaps::Bitmap<N_BITS_PER_PAGE>,
    data: Vec<T>,
}

impl<T: Sized + Clone> Default for LayerPageData<T> {
    fn default() -> Self {
        LayerPageData {
            map: Default::default(),
            data: vec![],
        }
    }
}

impl<T: Sized + Clone> LayerPageData<T> {
    fn page_get(&self, offset: usize) -> Option<&T> {
        if self.map.get(offset) {
            let vec_off = le_popcnt(self.map.clone(), offset);
            Some(&self.data[vec_off])
        } else {
            None
        }
    }
    fn page_get_mut(&mut self, offset: usize) -> Option<&mut T> {
        if self.map.get(offset) {
            let vec_off = le_popcnt(self.map.clone(), offset);
            self.data.get_mut(vec_off)
        } else {
            None
        }
    }

    fn page_set(&mut self, offset: usize, val: T) -> Option<T> {
        let vec_off = le_popcnt(self.map.clone(), offset);
        return if self.map.get(offset) {
            let old = std::mem::replace(&mut self.data[vec_off], val);

            Some(old)
        } else {
            self.data.insert(vec_off, val);
            self.map.set(offset, true);
            None
        };
    }

    fn page_reset(&mut self, offset: usize) -> Option<T> {
        let vec_off = le_popcnt(self.map.clone(), offset);
        return if self.map.get(offset) {
            self.map.set(offset, false);
            Some(self.data.remove(vec_off))
        } else {
            None
        };
    }

    fn page_len(&self) -> usize {
        return self.data.len();
    }
}

#[derive(Clone)]
enum BitmappedMapPage<T: Sized + Clone> {
    LeafPage {
        data: LayerPageData<T>,
    },
    InnerPage {
        data: LayerPageData<Box<BitmappedMapPage<T>>>,
        n_items: usize,
        layer: usize,
    },
}

impl<T: Sized + Clone> BitmappedMapPage<T> {
    fn page_get(&self, index: usize) -> Option<&T> {
        match self {
            BitmappedMapPage::LeafPage { data } => data.page_get(index),
            BitmappedMapPage::InnerPage { data, layer, .. } => {
                let (local, next) = destruct(*layer, index);

                if let Some(res) = data.page_get(local) {
                    res.as_ref().page_get(next)
                } else {
                    None
                }
            }
        }
    }

    fn page_set(&mut self, index: usize, value: T) -> Option<T> {
        match self {
            BitmappedMapPage::LeafPage { data } => data.page_set(index, value),
            BitmappedMapPage::InnerPage {
                data,
                layer,
                n_items,
            } => {
                let (local, lower) = destruct(*layer, index);

                let upper_res: Option<T>;

                if let Some(res) = data.page_get_mut(local) {
                    upper_res = res.as_mut().page_set(lower, value);
                } else {
                    data.page_set(local, Box::new(BitmappedMapPage::new(*layer - 1)));
                    upper_res = data
                        .page_get_mut(local)
                        .unwrap()
                        .as_mut()
                        .page_set(lower, value);
                }
                // If the child didn't have an entry at this place, we inserted a new item.
                // In that case, increase our count by one.
                if upper_res.is_none() {
                    *n_items += 1;
                }
                return upper_res;
            }
        }
    }

    fn page_reset(&mut self, index: usize) -> Option<T> {
        match self {
            BitmappedMapPage::LeafPage { data } => data.page_reset(index),
            BitmappedMapPage::InnerPage {
                data,
                n_items,
                layer,
            } => {
                let (local, lower) = destruct(*layer, index);

                if let Some(page) = data.page_get_mut(local) {
                    let upper_res: Option<T> = page.as_mut().page_reset(lower);

                    if upper_res.is_some() {
                        *n_items = *n_items - 1;

                        if page.as_mut().page_len() == 0 {
                            data.page_reset(local);
                        }
                    }
                    return upper_res;
                } else {
                    None
                }
            }
        }
    }

    fn page_len(&self) -> usize {
        match self {
            BitmappedMapPage::LeafPage { data } => data.page_len(),
            BitmappedMapPage::InnerPage { n_items, .. } => *n_items,
        }
    }

    fn first_index(&self) -> Option<usize> {
        match self {
            BitmappedMapPage::LeafPage { data } => data.map.first_index(),
            BitmappedMapPage::InnerPage { data, layer, .. } => {
                data.map.first_index().and_then(|upper| {
                    data.page_get(upper).and_then(|page| {
                        page.as_ref()
                            .first_index()
                            .map(|lower| construct(*layer, upper, lower))
                    })
                })
            }
        }
    }

    fn next_index(&self, index: usize) -> Option<usize> {
        match self {
            BitmappedMapPage::LeafPage { data } => {
                return data.map.next_index(index);
            }
            BitmappedMapPage::InnerPage { data, layer, .. } => {
                let (local, lower) = destruct(*layer, index);

                let result;
                if lower + 1 < N_BITS_PER_PAGE {
                    result = data
                        .page_get(local)
                        .and_then(|b| b.as_ref().next_index(lower))
                } else {
                    result = None
                }

                if result.is_some() {
                    return result;
                }

                return data.map.next_index(local).and_then(|index| {
                    data.page_get(index)
                        .and_then(|page| page.as_ref().first_index())
                });
            }
        }
    }

    fn new(depth: usize) -> BitmappedMapPage<T> {
        if depth == 0 {
            BitmappedMapPage::LeafPage {
                data: Default::default(),
            }
        } else {
            BitmappedMapPage::InnerPage {
                data: Default::default(),
                n_items: 0,
                layer: depth,
            }
        }
    }
}

#[derive(Clone)]
pub struct LayeredBitmap<T: Sized + Clone> {
    data: BitmappedMapPage<T>,
}

impl<T: Sized + Clone> LayeredBitmap<T> {
    pub fn get(&self, index: usize) -> Option<&T> {
        self.data.page_get(index)
    }

    pub fn set(&mut self, index: usize, value: T) -> Option<T> {
        self.data.page_set(index, value)
    }

    pub fn reset(&mut self, index: usize) -> Option<T> {
        self.data.page_reset(index)
    }

    pub fn len(&self) -> usize {
        self.data.page_len()
    }

    pub fn new(depth: usize) -> LayeredBitmap<T> {
        LayeredBitmap {
            data: BitmappedMapPage::<T>::new(depth),
        }
    }

    fn next(&self, index: usize) -> Option<usize> {
        self.data.next_index(index)
    }
    fn first(&self) -> Option<usize> {
        self.data.first_index()
    }
}

impl LayeredBitmap<()> {
    pub fn get_bool(&self, index: usize) -> bool {
        self.data.page_get(index).is_some()
    }

    pub fn set_bool(&mut self, index: usize, value: bool) -> bool {
        if value {
            self.data.page_set(index, ()).is_some()
        } else {
            self.data.page_reset(index).is_some()
        }
    }

    pub fn reset_bool(&mut self, index: usize) -> bool {
        self.data.page_reset(index).is_some()
    }
}

struct BitMapIterator<'a, T: Sized + Clone> {
    map: &'a LayeredBitmap<T>,
    current_index: Option<usize>,
}

impl<'a, T: Sized + Clone> Iterator for BitMapIterator<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        let result;
        match self.current_index {
            None => result = self.map.first(),
            Some(index) => {
                if index == usize::MAX {
                    return None;
                }
                result = self.map.next(index)
            }
        }

        match result {
            None => {
                self.current_index = Some(usize::MAX);
                None
            }
            Some(index) => {
                self.current_index = Some(index);
                self.map.get(index)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn length_checks() {
        const INDEX: usize = 0;

        let mut map: LayeredBitmap<()> = LayeredBitmap::new(0);
        assert_eq!(map.len(), 0);
        map.set(INDEX, ());
        map.set(INDEX + 1, ());
        assert_eq!(map.len(), 2);
        map.set(INDEX, ());
        assert_eq!(map.len(), 2);

        // Also correctly decrease lengths
        map.reset(INDEX);
        assert_eq!(map.len(), 1);
        map.reset(INDEX);
        assert_eq!(map.len(), 1);
        map.reset(INDEX + 1);
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn length_checks_multilayer() {
        const INDEX: usize = 0;

        let mut map: LayeredBitmap<()> = LayeredBitmap::new(0);
        assert_eq!(map.len(), 0);
        map.set(INDEX, ());
        map.set(INDEX + 1, ());
        assert_eq!(map.len(), 2);
        map.set(INDEX, ());
        assert_eq!(map.len(), 2);

        // Also correctly decrease lengths
        map.reset(INDEX);
        assert_eq!(map.len(), 1);
        map.reset(INDEX);
        assert_eq!(map.len(), 1);
        map.reset(INDEX + 1);
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn data_checks() {
        const INDEX: usize = 0;

        let mut map: LayeredBitmap<u8> = LayeredBitmap::new(0);
        let res = map.set(INDEX, 1);
        assert_eq!(res, None);
        let res = map.get(INDEX);
        assert_eq!(res, Option::<&u8>::Some(&1));
        let res = map.set(INDEX, 2);
        assert_eq!(res, Some(1));
        map.set(INDEX + 1, 3);
        let res = map.get(INDEX + 1);
        assert_eq!(res, Some(&3));
    }

    #[test]
    fn data_checks_multilayer() {
        const INDEX: usize = 3usize * N_BITS_PER_PAGE;

        let mut map: LayeredBitmap<u8> = LayeredBitmap::new(2);
        let res = map.set(INDEX, 1);
        assert_eq!(res, None);
        let res = map.get(INDEX);
        assert_eq!(res, Option::<&u8>::Some(&1));
        let res = map.set(INDEX, 2);
        assert_eq!(res, Some(1));
        map.set(INDEX + 1, 3);
        let res = map.get(INDEX + 1);
        assert_eq!(res, Some(&3));
    }
}
