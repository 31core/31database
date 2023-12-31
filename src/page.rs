use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io::{Result as IOResult, *};
use std::rc::Rc;

pub const PAGE_SIZE: usize = 4096;

const BITMAP_MANAGED_SIZE: usize = PAGE_SIZE * 8;

pub const PAGE_TYPEID_BTREE_INTERNAL: u8 = 1;
pub const PAGE_TYPEID_BTREE_LEAF: u8 = 2;
pub const PAGE_TYPEID_CONTENT: u8 = 3;
pub const PAGE_TYPEID_OVERFLOW: u8 = 4;

const OVERFLOWPAGE_AVAILABLE_SIZE: usize = PAGE_SIZE - 3;
const OVERFLOWED_OVERFLOWPAGE_AVAILABLE_SIZE: usize = PAGE_SIZE - 3 - 8;

#[derive(Clone, Copy)]
pub enum PageType {
    General,
    BtreePage,
    BitmapPage,
    ContentPage,
    OverflowPage,
}

#[derive(Clone, Copy)]
pub struct Page {
    pub page_type: PageType,
    pub count: u64,
    pub syncd: bool,
    pub data: [u8; PAGE_SIZE],
}

impl Page {
    pub fn new(count: u64, page_type: PageType) -> Self {
        Self {
            page_type,
            count,
            syncd: false,
            data: [0; PAGE_SIZE],
        }
    }
    /** Load page from disk */
    pub fn load<R>(reader: &mut R, count: u64) -> IOResult<Self>
    where
        R: Read + Seek,
    {
        let mut data = [0; PAGE_SIZE];
        reader.seek(SeekFrom::Start(count * PAGE_SIZE as u64))?;
        reader.read_exact(&mut data)?;

        Ok(Self {
            page_type: PageType::General,
            count,
            syncd: true,
            data,
        })
    }
    /** Sync to disk */
    pub fn sync<D>(&mut self, writer: &mut D) -> IOResult<()>
    where
        D: Write + Seek,
    {
        if !self.syncd {
            writer.seek(SeekFrom::Start(self.count * PAGE_SIZE as u64))?;
            writer.write_all(&self.data)?;
            self.syncd = true;
        }
        Ok(())
    }
    pub fn modify(&mut self, data: &[u8; PAGE_SIZE]) {
        self.data = *data;
        self.syncd = false;
    }
}

pub struct BitmapPage {
    page: Page,
}

impl BitmapPage {
    pub fn new(count: u64) -> Self {
        Self {
            page: Page::new(count, PageType::BitmapPage),
        }
    }
    pub fn get_used(&self, count: u64) -> bool {
        let byte = count as usize / 8;
        let bit = count as usize % 8;
        self.page.data[byte] >> (7 - bit) << 7 != 0
    }
    pub fn set_used(&mut self, count: u64) {
        let byte = count as usize / 8;
        let bit = count as usize % 8;
        self.page.data[byte] |= 1 << (7 - bit);
    }
    pub fn set_unused(&mut self, count: u64) {
        let byte = count as usize / 8;
        let bit = count as usize % 8;
        self.page.data[byte] &= !(1 << (7 - bit));
    }
    pub fn find_unused(&self) -> Option<u64> {
        for (i, byte) in self.page.data.iter().enumerate() {
            if *byte != 255 {
                for j in 0..8 {
                    let position = (i * 8 + j) as u64;
                    if !self.get_used(position) {
                        return Some(position);
                    }
                }
            }
        }
        None
    }
}

#[derive(Default, Clone, Debug)]
pub struct ContentEntry {
    pub data: Vec<u8>,
    pub overflow_page: Option<u64>,
}

impl ContentEntry {
    pub fn from_bytes<D>(device: &mut D, mgr: &mut PageManage, data: &[u8]) -> IOResult<Self>
    where
        D: Write + Read + Seek,
    {
        let mut entry = ContentEntry::default();

        /* requires overflow page */
        if data.len() > PAGE_SIZE - 5 {
            entry.data = data[..PAGE_SIZE - 12].to_owned();
            let mut data = &data[PAGE_SIZE - 12..];

            let mut last_count = None;
            let mut last_page: Option<OverflowPage> = None;
            let mut overflow_page_count = mgr.alloc(device, PageType::OverflowPage)?.borrow().count;
            entry.overflow_page = Some(overflow_page_count);
            loop {
                let mut overflow_page = OverflowPage::default();
                if last_page.is_some() {
                    let mut last = last_page.clone().unwrap();
                    last.next = Some(overflow_page_count);
                    last_page = Some(last);
                }

                overflow_page.put_data(data);
                if let Some(count) = last_count {
                    if let Some(page) = &last_page {
                        mgr.modify(device, count, &page.dump())?;
                    }
                }

                last_count = Some(overflow_page_count);
                last_page = Some(overflow_page.clone());
                data = &data[overflow_page.data.len()..];

                if data.is_empty() {
                    mgr.modify(device, overflow_page_count, &overflow_page.dump())?;
                    break;
                }
                overflow_page_count = mgr.alloc(device, PageType::OverflowPage)?.borrow().count;
            }
        } else {
            entry.data = data.to_owned();
        }
        Ok(entry)
    }
    /** Summary used size (not including overflowed part) */
    pub fn total_size(&self) -> usize {
        if self.overflow_page.is_none() {
            2 + self.data.len()
        } else {
            2 + 8 + self.data.len()
        }
    }
    pub fn precalculate_size(size: usize, overflowed: bool) -> usize {
        if overflowed {
            size + 2 + 8
        } else {
            size + 2
        }
    }
}

#[derive(Default, Debug)]
/**
 * # Data structure:
 *
 * |Start|End |Description|
 * |-----|----|-----------|
 * |0    |1   |Page type  |
 * |1    |2   |Count of entries|
 * |2    |4096|Entries    |
 *
 * ## Entry
 * Entry:
 *
 * |Start|End|Description|
 * |-----|---|-----------|
 * |0    |2  |Lenth      |
 *
 * Entry with overflow pages:
 *
 * |Start|End|Description|
 * |-----|---|-----------|
 * |0    |2  |Lenth      |
 * |2    |10 |Overflow page|
 */
pub struct ContentPage {
    pub entries: Vec<ContentEntry>,
}

impl ContentPage {
    /** Load from bytes */
    pub fn load(page_data: &[u8; PAGE_SIZE]) -> Self {
        let mut page = Self::default();
        let entries_len = page_data[1] as usize;
        let mut ptr = 2;
        for _ in 0..entries_len {
            let mut entry = ContentEntry::default();
            let mut size = u16::from_be_bytes(page_data[ptr..ptr + 2].try_into().unwrap());
            ptr += 2;
            if size >> 15 == 1 {
                size &= !0 << 1 >> 1;
                entry.overflow_page = Some(u64::from_be_bytes(
                    page_data[ptr..ptr + 8].try_into().unwrap(),
                ));
                ptr += 8;
            }
            entry.data = page_data[ptr..ptr + size as usize].to_vec();
            ptr += size as usize;
            page.entries.push(entry);
        }
        page
    }
    /** Dump to bytes */
    pub fn dump(&self) -> [u8; PAGE_SIZE] {
        let mut page_data = [0; PAGE_SIZE];
        page_data[0] = PAGE_TYPEID_CONTENT;
        page_data[1] = self.entries.len() as u8;
        let mut ptr = 2;
        for entry in &self.entries {
            let mut size = entry.data.len() as u16;
            if let Some(overflow_page) = entry.overflow_page {
                size |= 1 << 15;
                page_data[ptr..ptr + 2].copy_from_slice(&size.to_be_bytes());
                ptr += 2;
                page_data[ptr..ptr + 8].copy_from_slice(&overflow_page.to_be_bytes());
                ptr += 8;
            } else {
                page_data[ptr..ptr + 2].copy_from_slice(&size.to_be_bytes());
                ptr += 2;
            }
            page_data[ptr..ptr + entry.data.len()].copy_from_slice(&entry.data);
            ptr += entry.data.len();
        }
        page_data
    }
    /** Push a content entry */
    pub fn push(&mut self, entry: ContentEntry) -> std::result::Result<(), ()> {
        if self.total_size() + entry.total_size() <= PAGE_SIZE {
            self.entries.push(entry);
            Ok(())
        } else {
            Err(())
        }
    }
    /** Summary used size */
    pub fn total_size(&self) -> usize {
        let mut size = 2;
        for entry in &self.entries {
            size += entry.total_size();
        }
        size
    }
}

#[derive(Clone, Default, Debug)]
pub struct OverflowPage {
    pub data: Vec<u8>,
    pub next: Option<u64>,
}

impl OverflowPage {
    /** Load from bytes */
    pub fn load(data: &[u8; PAGE_SIZE]) -> Self {
        let mut page = Self::default();
        let size = u16::from_be_bytes(data[1..3].try_into().unwrap());
        if size >> 15 == 1 {
            let size = size << 1 >> 1;
            page.data = data[11..11 + size as usize].to_owned();
            page.next = Some(u64::from_be_bytes(data[3..11].try_into().unwrap()));
        } else {
            page.data = data[3..3 + size as usize].to_owned();
        }

        page
    }
    /** Dump to bytes */
    pub fn dump(&self) -> [u8; PAGE_SIZE] {
        let mut data = [0; PAGE_SIZE];
        data[0] = PAGE_TYPEID_OVERFLOW;
        if let Some(next) = self.next {
            data[1..3].copy_from_slice(&(self.data.len() as u16 | (1 << 15)).to_be_bytes()); // write size
            data[3..11].copy_from_slice(&next.to_be_bytes()); // write the next overflow page
            data[11..11 + self.data.len()].copy_from_slice(&self.data);
        } else {
            data[1..3].copy_from_slice(&(self.data.len() as u16).to_be_bytes()); // write size
            data[3..3 + self.data.len()].copy_from_slice(&self.data);
        }

        data
    }
    pub fn put_data(&mut self, data: &[u8]) {
        /* overflowed */
        if data.len() > OVERFLOWPAGE_AVAILABLE_SIZE {
            self.data = data[..OVERFLOWED_OVERFLOWPAGE_AVAILABLE_SIZE].to_owned();
        } else {
            self.data = data.to_owned();
        }
    }
}

#[derive(Default)]
pub struct PageManage {
    pages: BTreeMap<u64, Rc<RefCell<Page>>>,
    pub cache_size: usize,
    cache_pages: Vec<u64>,
}

impl PageManage {
    /** Find ot allocate an unused page */
    fn find_unused_page<D>(&mut self, device: &mut D) -> IOResult<u64>
    where
        D: Write + Read + Seek,
    {
        let mut bitmap_count = 0;
        loop {
            let mut bitmap_page = BitmapPage::new(bitmap_count);
            if let Ok(page) = self.get(device, bitmap_count) {
                bitmap_page.page = *page.borrow();
            } else {
                bitmap_page.page = *self
                    .alloc_with_count(device, bitmap_count, PageType::BitmapPage)
                    .borrow();
            }
            bitmap_page.set_used(0); // set bitmap page as used
            if let Some(count) = bitmap_page.find_unused() {
                bitmap_page.set_used(count);
                self.modify(device, bitmap_count, &bitmap_page.page.data)?;
                let count = count + bitmap_count;
                return Ok(count);
            }
            bitmap_count += BITMAP_MANAGED_SIZE as u64 + 1;
        }
    }
    /** Allocate a new page */
    pub fn alloc<D>(&mut self, device: &mut D, page_type: PageType) -> IOResult<Rc<RefCell<Page>>>
    where
        D: Write + Read + Seek,
    {
        self.limit_cache(device);
        let count = self.find_unused_page(device)?;
        let page = Page::new(count, page_type);
        let count = page.count;
        self.cache_pages.push(count);

        self.pages.insert(page.count, Rc::new(RefCell::new(page)));

        Ok(Rc::clone(self.pages.get(&count).unwrap()))
    }
    /** Allocate a new page with specified count */
    pub fn alloc_with_count<D>(
        &mut self,
        device: &mut D,
        count: u64,
        page_type: PageType,
    ) -> Rc<RefCell<Page>>
    where
        D: Write + Read + Seek,
    {
        self.limit_cache(device);
        let page = Page::new(count, page_type);
        let count = page.count;
        self.cache_pages.push(count);

        self.pages.insert(page.count, Rc::new(RefCell::new(page)));

        Rc::clone(self.pages.get(&count).unwrap())
    }
    /** Get page by count */
    pub fn get<D>(&mut self, device: &mut D, page_count: u64) -> IOResult<Rc<RefCell<Page>>>
    where
        D: Write + Read + Seek,
    {
        if let Some(page) = self.pages.get(&page_count) {
            return Ok(Rc::clone(page));
        }
        /* page does not loaded into memory */
        self.limit_cache(device);
        let page_res = Page::load(device, page_count);
        if let Ok(page) = page_res {
            self.cache_pages.push(page_count);
            self.pages.insert(page_count, Rc::new(RefCell::new(page)));
        } else {
            return Err(Error::new(ErrorKind::Other, ""));
        }
        self.get(device, page_count)
    }
    /** Sync all pages to disk */
    pub fn sync_all<W>(&mut self, writer: &mut W) -> IOResult<()>
    where
        W: Write + Seek,
    {
        for (_, i) in self.pages.iter() {
            i.borrow_mut().sync(writer)?;
        }
        Ok(())
    }
    /** Release ununsed page */
    pub fn release<D>(&mut self, device: &mut D, page_count: u64)
    where
        D: Write + Read + Seek,
    {
        self.pages.remove(&page_count);
        let bitmap_count =
            (page_count as usize / (BITMAP_MANAGED_SIZE + 1)) * (BITMAP_MANAGED_SIZE + 1);
        let bitmap_page = self.get(device, bitmap_count as u64);
        let mut bitmap = BitmapPage::new(bitmap_count as u64);
        bitmap.set_unused(page_count % (BITMAP_MANAGED_SIZE + 1) as u64);
        bitmap_page.unwrap().borrow_mut().modify(&bitmap.page.data);
    }
    /** Find or allocate a page by type */
    pub fn find_page_by_type<D>(
        &mut self,
        device: &mut D,
        start: u64,
        page_type: u8,
    ) -> IOResult<u64>
    where
        D: Write + Read + Seek,
    {
        let mut page_count = start;
        loop {
            /* is a bitmap page */
            if page_count % BITMAP_MANAGED_SIZE as u64 + 1 == 0 {
                page_count += 1;
                continue;
            }
            if let Ok(page) = self.get(device, page_count) {
                if page.borrow().data[0] == page_type {
                    return Ok(page_count);
                }
            } else {
                self.alloc(device, PageType::General)?;
                self.get(device, page_count).unwrap().borrow_mut().data[0] = page_type;
                return Ok(page_count);
            }
            page_count += 1;
        }
    }
    /** Modify a apge */
    pub fn modify<D>(
        &mut self,
        device: &mut D,
        page_count: u64,
        data: &[u8; PAGE_SIZE],
    ) -> IOResult<()>
    where
        D: Write + Read + Seek,
    {
        self.get(device, page_count)?.borrow_mut().modify(data);
        Ok(())
    }
    /** Get page data */
    pub fn get_data<D>(&mut self, device: &mut D, page_count: u64) -> IOResult<[u8; PAGE_SIZE]>
    where
        D: Write + Read + Seek,
    {
        Ok(self.get(device, page_count)?.borrow().data)
    }
    /** Limit the cache size to self.cache_size */
    fn limit_cache<D>(&mut self, device: &mut D)
    where
        D: Write + Read + Seek,
    {
        if self.cache_pages.len() >= self.cache_size {
            self.pages[&self.cache_pages[0]]
                .borrow_mut()
                .sync(device)
                .unwrap();
            self.pages.remove(&self.cache_pages[0]);
            self.cache_pages.remove(0);
        };
    }
}
