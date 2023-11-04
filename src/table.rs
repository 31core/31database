use crate::btree::*;
use crate::page::*;
use std::io::*;

pub fn location_to_u64(content_page_count: u64, offset: u8) -> u64 {
    content_page_count << 8 | offset as u64
}

pub fn location_from_u64(u64_val: u64) -> (u64, u8) {
    (u64_val >> 8, (u64_val & 255) as u8)
}

#[derive(Debug)]
pub enum ValueType {
    Number,
    Bytes,
}

#[derive(Debug)]
pub struct Value {
    pub value_type: ValueType,
    pub data: Vec<u8>,
}

impl Value {
    pub fn new(value_type: ValueType, data: &[u8]) -> Self {
        Self {
            value_type,
            data: data.to_vec(),
        }
    }
}

#[derive(Default)]
pub struct Record {
    pub rowid: u64,
    pub values: Vec<Value>,
    pub location: Vec<(u64, u8)>,
}

impl Record {
    /** Write a record into page */
    pub fn write_to_page<D>(&self, device: &mut D, mgr: &mut PageManage)
    where
        D: Write + Read + Seek,
    {
        for (i, val) in self.values.iter().enumerate() {
            let page = mgr.get(device, self.location[i].0).unwrap();
            let mut content_page = ContentPage::load(&page.borrow().data);
            content_page.entries[self.location[i].1 as usize].data = val.data.clone();
            page.borrow_mut().modify(&content_page.dump());
        }
    }
    /** Add a new record into page */
    pub fn add_to_page<D>(
        &mut self,
        device: &mut D,
        mgr: &mut PageManage,
        value: Value,
    ) -> std::result::Result<(), ()>
    where
        D: Write + Read + Seek,
    {
        let mut page_count = 0;
        let mut content_page;
        loop {
            page_count = mgr.find_page_by_type(device, page_count + 1, PAGE_TYPEID_CONTENT);
            content_page = ContentPage::load(&mgr.get(device, page_count).unwrap().borrow().data);
            if value.data.len() <= PAGE_SIZE - content_page.total_size() {
                break;
            }
        }
        let entry = ContentEntry {
            data: value.data.clone(),
            ..Default::default()
        };

        content_page.push(entry)?;
        self.values.push(value);
        self.location
            .push((page_count, (content_page.entries.len() - 1) as u8));
        mgr.modify(device, page_count, &content_page.dump());
        Ok(())
    }
}

#[derive(Default)]
pub struct Table {
    pub root_node: BtreeNode,
    pub value_types: Vec<ValueType>,
}

impl Table {
    /** Query a record by rowid */
    pub fn query<D>(&self, device: &mut D, mgr: &mut PageManage, rowid: u64) -> Record
    where
        D: Write + Read + Seek,
    {
        let node_val = self.root_node.find_id(device, mgr, rowid).unwrap();
        let (mut content_page_count, mut offset) = location_from_u64(node_val);
        let mut rec = Record::default();

        for i in 0..self.value_types.len() {
            let content_page =
                ContentPage::load(&mgr.get(device, content_page_count).unwrap().borrow().data);

            if i < self.value_types.len() - 1 {
                rec.values.push(Value::new(
                    ValueType::Bytes,
                    &content_page.entries[offset as usize].data[8..],
                ));
                (content_page_count, offset) = location_from_u64(u64::from_be_bytes(
                    content_page.entries[offset as usize].data[0..8]
                        .try_into()
                        .unwrap(),
                ));
            } else {
                rec.values.push(Value::new(
                    ValueType::Bytes,
                    &content_page.entries[offset as usize].data,
                ));
            }
        }

        rec
    }
    /** Insert a record */
    pub fn insert<D>(&mut self, device: &mut D, mgr: &mut PageManage, rec: Record) -> u64
    where
        D: Write + Read + Seek,
    {
        let mut rowid = 0;
        loop {
            if self.root_node.find_id(device, mgr, rowid).is_none() {
                break;
            }
            rowid += 1;
        }

        let mut page_count;
        page_count = mgr.find_page_by_type(device, 0, PAGE_TYPEID_CONTENT);
        let mut last_location: Option<u64> = None;
        for (count, val) in rec.values.iter().enumerate() {
            let mut entry = ContentEntry {
                data: val.data.clone(),
                ..Default::default()
            };

            if count < rec.values.len() - 1 {
                entry.data = {
                    let mut data = vec![0; 8];
                    data.extend(entry.data);
                    data
                };
            }

            let mut content_page =
                ContentPage::load(&mgr.get(device, page_count).unwrap().borrow().data);
            loop {
                if content_page.push(entry.clone()).is_ok() {
                    mgr.modify(device, page_count, &content_page.dump());
                    /* the first value */
                    if count == 0 {
                        /* find an empty id */
                        let mut id = 0;
                        loop {
                            if self.root_node.find_id(device, mgr, id).is_none() {
                                break;
                            }
                            id += 1;
                        }
                        /* set this location to btree node */
                        self.root_node.insert_id(
                            device,
                            mgr,
                            id,
                            location_to_u64(page_count, content_page.entries.len() as u8 - 1),
                        );
                    } else {
                        let (last_page_count, offset) = location_from_u64(last_location.unwrap());
                        let mut last_content_page = ContentPage::load(
                            &mgr.get(device, last_page_count).unwrap().borrow().data,
                        );
                        last_content_page.entries[offset as usize].data[0..8].copy_from_slice(
                            &location_to_u64(page_count, content_page.entries.len() as u8 - 1)
                                .to_be_bytes(),
                        );

                        mgr.modify(device, last_page_count, &last_content_page.dump());
                    }
                    last_location = Some(location_to_u64(
                        page_count,
                        content_page.entries.len() as u8 - 1,
                    ));
                    break;
                }
                page_count = mgr.find_page_by_type(device, page_count + 1, PAGE_TYPEID_CONTENT);
            }
        }
        rowid
    }
}
