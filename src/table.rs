use crate::btree::*;
use crate::page::*;
use std::io::{Result as IOResult, *};

pub fn location_to_u64(content_page_count: u64, offset: u8) -> u64 {
    content_page_count << 8 | offset as u64
}

pub fn location_from_u64(u64_val: u64) -> (u64, u8) {
    (u64_val >> 8, (u64_val & 255) as u8)
}

#[derive(Debug, Clone)]
pub enum ValueType {
    Number,
    Bytes,
}

#[derive(Debug, Clone)]
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

#[derive(Clone, Default, Debug)]
pub struct Record {
    pub rowid: u64,
    pub values: Vec<Value>,
    pub location: Vec<(u64, u8)>,
}

#[derive(Default)]
pub struct Table {
    pub root_node: BtreeNode,
    pub value_types: Vec<ValueType>,
}

impl Table {
    /** Query a record by rowid */
    pub fn query<D>(&self, device: &mut D, mgr: &mut PageManage, rowid: u64) -> IOResult<Record>
    where
        D: Write + Read + Seek,
    {
        let node_val = self.root_node.find_id(device, mgr, rowid).unwrap();
        let (mut content_page_count, mut offset) = location_from_u64(node_val);
        let mut rec = Record::default();

        for i in 0..self.value_types.len() {
            let content_page = ContentPage::load(&mgr.get_data(device, content_page_count)?);

            /* not the last value */
            if i != self.value_types.len() - 1 {
                let mut data = Vec::new();
                data.extend(&content_page.entries[offset as usize].data);

                if let Some(overflow_page) = content_page.entries[offset as usize].overflow_page {
                    let mut page = OverflowPage::load(&mgr.get_data(device, overflow_page)?);
                    data.extend(page.data);
                    while let Some(next) = page.next {
                        page = OverflowPage::load(&mgr.get_data(device, next)?);
                        data.extend(page.data);
                    }
                }
                rec.values.push(Value::new(ValueType::Bytes, &data[8..]));
                (content_page_count, offset) = location_from_u64(u64::from_be_bytes(
                    content_page.entries[offset as usize].data[0..8]
                        .try_into()
                        .unwrap(),
                ));
            } else {
                let mut data = Vec::new();
                data.extend(&content_page.entries[offset as usize].data);

                if let Some(overflow_page) = content_page.entries[offset as usize].overflow_page {
                    let mut page = OverflowPage::load(&mgr.get_data(device, overflow_page)?);
                    data.extend(page.data);
                    while let Some(next) = page.next {
                        page = OverflowPage::load(&mgr.get_data(device, next)?);
                        data.extend(page.data);
                    }
                }
                rec.values.push(Value::new(ValueType::Bytes, &data));
            }
        }

        Ok(rec)
    }
    /** Insert a record */
    pub fn insert<D>(
        &mut self,
        device: &mut D,
        mgr: &mut PageManage,
        record: Record,
    ) -> IOResult<u64>
    where
        D: Write + Read + Seek,
    {
        let rowid = self.root_node.find_unused(device, mgr);

        let mut page_count = mgr.find_page_by_type(device, 0, PAGE_TYPEID_CONTENT)?;
        let mut last_location: Option<u64> = None;
        for (count, val) in record.values.iter().enumerate() {
            let mut entry = ContentEntry::from_bytes(device, mgr, &val.data)?;

            /* not the last value */
            if count != record.values.len() - 1 {
                entry.data = {
                    let mut data = vec![0; 8];
                    data.extend(entry.data);
                    data
                };
            }

            /* write to content page */
            let mut content_page = ContentPage::load(&mgr.get_data(device, page_count)?);
            loop {
                if content_page.push(entry.clone()).is_ok() {
                    mgr.modify(device, page_count, &content_page.dump())?;
                    /* the first value */
                    if count == 0 {
                        let id = self.root_node.find_unused(device, mgr);
                        /* set this location to btree node */
                        self.root_node.insert_id(
                            device,
                            mgr,
                            id,
                            location_to_u64(page_count, content_page.entries.len() as u8 - 1),
                        )?;
                    } else {
                        let (last_page_count, offset) = location_from_u64(last_location.unwrap());
                        let mut last_content_page =
                            ContentPage::load(&mgr.get_data(device, last_page_count)?);
                        last_content_page.entries[offset as usize].data[0..8].copy_from_slice(
                            &location_to_u64(page_count, content_page.entries.len() as u8 - 1)
                                .to_be_bytes(),
                        );

                        mgr.modify(device, last_page_count, &last_content_page.dump())?;
                    }
                    last_location = Some(location_to_u64(
                        page_count,
                        content_page.entries.len() as u8 - 1,
                    ));
                    break;
                }
                page_count = mgr.find_page_by_type(device, page_count + 1, PAGE_TYPEID_CONTENT)?;
                content_page = ContentPage::load(&mgr.get_data(device, page_count)?);
            }
        }
        Ok(rowid)
    }
}
