use crate::page::*;
use std::io::{Read, Seek, Write};

const MAX_IDS: usize = PAGE_SIZE / (8 + 8) - 1;
const UNIT_SIZE: usize = 8 + 8;

#[derive(Default)]
pub struct BtreeNode {
    pub page_count: u64,
    pub ids: Vec<u64>,
    pub ptrs: Vec<u64>,
    pub node_type: u8,
}

impl BtreeNode {
    pub fn new(page_count: u64, page: &[u8; PAGE_SIZE]) -> Self {
        let mut node = Self::load(page);
        node.page_count = page_count;
        node
    }
    pub fn new_node(node_type: u8) -> Self {
        Self {
            node_type,
            ..Default::default()
        }
    }
    /** Load from bytes */
    pub fn load(page: &[u8; PAGE_SIZE]) -> Self {
        let mut node = Self::new_node(page[0]);

        let id_count = page[1] as usize;

        for i in 0..id_count {
            node.push(
                u64::from_be_bytes(
                    page[UNIT_SIZE * (i + 1)..UNIT_SIZE * (i + 1) + 8]
                        .try_into()
                        .unwrap(),
                ),
                u64::from_be_bytes(
                    page[UNIT_SIZE * (i + 1) + 8..UNIT_SIZE * (i + 1) + UNIT_SIZE]
                        .try_into()
                        .unwrap(),
                ),
            );
        }
        node
    }
    /** Dump to bytes */
    pub fn dump(&self) -> [u8; PAGE_SIZE] {
        let mut page = [0; PAGE_SIZE];
        page[0] = self.node_type;
        page[1] = self.len() as u8;
        for (i, _) in self.ids.iter().enumerate() {
            page[UNIT_SIZE * (i + 1)..UNIT_SIZE * (i + 1) + 8]
                .copy_from_slice(&self.ids[i].to_be_bytes());
            page[UNIT_SIZE * (i + 1) + 8..UNIT_SIZE * (i + 1) + UNIT_SIZE]
                .copy_from_slice(&self.ptrs[i].to_be_bytes());
        }
        page
    }
    /** Add an id into the node */
    fn add(&mut self, id: u64, ptr: u64) {
        if self.ids.is_empty() {
            self.push(id, ptr);
        } else {
            for (i, _) in self.ids.iter().enumerate() {
                if i < self.len() - 1 && id > self.ids[i] && id < self.ids[i + 1]
                    || i == self.len() - 1
                {
                    self.insert(i + 1, id, ptr);
                    break;
                }
            }
        }
    }
    /** Push an id into the current node
     *
     * Return:
     * * node ID of the right node
     * * page count of the right node */
    fn part<D>(&mut self, device: &mut D, mgr: &mut PageManage) -> (u64, u64)
    where
        D: Write + Read + Seek,
    {
        let mut another = Self::new_node(self.node_type);
        for _ in 0..self.len() / 2 {
            another.insert(0, self.ids.pop().unwrap(), self.ptrs.pop().unwrap());
        }

        let another_page = mgr.alloc(device, PageType::BtreePage);
        another.page_count = another_page.borrow().count;
        another_page.borrow_mut().modify(&another.dump());
        mgr.modify(device, self.page_count, &self.dump());

        (*another.ids.first().unwrap(), another.page_count)
    }
    /** Insert an id into B-Tree */
    pub fn insert_id<D>(&mut self, device: &mut D, mgr: &mut PageManage, id: u64, value: u64)
    where
        D: Write + Read + Seek,
    {
        if let Some((id, page)) = self.insert_id_nontop(device, mgr, id, value) {
            let mut left = Self::new_node(self.node_type);
            for i in 0..self.len() {
                left.push(self.ids[i], self.ptrs[i]);
            }

            let left_page = mgr.alloc(device, PageType::BtreePage);
            left.page_count = left_page.borrow().count;
            left_page.borrow_mut().modify(&left.dump());

            self.clear();
            self.node_type = PAGE_TYPEID_BTREE_INTERNAL;
            self.push(*left.ids.first().unwrap(), left_page.borrow().count);
            self.push(id, page);
            mgr.modify(device, self.page_count, &self.dump());
        }
    }
    /** Insert an id
     *
     * Return:
     * * node ID of the right node
     * * page count of the right node
     */
    fn insert_id_nontop<D>(
        &mut self,
        device: &mut D,
        mgr: &mut PageManage,
        id: u64,
        value: u64,
    ) -> Option<(u64, u64)>
    where
        D: Write + Read + Seek,
    {
        if self.is_leaf() {
            self.add(id, value);
            mgr.modify(device, self.page_count, &self.dump());

            /* part into two child nodes */
            if self.len() >= MAX_IDS {
                return Some(self.part(device, mgr));
            }
        } else {
            /* find child node to insert */
            for i in 0..self.len() {
                if i < self.len() - 1 && id > self.ids[i] && id < self.ids[i + 1]
                    || i == self.len() - 1
                {
                    let child = mgr.get(device, self.ptrs[i]).unwrap();
                    let mut child_node = Self::new(child.borrow().count, &child.borrow().data);
                    /* if parted into tow sub trees */
                    if let Some((id, page)) = child_node.insert_id_nontop(device, mgr, id, value) {
                        self.add(id, page);
                        mgr.modify(device, self.page_count, &self.dump());
                    }

                    if self.len() >= MAX_IDS {
                        return Some(self.part(device, mgr));
                    }
                }
            }
        }
        None
    }
    /** Remove an id from B-Tree */
    pub fn remove_id<D>(&mut self, device: &mut D, mgr: &mut PageManage, id: u64)
    where
        D: Write + Read + Seek,
    {
        if self.is_internal() {
            for i in 0..self.len() {
                if i < self.len() - 1 && id >= self.ids[i] && id < self.ids[i + 1]
                    || i == self.len() - 1
                {
                    let child_page = mgr.get(device, self.ptrs[i]).unwrap();
                    let mut child_node =
                        Self::new(child_page.borrow().count, &child_page.borrow().data);
                    child_node.remove_id(device, mgr, id);
                    /* when child_node is empty, self.len() must be 0 */
                    if child_node.is_empty() {
                        self.remove(i);
                    } else if child_node.len() < MAX_IDS / 2 {
                        if i > 0 {
                            let previous_node_page = mgr.get(device, self.ptrs[i - 1]).unwrap();
                            let mut previous_node = Self::new(
                                previous_node_page.borrow().count,
                                &previous_node_page.borrow().data,
                            );
                            /* merge this child node into previous node */
                            if previous_node.len() + child_node.len() <= MAX_IDS {
                                for child_i in 0..child_node.len() {
                                    previous_node
                                        .push(child_node.ids[child_i], child_node.ptrs[child_i]);
                                }
                                mgr.release(device, child_node.page_count);
                                self.remove(i);
                            } else {
                                let id = previous_node.ids.pop().unwrap();
                                let ptr = previous_node.ptrs.pop().unwrap();
                                child_node.insert(0, id, ptr);
                                child_page.borrow_mut().modify(&child_node.dump());
                                self.ids[i] = id;
                            }
                            previous_node_page
                                .borrow_mut()
                                .modify(&previous_node.dump());
                        } else if i < self.len() - 1 {
                            let next_node_page = mgr.get(device, self.ptrs[i + 1]).unwrap();
                            let mut next_node = Self::new(
                                next_node_page.borrow().count,
                                &next_node_page.borrow().data,
                            );
                            /* merge this child node into next node */
                            if next_node.len() + child_node.len() <= MAX_IDS {
                                for child_i in (0..child_node.len()).rev() {
                                    next_node.insert(
                                        0,
                                        child_node.ids[child_i],
                                        child_node.ptrs[child_i],
                                    );
                                }
                                self.ids[i + 1] = *next_node.ids.first().unwrap();
                                mgr.release(device, child_node.page_count);
                                self.remove(i);
                            } else {
                                let id = *next_node.ids.first().unwrap();
                                let ptr = *next_node.ptrs.first().unwrap();
                                next_node.remove(0);
                                child_node.push(id, ptr);
                                child_page.borrow_mut().modify(&child_node.dump());
                                self.ids[i + 1] = *next_node.ids.first().unwrap();
                            }
                            next_node_page.borrow_mut().modify(&next_node.dump());
                        }
                    }
                    mgr.modify(device, self.page_count, &self.dump());
                }
            }
        } else {
            /* find and remove */
            for i in 0..self.len() {
                if self.ids[i] == id {
                    self.remove(i);
                    mgr.modify(device, self.page_count, &self.dump());
                    break;
                }
            }
        }
    }
    /** Find pointer by id */
    pub fn find_id<D>(&self, device: &mut D, mgr: &mut PageManage, id: u64) -> Option<u64>
    where
        D: Write + Read + Seek,
    {
        if self.is_internal() {
            for i in 0..self.len() {
                if i < self.len() - 1 && id >= self.ids[i] && id < self.ids[i + 1]
                    || i == self.len() - 1
                {
                    let page = mgr.get(device, self.ptrs[i]).unwrap();
                    let child = Self::new(page.borrow().count, &page.borrow().data);
                    return child.find_id(device, mgr, id);
                }
            }
        } else {
            for i in 0..self.ids.len() {
                if id == self.ids[i] {
                    return Some(self.ptrs[i]);
                }
            }
        }
        None
    }
    /** 
     * Return:
     * * Unused id
     * * useed id count (only a leaf node will returns this)
     */
    fn find_unused_nontop<D>(
        &self,
        device: &mut D,
        mgr: &mut PageManage,
    ) -> (Option<u64>, Option<u64>)
    where
        D: Write + Read + Seek,
    {
        if self.is_internal() {
            for i in 0..self.len() {
                let page = mgr.get(device, self.ptrs[i]).unwrap();
                let child = Self::new(page.borrow().count, &page.borrow().data);
                let result = child.find_unused_nontop(device, mgr);

                if let Some(id) = result.0 {
                    return (Some(id), None);
                } else if let Some(id) = result.1 {
                    if i < self.len() - 1 && id + 1 < self.ids[i + 1] || i == self.len() - 1 {
                        return (Some(id + 1), None);
                    }
                }
            }
        } else if self.ids.len() > 1 {
            for i in 0..self.ids.len() - 1 {
                if self.ids[i] + 1 < self.ids[i + 1] {
                    return (Some(self.ids[i] + 1), None);
                }
            }
            return (None, Some(*self.ids.last().unwrap()));
        }
        (None, None)
    }
    /** Find unused id */
    pub fn find_unused<D>(&self, device: &mut D, mgr: &mut PageManage) -> u64
    where
        D: Write + Read + Seek,
    {
        let result = self.find_unused_nontop(device, mgr);
        if let Some(id) = result.0 {
            id
        } else if let Some(id) = result.1 {
            id
        } else {
            0
        }
    }
    pub fn is_internal(&self) -> bool {
        self.node_type == PAGE_TYPEID_BTREE_INTERNAL
    }
    pub fn is_leaf(&self) -> bool {
        self.node_type == PAGE_TYPEID_BTREE_LEAF
    }
    pub fn len(&self) -> usize {
        self.ids.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn push(&mut self, id: u64, ptr: u64) {
        self.ids.push(id);
        self.ptrs.push(ptr);
    }
    pub fn insert(&mut self, index: usize, id: u64, ptr: u64) {
        self.ids.insert(index, id);
        self.ptrs.insert(index, ptr);
    }
    pub fn remove(&mut self, index: usize) {
        self.ids.remove(index);
        self.ptrs.remove(index);
    }
    pub fn clear(&mut self) {
        self.ids.clear();
        self.ptrs.clear();
    }
}
