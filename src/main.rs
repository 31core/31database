#[allow(dead_code)]
mod btree;
#[allow(dead_code)]
mod page;
#[allow(dead_code)]
mod table;

fn main() -> std::io::Result<()> {
    let mut f = std::fs::File::options()
        .create(true)
        .write(true)
        .read(true)
        .open("31.db")?;

    let mut node = btree::BtreeNode::new_node(page::PAGE_TYPEID_BTREE_LEAF);
    let mut mgr = page::PageManage::default();
    mgr.cache_size = 1024;
    let root_page = mgr.alloc(&mut f, page::PageType::BtreePage);
    node.page_count = root_page.borrow().count;

    let mut rec = table::Record::default();

    rec.values
        .push(table::Value::new(table::ValueType::Bytes, b"test"));
    rec.values
        .push(table::Value::new(table::ValueType::Bytes, b"test1"));

    let mut table = table::Table {
        root_node: node,
        ..Default::default()
    };

    table.value_types.push(table::ValueType::Bytes);
    table.value_types.push(table::ValueType::Bytes);
    let rowid = table.insert(&mut f, &mut mgr, rec);
    mgr.sync_all(&mut f)?;
    let rec = table.query(&mut f, &mut mgr, rowid);
    println!("{:?}", rec.values[0]);
    Ok(())
}
