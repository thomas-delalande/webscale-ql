use std::io::Write;

use binary_search_tree::BinarySearchTree;
use rand::{distributions::Alphanumeric, thread_rng, Rng};

use crate::{
    index_util::{KeyValuePair, read_node_from_bytes},
    load_table_data, read_row,
    utils::{get_unique_flag, to_column_schema, to_string, column_size_bytes},
    DATA_PATH, INT_SIZE_BITS, STRING_SIZE_BITS,
};

#[derive(Clone, Debug)]
pub struct ColumnDefinition {
    pub name: String,
    pub column_type: ColumnType,
    pub column_position: u8,
}

#[derive(Debug, Clone)]
pub enum ColumnType {
    INT,
    STRING,
}

pub fn create(table_name: &str, columns: Vec<ColumnDefinition>) {
    let row_size = columns
        .iter()
        .map(|definition| match &definition.column_type {
            ColumnType::INT => INT_SIZE_BITS,
            ColumnType::STRING => STRING_SIZE_BITS,
        })
        .collect::<Vec<usize>>()
        .iter()
        .sum::<usize>();

    let mut file = std::fs::File::create(format!("{DATA_PATH}/{table_name}.schema")).unwrap();
    file.write_all(format!("-1\n{row_size}\n").as_bytes())
        .unwrap();
    columns.iter().for_each(|column| {
        file.write_all(
            format!(
                "{} {}\n",
                column.name,
                to_string(column.column_type.clone())
            )
            .as_bytes(),
        )
        .unwrap();
    });
}

pub fn insert(
    table_name: &str,
    columns: Vec<ColumnDefinition>,
    values: Vec<&str>,
    prev_row_id: u32,
) {
    let mut values = values.clone();
    let prev_row_id: &str = &prev_row_id.to_string();
    values.insert(0, prev_row_id);

    let row_as_bytes = values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let mut value = value.as_bytes().to_vec();
            let mut fixed_size_value = vec![0; column_size_bytes(&columns[index])];
            fixed_size_value.drain(0..value.len());
            fixed_size_value.append(&mut value);
            fixed_size_value
        })
        .collect::<Vec<Vec<u8>>>()
        .concat();

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(format!("{}/{}.data", DATA_PATH, table_name))
        .unwrap();
    file.write_all(&row_as_bytes).unwrap();
}

pub fn update_index(table_name: &str, columns: Vec<ColumnDefinition>) {
    columns.iter().for_each(|col| {
        if std::path::Path::new(format!("{}/{}.{}.index", DATA_PATH, table_name, col.name).as_str())
            .exists()
        {
            create_index(table_name.to_string(), col);
        };
    });
}

pub fn create_index(table_name: String, column: &ColumnDefinition) {
    let data_file = std::fs::read_to_string(format!("{}/{}.data", DATA_PATH, table_name)).unwrap();
    let table_data = load_table_data(&table_name);
    let schema = column;
    let row_size = table_data.row_size;
    let rows = data_file
        .as_bytes()
        .chunks(row_size)
        .enumerate()
        .filter(|row| row.1.len() > 1)
        .map(|(_index, row)| {
            let values = read_row(&row.to_vec(), &table_data.schema);
            let index: usize = schema.column_position.into();
            let data = values.get(index).unwrap();
            data.to_owned()
        })
        .collect::<Vec<String>>();

    let column_name = &column.name;

    let mut ctree = BinarySearchTree::new();

    rows.iter().enumerate().for_each(|(index, row)| {
        ctree.insert(KeyValuePair::new(row.to_string(), index));
    });

    save_tree(&table_name, column_name, &mut ctree);
    println!("Saved index on column {column_name}.")
}

fn save_tree(table_name: &String, column_name: &String, tree: &mut BinarySearchTree<KeyValuePair>) {
    if tree.size % 2 == 0 {
        tree.insert(KeyValuePair::new("".to_string(), 0));
    }
    let mut file =
        std::fs::File::create(format!("{DATA_PATH}/{table_name}.{column_name}.index",)).unwrap();
    tree.level_order()
        .collect::<Vec<&KeyValuePair>>()
        .iter()
        .for_each(|node| {
            let text = format!("{:0>16}{:09x}", node.key, node.value);
            file.write_all(text.as_bytes()).unwrap();
        });
}

pub fn select(table_name: &str, column: &str, expected: &str) {
    let table_data = load_table_data(&table_name.to_string());
    let column_schemas = table_data.schema;
    let row_size = table_data.row_size;

    let column_schema = to_column_schema(column, &column_schemas).unwrap();
    let data_file = std::fs::read_to_string(format!("{}/{}.data", DATA_PATH, table_name)).unwrap();
    let rows = if column == "_rowid" {
        let row_id: usize = expected.parse().unwrap();
        let start = row_size * row_id;
        let row = &data_file.as_bytes()[start..start + row_size].to_vec();

        let values = read_row(row, &column_schemas);
        vec![values]
    } else {
        let mut rows = data_file
            .as_bytes()
            .chunks(row_size)
            .enumerate()
            .filter(|row| row.1.len() == row_size)
            .map(|(_index, row)| read_row(&row.to_vec(), &column_schemas))
            .collect::<Vec<Vec<String>>>();
        rows.retain(|row| {
            let values = row;
            return values.into_iter().nth(column_schema.column_position.into()).unwrap() == expected;
        });
        rows
    };
    let num_rows = rows.len();

    for row in rows {
        println!("{:?}", row)
    }
    println!("Results: {}", num_rows);
}

pub fn search_with_index(table_name: &String, column_name: &String, value: &String) -> usize {
    let file = std::fs::read_to_string(format!(
        "{}/{}.{}.index",
        DATA_PATH, table_name, column_name
    ))
    .unwrap();

    let expected = value.to_string();
    let binary_tree_node_size: usize = 16 + INT_SIZE_BITS as usize;
    let bytes = file.as_bytes();
    let mut position = 0;
    loop {
        let node = bytes.chunks(binary_tree_node_size).nth(position).unwrap();
        let data = read_node_from_bytes(&node, 16);

        if data.value == expected {
            return data.position;
        }
        if data.value > expected {
            position = 2 * position + 1;
        }

        if data.value < expected {
            position = 2 * position + 2;
        }
    }
}

pub fn random(table: &str, flags: Vec<&str>) {
    let num = &get_unique_flag(flags, &"num".to_string())[0]
        .parse::<usize>()
        .unwrap();
    let table_data = load_table_data(&table.to_string());
    let types = &table_data
        .clone()
        .schema
        .iter()
        .map(|x| x.column_type.clone())
        .collect::<Vec<ColumnType>>()
        .to_vec();

    for index in 0..*num {
        let values = types
            .iter()
            .map(|col| match col {
                ColumnType::INT => thread_rng().gen::<u16>().to_string(),
                ColumnType::STRING => thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(8)
                    .map(char::from)
                    .collect(),
            })
            .collect::<Vec<String>>();

        let index: i32 = index.try_into().unwrap();
        let row_id: i32 = index + table_data.last_row_id + 1;
        let mut values: Vec<&str> = values.iter().map(|s| s as &str).collect();
        values.remove(0);
        insert(
            table,
            table_data.clone().schema,
            values.to_vec(),
            row_id.try_into().unwrap(),
        );
    }
}
