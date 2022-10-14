use domain::{create_index, select, search_with_index, random};
use utils::{column_size, convert_bytes_to_column, get_unique_flag};
use std::{
    fs::File,
    io::{self, BufRead, BufReader, Write},
    time::Instant,
};

use crate::{domain::{ColumnDefinition, ColumnType, create, insert, update_index}, utils::{to_column_type, load_table_data}};

mod domain;
mod index_util;
mod utils;

const INT_SIZE_BITS: u32 = 9;
const STRING_SIZE_BITS: u32 = 16;
const DATA_PATH: &str = "data";

fn main() {
    loop {
        print!("webscale $ ");
        io::stdout().flush().unwrap();

        let mut line = String::new();
        std::io::stdin().read_line(&mut line).unwrap();
        line.pop();
        handle_line(line);
    }
}

fn handle_line(line: String) {
    let words = line.split(' ').collect::<Vec<&str>>();
    let flags = line.split('-').collect::<Vec<&str>>();
    let verb = words[0];
    let table = words[1];
    match verb {
        "create" => create_command(table, flags),
        "insert" => insert_command(table, flags),
        "select" => select_command(table, flags),
        "index" => create_index_command(table, flags),
        "select-index" => select_with_index_command(table, flags),
        "random" => random(table, flags),
        _ => println!("Command not found: {}", words[0]),
    }
}

fn create_command(table_name: &str, flags: Vec<&str>) {
    let cols = flags.iter().filter(|flag| flag.starts_with("col"));
    let mut index = 0;
    let mut col_schemas = cols
        .map(|flag| {
            let words = flag.split(' ').collect::<Vec<&str>>();
            if words[0] == "col" {
                let name = words[1];
                let row_type = to_column_type(words[2]);
                index += 1;
                return ColumnDefinition {
                    name: name.to_string(),
                    column_type: row_type,
                    column_position: index,
                };
            } else {
                panic!("Can only create col.")
            }
        })
        .collect::<Vec<ColumnDefinition>>();
    col_schemas.insert(
        0,
        ColumnDefinition {
            column_type: ColumnType::INT,
            column_position: 0,
            name: "_rowid".to_string(),
        },
    );

    create(table_name, col_schemas.clone());
    println!("Created table '{table_name}'");
}

fn parse_flag(string: &str) -> Vec<&str> {
    let mut strings = string
        .trim()
        .split("\"")
        .map(|x| x.trim())
        .collect::<Vec<&str>>();
    strings.retain(|x| x.len() > 0);
    let mut values = strings
        .iter()
        .enumerate()
        .map(|(index, s)| {
            if index % 2 == 0 {
                s.trim().split(" ").collect::<Vec<&str>>()
            } else {
                vec![s.to_owned().trim()]
            }
        })
        .flatten()
        .collect::<Vec<&str>>();
    values.remove(0);
    values
}

fn insert_command(table_name: &str, flags: Vec<&str>) {
    let timer = Instant::now();
    let table_data = load_table_data(&table_name.to_string());
    let column_schemas = table_data.schema;

    let rows = flags
        .iter()
        .filter(|flag| flag.starts_with("row"))
        .map(|row| row.to_owned())
        .collect::<Vec<&str>>();

    rows.iter().enumerate().for_each(|(index, row)| {
        let values = parse_flag(row);

        let index: i32 = index.try_into().unwrap();
        insert(
            table_name,
            column_schemas.clone(),
            values,
            (table_data.last_row_id + index + 1).try_into().unwrap(),
        );
        update_last_row_id(table_name, table_data.last_row_id + index + 1);
    });

    update_index(table_name, column_schemas);
    println!("Inserted rows: {:?}", rows);
    println!("Insert took: {:?}", timer.elapsed());
}

fn update_last_row_id(table_name: &str, last_row_id: i32) {
    let file_path = format!("{DATA_PATH}/{table_name}.schema");
    let lines = BufReader::new(File::open(file_path).unwrap()).lines();
    let mut lines = lines.skip(1).map(|x| x.unwrap()).collect::<Vec<String>>();

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(format!("{DATA_PATH}/{table_name}.schema"))
        .unwrap();

    lines.insert(0, last_row_id.to_string());

    file.write_all(lines.join("\n").as_bytes()).unwrap();
    file.write("\n".as_bytes()).unwrap();
}

fn read_row(row: &Vec<u8>, schema: &Vec<ColumnDefinition>) -> Vec<String> {
    let mut position = 0;
    schema
        .iter()
        .map(|col| {
            let size: usize = column_size(col).try_into().unwrap();
            let value = row[position..(position + size)].to_vec();
            position += size;
            convert_bytes_to_column(&value, col)
        })
        .collect::<Vec<String>>()
}

fn select_command(name: &str, flags: Vec<&str>) {
    let timer = std::time::Instant::now();
    let clause = flags
        .iter()
        .filter(|flag| flag.starts_with("where"))
        .collect::<Vec<_>>();
    let strings = parse_flag(clause.first().unwrap());
    select(name, strings.get(0).unwrap(), strings.get(1).unwrap());
    println!("Query took {:?}", timer.elapsed())
}


fn create_index_command(table: &str, flags: Vec<&str>) {
    let values = get_unique_flag(flags, &"on".to_string());
    let column = &values[0];
    let table_data = load_table_data(&table.to_string());
    let column_schemas = table_data.schema;
    let expected_schema = column_schemas
        .iter()
        .filter(|sce| sce.name == column.clone())
        .next()
        .unwrap();
    create_index(table.to_string(), expected_schema);
}

fn select_with_index_command(table: &str, flags: Vec<&str>) {
    let values = get_unique_flag(flags, &"where".to_string());
    let column = &values[0];
    let expected = &values[1];
    let table_data = load_table_data(&table.to_string());
    let column_schemas = table_data.schema;
    let expected_schema = column_schemas
        .iter()
        .filter(|sce| sce.name == column.clone())
        .next()
        .unwrap();
    let row_id = search_with_index(&table.to_string(), &expected_schema.name, &expected);
    select(table, "_rowid", &row_id.to_string());
}

