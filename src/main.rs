use rand::{distributions::Alphanumeric, thread_rng, Rng};
use std::{
    io::{self, Write},
    time::Instant,
};

use rbtree::RBTree;

const INT_SIZE_BITS: u32 = 32;
const STRING_SIZE_BITS: u32 = 32;
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
        "create" => create(table, flags),
        "insert" => insert(table, flags),
        "select" => select(table, flags),
        "index" => create_index_command(table, flags),
        "select-index" => select_with_index(table, flags),
        "random" => random(table, flags),
        _ => println!("Command not found: {}", words[0]),
    }
}

fn create(name: &str, flags: Vec<&str>) {
    let cols = flags.iter().filter(|flag| flag.starts_with("col"));
    let mut index = 0;
    let col_schemas = cols
        .map(|flag| {
            let words = flag.split(' ').collect::<Vec<&str>>();
            if words[0] == "col" {
                let name = words[1];
                let row_type = to_column_type(words[2]);
                index += 1;
                return ColumnSchema {
                    name: name.to_string(),
                    column_type: row_type,
                    index: index - 1,
                };
            } else {
                panic!("Can only create col.")
            }
        })
        .collect::<Vec<ColumnSchema>>();
    let mut file = std::fs::File::create(format!("{}/{}.schema", DATA_PATH, name)).unwrap();
    file.write_all(format!("{}\n", name).as_bytes()).unwrap();
    let size = calculate_size(&col_schemas);
    file.write_all(format!("{}\n", size).as_bytes()).unwrap();
    for row in col_schemas {
        file.write_all(format!("{} {}\n", row.name, to_string(row.column_type)).as_bytes())
            .unwrap();
    }
}

fn insert(name: &str, flags: Vec<&str>) {
    let timer = Instant::now();
    let table_data = get_column_schemas_from_file(&name.to_string());
    let column_schemas = table_data.schema;

    let rows = flags
        .iter()
        .filter(|flag| flag.starts_with("row"))
        .collect::<Vec<_>>();
    rows.iter().for_each(|row| {
        let clean_row = row.to_string().trim_start().trim_end().to_string();
        let mut values = clean_row.split(' ').collect::<Vec<&str>>();
        values.remove(0);
        let mut index = 0;
        for value in values {
            let error = match column_schemas[index].column_type {
                ColumnType::INT => !is_string_numeric(value.to_string()),
                _ => false,
            };
            if error {
                panic!(
                    "Does not match schema, {}, {:?}",
                    value, column_schemas[index].column_type
                );
            }
            index += 1;
        }
    });
    let strings = rows
        .iter()
        .map(|row| {
            let clean_row = row.to_string().trim_start().trim_end().to_string();
            let mut values = clean_row.split(' ').collect::<Vec<&str>>();
            values.remove(0);
            let mut index = 0;
            let hex = values
                .iter()
                .map(|value| {
                    let hex_cols = match column_schemas[index].column_type {
                        ColumnType::INT => format!("{:032x}", value.parse::<i32>().unwrap()),
                        ColumnType::STRING => format!("{:0>32}", hex::encode(value)),
                    };
                    index += 1;
                    hex_cols
                })
                .collect::<Vec<String>>();
            hex.join("")
        })
        .collect::<Vec<String>>();

    insert_row(name, strings);
    column_schemas.iter().for_each(|col| {
        if std::path::Path::new(format!("{}/{}.{}.index", DATA_PATH, name, col.name).as_str())
            .exists()
        {
            create_index(name.to_string(), col);
        };
    });
    println!("Insert took: {:?}", timer.elapsed());
}

fn insert_row(table: &str, rows: Vec<String>) {
    let joined = rows.join("");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(format!("{}/{}.data", DATA_PATH, table))
        .unwrap();
    file.write_all(joined.to_string().as_bytes()).unwrap();
}

fn select(name: &str, flags: Vec<&str>) {
    let timer = std::time::Instant::now();
    let data_file = std::fs::read_to_string(format!("{}/{}.data", DATA_PATH, name)).unwrap();

    let table_data = get_column_schemas_from_file(&name.to_string());
    let column_schemas = table_data.schema;
    let row_size = table_data.row_size;

    let clauses = flags
        .iter()
        .filter(|flag| flag.starts_with("where"))
        .collect::<Vec<_>>();

    let mut rows = data_file
        .as_bytes()
        .chunks(row_size)
        .enumerate()
        .filter(|row| row.1.len() > 1)
        .map(|(_index, row)| {
            row.chunks(32)
                .enumerate()
                .map(|(index, value)| {
                    let schema = &column_schemas[index];
                    convert_bytes_to_column(value, schema)
                })
                .collect::<Vec<String>>()
        })
        .collect::<Vec<Vec<String>>>();

    clauses.iter().for_each(|clause| {
        let clean_clause = clause.to_string().trim_start().trim_end().to_string();
        let mut values = clean_clause.split(' ').collect::<Vec<&str>>();
        values.remove(0);
        let column = values[0];
        let expected = values[1];
        let column_schema = match to_column_schema(column, &column_schemas) {
            Ok(schema) => schema,
            Err(msg) => {
                println!("{}", msg);
                return;
            }
        };
        rows.retain(|row| {
            let values = row;
            return values.into_iter().nth(column_schema.index.into()).unwrap() == expected;
        });
    });
    let num_rows = rows.len();

    for row in rows {
        println!("{:?}", row)
    }
    println!("Results: {}", num_rows);
    println!("Query took {:?}", timer.elapsed())
}

fn create_index(table_name: String, column: &ColumnSchema) -> RBTree<String, usize> {
    let data_file = std::fs::read_to_string(format!("{}/{}.data", DATA_PATH, table_name)).unwrap();
    let table_data = get_column_schemas_from_file(&table_name);
    let schema = column;
    let row_size = table_data.row_size;
    let rows = data_file
        .as_bytes()
        .chunks(row_size)
        .enumerate()
        .filter(|row| row.1.len() > 1)
        .map(|(_index, row)| {
            let data = &row[0..32];
            match schema.column_type {
                ColumnType::STRING => std::str::from_utf8(&data)
                    .unwrap()
                    .to_string()
                    .trim_start_matches('\0')
                    .to_string(),
                ColumnType::INT => {
                    i64::from_str_radix(&std::str::from_utf8(data).unwrap().to_string(), 16)
                        .unwrap()
                        .to_string()
                }
            }
        })
        .collect::<Vec<String>>();

    let mut tree = RBTree::new();
    rows.iter()
        .enumerate()
        .for_each(|(index, row)| tree.insert(row.to_string(), index));
    save_tree(&table_name, &column.name, &tree);
    tree
}

fn save_tree(table_name: &String, column_name: &String, tree: &RBTree<String, usize>) {
    let mut file = std::fs::File::create(format!(
        "{}/{}.{}.index",
        DATA_PATH, table_name, column_name
    ))
    .unwrap();
    tree.iter().for_each(|node| {
        let position = format!("{:032x}", node.1);
        let text = format!("{}{}", node.0, position);
        file.write_all(text.as_bytes()).unwrap();
    });
}

fn create_index_command(table: &str, flags: Vec<&str>) {
    let values = get_unique_flag(flags, &"on".to_string());
    let column = &values[0];
    let table_data = get_column_schemas_from_file(&table.to_string());
    let column_schemas = table_data.schema;
    let expected_schema = column_schemas
        .iter()
        .filter(|sce| sce.name == column.clone())
        .next()
        .unwrap();
    let _ = create_index(table.to_string(), expected_schema);
}

fn select_with_index(table: &str, flags: Vec<&str>) {
    let timer = std::time::Instant::now();
    let values = get_unique_flag(flags, &"where".to_string());
    let column = &values[0];
    let expected = &values[1];
    let table_data = get_column_schemas_from_file(&table.to_string());
    let column_schemas = table_data.schema;
    let expected_schema = column_schemas
        .iter()
        .filter(|sce| sce.name == column.clone())
        .next()
        .unwrap();
    let position = search_with_index(&table.to_string(), &expected_schema.name, &expected);
    let data_file = std::fs::read_to_string(format!("{}/{}.data", DATA_PATH, table)).unwrap();
    let row_size = table_data.row_size;
    let start = position * row_size;
    let row = &data_file.as_bytes()[start as usize..(row_size + start) as usize];
    let data = row
        .chunks(32)
        .enumerate()
        .map(|(index, value)| {
            let schema = &column_schemas[index];
            let thing = convert_bytes_to_column(value, schema);
            thing
        })
        .collect::<Vec<String>>();

    println!("{:?}", data);
    println!("Query took {:?}", timer.elapsed());
}
fn search_with_index(table_name: &String, column_name: &String, value: &String) -> usize {
    let file = std::fs::read_to_string(format!(
        "{}/{}.{}.index",
        DATA_PATH, table_name, column_name
    ))
    .unwrap();

    let expected = value.to_string();
    let binary_tree_node_size = 64;
    let bytes = file.as_bytes();
    let mut position = 0;
    loop {
        let node = bytes.chunks(binary_tree_node_size).nth(position).unwrap();
        let data = read_node_from_bytes(&node);
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

fn random(table: &str, flags: Vec<&str>) {
    let num = &get_unique_flag(flags, &"num".to_string())[0]
        .parse::<usize>()
        .unwrap();
    let schema = get_column_schemas_from_file(&table.to_string()).schema;
    let mut rows: Vec<String> = Vec::new();

    for _ in 0..*num {
        let row = schema
            .iter()
            .map(|col| {
                let string = match col.column_type {
                    ColumnType::INT => thread_rng().gen::<i32>().to_string(),
                    ColumnType::STRING => thread_rng()
                        .sample_iter(&Alphanumeric)
                        .take(12)
                        .map(char::from)
                        .collect(),
                };
                convert_string_to_hex(string, col)
            })
            .collect::<String>();
        rows.push(row);
    }
    insert_row(table, rows);
}
struct NodeData {
    value: String,
    position: usize,
}

fn read_node_from_bytes(bytes: &[u8]) -> NodeData {
    let value_bytes = bytes.chunks(32).nth(0).unwrap();
    let value_string = std::str::from_utf8(&value_bytes).unwrap();
    let hex = hex::decode(value_string).unwrap();
    let value = std::str::from_utf8(&hex)
        .unwrap()
        .to_string()
        .trim_start_matches('\0')
        .to_string();
    let position_bytes = bytes.chunks(32).nth(1).unwrap();
    let position = i64::from_str_radix(
        &std::str::from_utf8(position_bytes).unwrap().to_string(),
        16,
    )
    .unwrap();

    NodeData {
        value,
        position: position.try_into().unwrap(),
    }
}

fn get_unique_flag(flags: Vec<&str>, keyword: &String) -> Vec<String> {
    let clause = flags
        .iter()
        .filter(|flag| flag.starts_with(keyword))
        .next()
        .unwrap();
    let clean_clause = clause.to_string().trim_start().trim_end().to_string();
    let mut values = clean_clause.split(' ').collect::<Vec<&str>>();
    values.remove(0);
    values.iter().map(|val| val.to_string()).collect()
}

struct TableData {
    row_size: usize,
    schema: Vec<ColumnSchema>,
}

fn get_column_schemas_from_file(table_name: &String) -> TableData {
    let schema_file =
        std::fs::read_to_string(format!("{}/{}.schema", DATA_PATH, table_name)).unwrap();
    let mut schema_file_lines = schema_file.split('\n').collect::<Vec<&str>>();
    // remove last line (which is blank)
    schema_file_lines.pop();
    let row_size = schema_file_lines[1].parse::<usize>().unwrap();
    let columns = &schema_file_lines[2..];
    let mut index = 0;
    let schema = columns
        .iter()
        .map(|col| {
            let split = col.split(' ').collect::<Vec<&str>>();
            index += 1;
            return ColumnSchema {
                name: split[0].to_string(),
                column_type: to_column_type(&split[1].to_string()),
                index: index - 1,
            };
        })
        .collect::<Vec<ColumnSchema>>();

    TableData { row_size, schema }
}

fn convert_string_to_hex(value: String, schema: &ColumnSchema) -> String {
    match schema.column_type {
        ColumnType::INT => format!("{:032x}", value.parse::<i32>().unwrap()),
        ColumnType::STRING => format!("{:0>32}", hex::encode(value)),
    }
}

fn convert_bytes_to_column(value: &[u8], schema: &ColumnSchema) -> String {
    match schema.column_type {
        ColumnType::STRING => {
            let word = hex::decode(value).unwrap();
            std::str::from_utf8(&word)
                .unwrap()
                .to_string()
                .trim_start_matches('\0')
                .to_string()
        }
        ColumnType::INT => {
            i64::from_str_radix(&std::str::from_utf8(value).unwrap().to_string(), 16)
                .unwrap()
                .to_string()
        }
    }
}

fn calculate_size(rows: &Vec<ColumnSchema>) -> u32 {
    return rows
        .iter()
        .map(|row| {
            return match &row.column_type {
                ColumnType::INT => INT_SIZE_BITS,
                ColumnType::STRING => STRING_SIZE_BITS,
            };
        })
        .collect::<Vec<u32>>()
        .iter()
        .sum::<u32>();
}

#[derive(Debug, Clone)]
enum ColumnType {
    INT,
    STRING,
}

fn to_column_schema(value: &str, schemas: &Vec<ColumnSchema>) -> Result<ColumnSchema, String> {
    for schema in schemas {
        if value == schema.name {
            return Ok(schema.clone());
        }
    }
    return Err(format!("Could not find column for value: {}", value));
}

#[derive(Clone)]
struct ColumnSchema {
    name: String,
    column_type: ColumnType,
    index: u8,
}

fn to_string(column: ColumnType) -> String {
    return match column {
        ColumnType::INT => "int".to_string(),
        ColumnType::STRING => "string".to_string(),
    };
}

fn to_column_type(string: &str) -> ColumnType {
    return match string {
        "int" => ColumnType::INT,
        "string" => ColumnType::STRING,
        _ => panic!("Unknown type: {}", string),
    };
}

fn is_string_numeric(str: String) -> bool {
    for c in str.chars() {
        if !c.is_numeric() {
            return false;
        }
    }
    return true;
}
