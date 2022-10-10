use binary_search_tree::BinarySearchTree;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use std::{
    cmp::Ordering,
    fs::File,
    io::{self, BufRead, BufReader, Write},
    time::Instant,
};

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
                    index,
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
            index: 0,
            name: "_rowid".to_string(),
        },
    );

    create(table_name, col_schemas.clone());
    println!("Created table '{table_name}'");
}

#[derive(Clone, Debug)]
struct ColumnDefinition {
    name: String,
    column_type: ColumnType,
    index: u8,
}

#[derive(Debug, Clone)]
enum ColumnType {
    INT,
    STRING,
}

fn create(table_name: &str, columns: Vec<ColumnDefinition>) {
    let row_size = columns
        .iter()
        .map(|definition| match &definition.column_type {
            ColumnType::INT => INT_SIZE_BITS,
            ColumnType::STRING => STRING_SIZE_BITS,
        })
        .collect::<Vec<u32>>()
        .iter()
        .sum::<u32>();

    let mut file = std::fs::File::create(format!("{DATA_PATH}/{table_name}.schema")).unwrap();
    file.write_all(format!("\n{row_size}\n").as_bytes())
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

fn insert(
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
        .map(|(index, value)| match columns[index].column_type {
            ColumnType::INT => format!("{:09x}", value.parse::<i64>().unwrap())
                .as_bytes()
                .to_owned(),
            ColumnType::STRING => format!("{:0>16}", hex::encode(value)).as_bytes().to_owned(),
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

fn update_index(table_name: &str, columns: Vec<ColumnDefinition>) {
    columns.iter().for_each(|col| {
        if std::path::Path::new(format!("{}/{}.{}.index", DATA_PATH, table_name, col.name).as_str())
            .exists()
        {
            create_index(table_name.to_string(), col);
        };
    });
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

fn select(table_name: &str, column: &str, expected: &str) {
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
            return values.into_iter().nth(column_schema.index.into()).unwrap() == expected;
        });
        rows
    };
    let num_rows = rows.len();

    for row in rows {
        println!("{:?}", row)
    }
    println!("Results: {}", num_rows);
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

#[derive(Clone, Eq, Debug)]
pub struct KeyValuePair {
    pub key: String,
    pub value: usize,
}

impl Ord for KeyValuePair {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl PartialOrd for KeyValuePair {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for KeyValuePair {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.value == other.value
    }
}

impl KeyValuePair {
    pub fn new(key: String, value: usize) -> KeyValuePair {
        KeyValuePair { key, value }
    }
}
fn create_index(table_name: String, column: &ColumnDefinition) {
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
            let index: usize = schema.index.into();
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

fn save_tree(
    table_name: &String,
    column_name: &String,
    tree: &mut BinarySearchTree<KeyValuePair>,
) {
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
fn search_with_index(table_name: &String, column_name: &String, value: &String) -> usize {
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

fn random(table: &str, flags: Vec<&str>) {
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

#[derive(Debug)]
struct NodeData {
    value: String,
    position: usize,
}

fn read_node_from_bytes(bytes: &[u8], column_size: usize) -> NodeData {
    let value_bytes = &bytes[0..column_size];
    let position_bytes = &bytes[column_size..(column_size + INT_SIZE_BITS as usize)];
    let position = i64::from_str_radix(
        &std::str::from_utf8(position_bytes).unwrap().to_string(),
        16,
    )
    .unwrap();

    let value = std::str::from_utf8(value_bytes)
        .unwrap()
        .trim_start_matches(|x| x == '0');

    NodeData {
        value: value.to_string(),
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

#[derive(Clone, Debug)]
struct TableData {
    row_size: usize,
    schema: Vec<ColumnDefinition>,
    last_row_id: i32,
}

fn load_table_data(table_name: &String) -> TableData {
    let schema_file =
        std::fs::read_to_string(format!("{}/{}.schema", DATA_PATH, table_name)).unwrap();
    let mut schema_file_lines = schema_file.split('\n').collect::<Vec<&str>>();
    // remove last line (which is blank)
    schema_file_lines.pop();
    let last_row_id = schema_file_lines[0].parse::<i32>().unwrap();
    let row_size = schema_file_lines[1].parse::<usize>().unwrap();
    let columns = &schema_file_lines[2..];
    let mut index = 0;
    let schema = columns
        .iter()
        .map(|col| {
            let split = col.split(' ').collect::<Vec<&str>>();
            index += 1;
            return ColumnDefinition {
                name: split[0].to_string(),
                column_type: to_column_type(&split[1].to_string()),
                index: index - 1,
            };
        })
        .collect::<Vec<ColumnDefinition>>();

    TableData {
        last_row_id,
        row_size,
        schema,
    }
}

fn convert_bytes_to_column(value: &[u8], schema: &ColumnDefinition) -> String {
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

fn column_size(column: &ColumnDefinition) -> u32 {
    match &column.column_type {
        ColumnType::INT => INT_SIZE_BITS,
        ColumnType::STRING => STRING_SIZE_BITS,
    }
}

fn to_column_schema(
    value: &str,
    schemas: &Vec<ColumnDefinition>,
) -> Result<ColumnDefinition, String> {
    for schema in schemas {
        if value == schema.name {
            return Ok(schema.clone());
        }
    }
    return Err(format!("Could not find column for value: {}", value));
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
