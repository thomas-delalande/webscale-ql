use std::io::{self, Write};

const INT_SIZE_BITS: u32 = 32;
const STRING_SIZE_BITS: u32 = 32;

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
    let mut file = std::fs::File::create(format!("{}.schema", name)).unwrap();
    file.write_all(format!("{}\n", name).as_bytes()).unwrap();
    let size = calculate_size(&col_schemas);
    file.write_all(format!("{}\n", size).as_bytes()).unwrap();
    for row in col_schemas {
        file.write_all(format!("{} {}\n", row.name, to_string(row.column_type)).as_bytes())
            .unwrap();
    }
}

fn insert(name: &str, flags: Vec<&str>) {
    let file = std::fs::read_to_string(format!("{}.schema", name)).unwrap();
    let mut lines = file.split('\n').collect::<Vec<&str>>();
    lines.pop();
    let columns = &lines[2..];
    let mut index = 0;
    let column_schemas = columns
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
            let mut data = clean_row;
            data.remove(0);
            data.remove(0);
            data.remove(0);
            data.remove(0);
            data
        })
        .collect::<Vec<String>>();

    let joined = strings.join("~");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(format!("{}.data", name))
        .unwrap();
    write!(file, "{}~", joined.to_string()).unwrap();
}

fn is_string_numeric(str: String) -> bool {
    for c in str.chars() {
        if !c.is_numeric() {
            return false;
        }
    }
    return true;
}
fn select(name: &str, flags: Vec<&str>) {
    let schema_file = std::fs::read_to_string(format!("{}.schema", name)).unwrap();
    let data_file = std::fs::read_to_string(format!("{}.data", name)).unwrap();

    let mut schema_file_lines = schema_file.split('\n').collect::<Vec<&str>>();
    // remove last line (which is blank)
    schema_file_lines.pop();
    let columns = &schema_file_lines[2..];
    let mut index = 0;
    let column_schemas = columns
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

    let clauses = flags
        .iter()
        .filter(|flag| flag.starts_with("where"))
        .collect::<Vec<_>>();

    let mut rows = data_file
        .strip_suffix('\n')
        .unwrap_or(&data_file)
        .split('~')
        .collect::<Vec<&str>>();
    rows.pop();

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
            let values = row.split(' ').collect::<Vec<&str>>();
            return values.into_iter().nth(column_schema.index.into()).unwrap() == expected;
        });
    });

    for row in rows {
        println!("{}", row)
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
