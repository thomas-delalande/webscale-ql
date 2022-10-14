use crate::{
    domain::{ColumnDefinition, ColumnType},
    DATA_PATH, INT_SIZE_BITS, STRING_SIZE_BITS,
};

pub fn get_unique_flag(flags: Vec<&str>, keyword: &String) -> Vec<String> {
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
pub struct TableData {
    pub row_size: usize,
    pub schema: Vec<ColumnDefinition>,
    pub last_row_id: i32,
}

pub fn load_table_data(table_name: &String) -> TableData {
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
                column_position: index - 1,
            };
        })
        .collect::<Vec<ColumnDefinition>>();

    TableData {
        last_row_id,
        row_size,
        schema,
    }
}

pub fn convert_bytes_to_column(value: &[u8], schema: &ColumnDefinition) -> String {
    std::str::from_utf8(&value)
        .unwrap()
        .to_string()
        .trim_start_matches('\0')
        .to_string()
}

pub fn column_size(column: &ColumnDefinition) -> usize {
    match &column.column_type {
        ColumnType::INT => INT_SIZE_BITS,
        ColumnType::STRING => STRING_SIZE_BITS,
    }
}

pub fn to_column_schema(
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

pub fn to_string(column: ColumnType) -> String {
    return match column {
        ColumnType::INT => "int".to_string(),
        ColumnType::STRING => "string".to_string(),
    };
}

pub fn to_column_type(string: &str) -> ColumnType {
    return match string {
        "int" => ColumnType::INT,
        "string" => ColumnType::STRING,
        _ => panic!("Unknown type: {}", string),
    };
}
