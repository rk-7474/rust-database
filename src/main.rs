use std::io::{BufWriter, Write};
use std::time::Instant;
use std::path::{Path, self};
use std::fs::{self, File};
use serde_json::{self, Value, Map, value};
use std::collections::HashMap;
use uuid::Uuid;

// fn load_tables() -> Vec<&'static str> {
//     let paths = fs::read_dir("data/").unwrap();

//     let mut tables_array: Vec<&'static str> = Vec::new();


//     for path in paths {
//         let unwrapped_path = path.unwrap();

//         if unwrapped_path.metadata().unwrap().is_dir() {
//             let file_name = unwrapped_path.file_name();
//             if let Some(file_name_str) = file_name.to_str() {
//                 let static_str: &'static str = Box::leak(file_name_str.to_string().into_boxed_str());
//                 tables_array.push(static_str);
//             }
//         }
//     }

//     return tables_array;
// }

fn file_create(path: &str) {
    File::create(path).expect("Errore creando file");
}

fn file_write(path: &str, data: &str) {
    std::fs::write(path, data).expect("errore scrivendo nel file");
}

// fn file_update(path: &str, data: &str) {
//     println!("{}", path);
//     let file = File::open(path).expect("Errore aprendo file");
//     let mut writer = BufWriter::new(file);
//     serde_json::to_writer(&mut writer, data).expect("Errore scrivendo file");
//     writer.flush().expect("Errore flushando file");
// }

fn json_decode(input: &str) -> Value {
    return serde_json::from_str(input).expect("Errore convertendo json");
}

fn file_read(path: String) -> String {
    return fs::read_to_string(path.clone()).expect(&format!("Errore leggendo file ({})", path));
}

// fn get_db_scheme(data_file: &str) -> HashMap<String, Vec<HashMap<&str, &str>>> {

//     file_read("data/tables.json", )

//     return scheme;
// }

fn path_exists(path: &str) -> bool {
    return Path::new(path).exists()
}

fn create_dir(path: &str) {
    fs::create_dir(path).expect("Errore creando cartella");
}


struct Database {
    scheme: Value,
    tables_data: HashMap<String, &'static [Value] >,
    db_main_path: String,
    load_into_memory: bool
}

impl Database {

    fn setup(&mut self) {
        let filepath = format!("{}/tables.json", self.db_main_path);

        let jsonstring = file_read(filepath);

        self.scheme = json_decode(&jsonstring);
    }

    fn file_checks(&mut self) {

        if !path_exists(&self.db_main_path) {
            create_dir(&self.db_main_path);
        }

        for (name, _) in self.scheme.as_object().unwrap() {
            let table_path = &format!("{}/{}", self.db_main_path, name);

            if !path_exists(table_path) {
                create_dir(table_path);
            }

            let file_path = &format!("{}/data.json", table_path);

            if !path_exists(file_path) {
                file_create(file_path);
                file_write(file_path, "[]");
            }
        }
    }

    fn load_all_tables(&mut self) {
        let tables_copy = self.scheme.as_object().unwrap().clone();
        for (name, _) in tables_copy {
            self.load_table(name);
        }
    }

    fn load_table(&mut self, table_name: String) {
        let path: String = format!("{}/{}/data.json", self.db_main_path, table_name);
        let data: &mut [Value] = json_decode(&file_read(path)).as_array().unwrap().clone().leak(); 
        println!("Tabella caricata: {}", table_name);

        self.tables_data.insert(table_name.to_string(), data);
    }

    fn insert(&mut self, table_name: String, data: Value) {
        let table = *self.tables_data.get(&table_name).unwrap();
        let mut vec = table.to_vec();
        vec.push(data);

        let table_path = &format!("{}/{}", self.db_main_path, table_name.clone());

        self.tables_data.insert(table_name, vec.clone().leak());

        file_write(&format!("{}/data.json", table_path), &serde_json::json!(vec).to_string());
    }

    fn select(&mut self, table_name: String, column: String, value: String) -> Vec<&Value> {
        let table = *self.tables_data.get(&table_name).unwrap();
        let mut result = Vec::new();

        for row in table {
            if row[&column] == value {
                result.push(row);
            }
        }

        return result;
    }

    fn update(&mut self, table_name: String, search_column: String, search_value: String, update_column: String, update_value: String) {
        let table = *self.tables_data.get(&table_name).unwrap();
        let result = table.to_vec();

        for (index, row) in table.iter().enumerate() {
            if row[&search_column] == search_value {
                result.get()[&update_column] = serde_json::Value::String(update_value);
            }
        }
    }

    fn read(&mut self, table_name: String) -> &[Value] {
        return self.tables_data.get(&table_name).unwrap();
    }

    fn initialize(&mut self) {
        self.setup();
        self.file_checks();
        if self.load_into_memory {
            self.load_all_tables();
        }
    }
}


fn main() {

    let mut start = Instant::now();

    let mut database = Database { scheme: Value::Object(Map::new()), tables_data: HashMap::new(), load_into_memory: true, db_main_path: String::from("D:/Progetti/rust-database/data") };
    database.initialize();

    println!("{:?}", start.elapsed());


    start = Instant::now();

    let mut result = database.read(String::from("auth"));

    for data in result {
        println!("{} {}", data["password"], data["name"]);
    }

    println!("=====================");

    let result = database.select(String::from("auth"), String::from("name"), String::from("ciao"));

    println!("{:?}", result);

    println!("{:?}", start.elapsed());

    

}


