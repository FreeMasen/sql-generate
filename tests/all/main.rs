use sqlparser::parser::Parser;
use sql_generate::SqlWriter;

mod mssql;

/// Base test runner, captures the text in the provided
/// path, parses that file into a `Vec<Statement>`
/// and writes them in to the provided `SqlWriter`
/// returning the contents of the file
pub fn run_test(path: &str, dialect: impl sqlparser::dialect::Dialect, w: &mut impl SqlWriter) -> Result<String, Box<dyn std::error::Error>> {
    let s = std::fs::read_to_string(path).unwrap();
    let stmts = Parser::parse_sql(&dialect, s.clone()).unwrap();
    for stmt in &stmts {
        w.write_statement(stmt).unwrap();
    }
    Ok(s)
}