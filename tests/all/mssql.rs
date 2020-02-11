use sql_generate::MsSqlWriter;
use sqlparser::dialect::MsSqlDialect;

#[test]
fn simple_select() {
    run_test("tests/sql/ms/simple-select.sql").unwrap();
}

#[test]
fn decalre() {
    run_test("tests/sql/ms/declare.sql").unwrap();
}

fn run_test(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let buf: Vec<u8> = Vec::new();
    let mut w = MsSqlWriter::new("    ", buf);
    let s = super::run_test(path, MsSqlDialect {}, &mut w)?;
    let buf = w.into_inner();
    let s2 = String::from_utf8(buf)?;
    assert_eq!(s, s2);
    Ok(())
}