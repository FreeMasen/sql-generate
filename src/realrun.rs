use sqlparser::{dialect::MsSqlDialect, parser::Parser};

fn main() {
    let mut args = std::env::args();
    let _ = args.next();
    let path = if let Some(arg) = args.next() {
        arg
    } else {
        eprintln!("USAGE
realrun <path>");
        std::process::exit(0);
    };
    println!("running: {}", path);
    let stmts = Parser::parse_sql(&MsSqlDialect {}, std::fs::read_to_string(path).unwrap()).unwrap();
    println!("{:#?}", stmts);
}