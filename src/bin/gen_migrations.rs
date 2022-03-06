use regex::Regex;
use std::fs;
use std::process::Command;

// TODO:
// * Move this into rusie_queue_cli
// * Accept source opt for passing through to SQLx

fn main() {
    // 1. Call SQLx CLI to gen migration files.
    //    * Can probably assume that users will have the SQLx CLI installed (at least to start).
    //    * Can also assume user is using reversible migrationes and using the same CLI versionas
    //      my local version of the SQLx CLI.
    //    * Are there differences in the SQLx CLI between versions that could break things?
    // 2. Parse filenames from output
    // 3. Write migrations to files

    let output = Command::new("sqlx")
        .arg("migrate")
        .arg("add")
        .arg("-r")
        .arg("add_rusie_queue_table")
        .output()
        .expect("Failed to call SQLx CLI");

    let up_file_regex = Regex::new(r"migrations/\d+_add_rusie_queue_table.up.sql").unwrap();
    let down_file_regex = Regex::new(r"migrations/\d+_add_rusie_queue_table.down.sql").unwrap();

    let stdout = String::from_utf8(output.stdout).unwrap();
    let up_file = up_file_regex.find(&stdout).unwrap().as_str();
    let down_file = down_file_regex.find(&stdout).unwrap().as_str();

    let up_sql = "\
CREATE TABLE queue (
    id UUID PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,

    scheduled_for TIMESTAMP WITH TIME ZONE NOT NULL,
    failed_attempts INT NOT NULL,
    status INT NOT NULL,
    message JSONB NOT NULL
);
CREATE INDEX index_queue_on_scheduled_for ON queue (scheduled_for);
CREATE INDEX index_queue_on_status ON queue (status);
";

    let down_sql = "DROP TABLE queue;\n";

    fs::write(up_file, up_sql).expect("Unable to write up migration");
    fs::write(down_file, down_sql).expect("Unable to write down migration");

    print!("{}", stdout);
}
