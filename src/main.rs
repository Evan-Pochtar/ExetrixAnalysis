use std::env;
use std::fs;
use std::io::Write;
use std::process::{Command, Stdio};
use tempfile::tempdir;

const PROFILER_PY: &str = include_str!("profiler_wrapper.py");

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <python_script> [args...]", args[0]);
        eprintln!("       Creates a performance report for the Python script");
        eprintln!();
        eprintln!("Example: {} my_script.py --input data.csv", args[0]);
        std::process::exit(2);
    }

    let exe_path = env::current_exe()?;
    let exe_dir = exe_path.parent().unwrap();
    
    let report_dir = exe_dir.join("../../report");
    if report_dir.exists() {
        println!("Cleaning previous report directory...");
        fs::remove_dir_all(&report_dir)?;
    }
    fs::create_dir_all(&report_dir)?;

    let temp_dir = tempdir()?;
    let profiler_path = temp_dir.path().join("profiler_wrapper.py");
    {
        let mut f = fs::File::create(&profiler_path)?;
        f.write_all(PROFILER_PY.as_bytes())?;
    }

    let mut cmd = Command::new("python");
    cmd.arg(profiler_path.to_str().unwrap())
        .arg("--report-dir")
        .arg(report_dir.to_str().unwrap())
        .arg("--")
        .arg(&args[1]);
    
    for a in args.iter().skip(2) {
        cmd.arg(a);
    }

    cmd.stdin(Stdio::inherit())
       .stdout(Stdio::inherit())
       .stderr(Stdio::inherit());

    println!("Running Python script under profiler...");
    println!("   Script: {}", args[1]);
    if args.len() > 2 {
        println!("   Args: {}", args[2..].join(" "));
    }
    println!();
    
    let status = cmd.status()?;

    if !status.success() {
        eprintln!("\nTarget process exited with {}", status);
        std::process::exit(status.code().unwrap_or(1));
    }

    println!("\nProfiling complete!");
    println!("Reports generated:");
    println!("   - JSON: {}", report_dir.join("report.json").display());
    println!("   - HTML: {}", report_dir.join("report.html").display());
    println!();
    println!("Open report.html in your browser to view the interactive report");

    Ok(())
}
