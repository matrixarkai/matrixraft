// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Writes the deployable observability artifacts, or checks the checked-in ones
//! still match.
//!
//! The dashboard and the alert rules are rendered from the model in
//! `metrics.rs`, so a panel or an alert added there reaches the deployed files
//! by regenerating rather than by being copied by hand.
//!
//!     cargo run --example render_observability_artifacts
//!     cargo run --example render_observability_artifacts -- --check

use std::path::Path;

use matrixraft::observability_artifacts::matrixraft_observability_artifacts;

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let check = args.iter().any(|arg| arg == "--check");
    let root = args
        .iter()
        .find(|arg| !arg.starts_with("--"))
        .cloned()
        .unwrap_or_else(|| "observability".to_string());
    let root = Path::new(&root);

    let mut drifted = Vec::new();
    for artifact in matrixraft_observability_artifacts() {
        let path = root.join(&artifact.path);
        if check {
            match std::fs::read_to_string(&path) {
                Ok(found) if found == artifact.contents => {}
                Ok(_) => drifted.push(format!(
                    "{} differs from what the model renders",
                    artifact.path
                )),
                Err(err) => drifted.push(format!("{}: {err}", artifact.path)),
            }
            continue;
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, artifact.contents.as_bytes())?;
        println!("wrote {}", path.display());
    }

    if check {
        if drifted.is_empty() {
            println!("observability artifacts match the model");
            return Ok(());
        }
        for issue in &drifted {
            eprintln!("drift: {issue}");
        }
        std::process::exit(1);
    }
    Ok(())
}
