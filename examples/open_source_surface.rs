// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::readiness::matrixraft_open_source_surface;

fn main() {
    let surface = matrixraft_open_source_surface();
    println!("{}", serde_json::to_string_pretty(&surface).unwrap());
}
