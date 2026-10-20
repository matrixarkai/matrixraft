// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What the configurable pipeline limits cost a group, exactly.

use matrixraft::{Config, PipelineLimits, RaftCluster};

fn main() {
    println!(
        "RaftCluster      {:>5} bytes",
        std::mem::size_of::<RaftCluster>()
    );
    println!(
        "Config           {:>5} bytes",
        std::mem::size_of::<Config>()
    );
    println!(
        "PipelineLimits   {:>5} bytes",
        std::mem::size_of::<PipelineLimits>()
    );
    println!(
        "Box<PipelineLimits> {:>2} bytes",
        std::mem::size_of::<Box<PipelineLimits>>()
    );
    let groups: u64 = 8192;
    let added = std::mem::size_of::<PipelineLimits>() as u64;
    println!(
        "\nat {groups} groups: {} bytes = {:.1} KiB",
        groups * added,
        (groups * added) as f64 / 1024.0
    );
}
