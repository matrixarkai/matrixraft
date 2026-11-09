// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What the configurable pipeline limits cost a group, exactly.

use matrixraft::{Config, PeerProgress, PipelineLimits, RaftCluster, ReplicationPipeline};

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
        "PeerProgress     {:>5} bytes",
        std::mem::size_of::<PeerProgress>()
    );
    println!(
        "ReplicationPipeline {:>2} bytes",
        std::mem::size_of::<ReplicationPipeline>()
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
    let pipeline_bytes = groups * std::mem::size_of::<ReplicationPipeline>() as u64;
    println!(
        "{groups} idle pipelines: {pipeline_bytes} bytes = {:.1} KiB",
        pipeline_bytes as f64 / 1024.0
    );
}
