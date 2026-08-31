// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A Prometheus endpoint, so the Grafana stack has something to scrape.
//!
//! The crate renders Prometheus text but does not serve it -- every
//! `*_prometheus` function takes a report and hands back a
//! [`matrixraft::PrometheusMetricSet`], leaving exposition to the embedder.
//! That means the dashboards in `observability/` have nothing to draw until
//! something publishes the text, which is what this does.
//!
//! It serves the metrics that exist without a running cluster: the operator
//! runbook, and the provisioning validation that compares the advertised
//! contract against the dashboard and alert rules. **A real deployment should
//! serve its own reports** -- the per-node latency and queue-depth series the
//! dashboard also plots come from a live node, not from here.
//!
//!     cargo run --example metrics_exporter -- 0.0.0.0:9464

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};

use matrixraft::{
    matrixraft_observability_provisioning, matrixraft_observability_provisioning_runbook_steps,
    matrixraft_observability_provisioning_validation_prometheus,
    matrixraft_operator_runbook_prometheus, matrixraft_validate_observability_provisioning,
};

fn metrics_text() -> String {
    let labels: [(&str, &str); 1] = [("service", "matrixraft")];
    let runbook = matrixraft_operator_runbook_prometheus(
        &matrixraft_observability_provisioning_runbook_steps(),
        &labels,
    );
    let validation =
        matrixraft_validate_observability_provisioning(&matrixraft_observability_provisioning());
    let provisioning =
        matrixraft_observability_provisioning_validation_prometheus(&validation, &labels);
    format!("{}{}", runbook.text, provisioning.text)
}

fn serve(mut stream: TcpStream) -> std::io::Result<()> {
    let mut request_line = String::new();
    BufReader::new(stream.try_clone()?).read_line(&mut request_line)?;
    let body = metrics_text();
    // Prometheus is content-type sensitive; this is the text exposition format
    // it expects, and the version matters to older scrapers.
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream.write_all(response.as_bytes())
}

fn main() -> std::io::Result<()> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "0.0.0.0:9464".to_string());
    let listener = TcpListener::bind(&addr)?;
    println!("serving matrixraft metrics on http://{addr}/metrics");
    for stream in listener.incoming() {
        match stream {
            // One connection at a time is enough for a scrape every 15s, and
            // keeps this example about exposition rather than about serving.
            Ok(stream) => {
                let _ = serve(stream);
            }
            Err(err) => eprintln!("accept failed: {err}"),
        }
    }
    Ok(())
}
