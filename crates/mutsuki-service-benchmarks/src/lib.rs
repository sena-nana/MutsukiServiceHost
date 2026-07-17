use std::collections::BTreeMap;

use mutsuki_runtime_contracts::{
    CompletionBatch, EntryCompletion, RunnerDescriptor, RunnerResult, Task, WorkBatch,
};
use mutsuki_runtime_core::{Runner, RunnerContext, RuntimeFailure, RuntimeResult};
use mutsuki_runtime_sdk::PluginBuilder;
use serde_json::{Value, json};

pub const PLUGIN_ID: &str = "mutsuki.test.abi-fixture";
pub const RUNNER_ID: &str = "mutsuki.test.abi-fixture.runner";
pub const FIXTURE_PROTOCOLS: [&str; 6] = [
    "runner.noop",
    "runner.echo",
    "runner.calibrated-cpu",
    "runner.wait",
    "runner.resource",
    "runner.fault",
];

pub struct FixtureRunner {
    descriptor: RunnerDescriptor,
}

impl FixtureRunner {
    pub fn new(descriptor: RunnerDescriptor) -> Self {
        Self { descriptor }
    }
}

impl Runner for FixtureRunner {
    fn descriptor(&self) -> &RunnerDescriptor {
        &self.descriptor
    }

    fn run_batch(
        &mut self,
        _ctx: RunnerContext,
        batch: WorkBatch,
    ) -> RuntimeResult<CompletionBatch> {
        let tasks = batch.row_payload_tasks().map_err(RuntimeFailure::new)?;
        let tasks = tasks
            .into_iter()
            .map(|task| (task.task_id.clone(), task))
            .collect::<BTreeMap<_, _>>();
        let results = batch
            .entries
            .iter()
            .map(|entry| {
                let task = tasks.get(&entry.task_id).expect("batch task");
                match fixture_result(task) {
                    Ok(result) => EntryCompletion {
                        entry_id: entry.entry_id.clone(),
                        task_id: entry.task_id.clone(),
                        result: Some(result),
                        error: None,
                    },
                    Err(error) => EntryCompletion {
                        entry_id: entry.entry_id.clone(),
                        task_id: entry.task_id.clone(),
                        result: None,
                        error: Some(error.error().clone()),
                    },
                }
            })
            .collect();
        Ok(CompletionBatch::from_results(&batch, results))
    }
}

pub fn fixture_result(task: &Task) -> RuntimeResult<RunnerResult> {
    let output = match task.protocol_id.as_str() {
        "runner.noop" => json!({"status": "ok"}),
        "runner.echo" => json!({"echo": task.payload}),
        "runner.calibrated-cpu" => {
            let seed = task
                .payload
                .get("seed")
                .and_then(Value::as_u64)
                .unwrap_or(1_297_435_713);
            let iterations = task
                .payload
                .get("iterations")
                .and_then(Value::as_u64)
                .unwrap_or(4_096);
            json!({"checksum": calibrated_checksum_with_iterations(seed, iterations)})
        }
        "runner.wait" => json!({"resumed": true}),
        "runner.resource" => json!({
            "resource_id": task.payload.get("resource_ref").cloned().unwrap_or(Value::Null),
            "version": task.payload.get("version").cloned().unwrap_or(Value::Null),
        }),
        "runner.fault" => {
            return Err(RuntimeFailure::new(
                mutsuki_runtime_contracts::RuntimeError::new(
                    "fixture.failure",
                    PLUGIN_ID,
                    "fixture.requested_failure",
                ),
            ));
        }
        protocol => {
            return Err(RuntimeFailure::new(
                mutsuki_runtime_contracts::RuntimeError::new(
                    mutsuki_runtime_contracts::ERR_RUNTIME_HOST_FAILED,
                    PLUGIN_ID,
                    format!("fixture.unsupported_protocol.{protocol}"),
                ),
            ));
        }
    };
    let mut result = RunnerResult::completed(task.task_id.clone());
    result.output = Some(output);
    Ok(result)
}

pub fn calibrated_checksum(seed: u64) -> String {
    calibrated_checksum_with_iterations(seed, 4_096)
}

pub fn calibrated_checksum_with_iterations(seed: u64, iterations: u64) -> String {
    let mut value = seed;
    for _ in 0..iterations {
        value = value
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        value ^= value >> 33;
    }
    format!("{value:016x}")
}

pub fn fixture_manifest_for(
    artifact: mutsuki_runtime_contracts::PluginArtifact,
) -> mutsuki_runtime_contracts::PluginManifest {
    let mut provides =
        mutsuki_service_abi_fixture::benchmark_manifest(&artifact.path, &artifact.sha256).provides;
    provides.host_extensions.clear();
    provides.plugin_backends.clear();
    provides.codecs.clear();
    provides.bridges.clear();
    PluginBuilder::new(PLUGIN_ID)
        .artifact(artifact)
        .provides(provides)
        .build()
        .manifest
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    #[test]
    fn calibrated_cpu_fixture_honors_requested_iterations() {
        let one = fixture_result(&Task::new(
            "one",
            "runner.calibrated-cpu",
            json!({"seed": 1_297_435_713_u64, "iterations": 1}),
        ))
        .unwrap();
        let many = fixture_result(&Task::new(
            "many",
            "runner.calibrated-cpu",
            json!({"seed": 1_297_435_713_u64, "iterations": 65_536}),
        ))
        .unwrap();
        assert_ne!(one.output, many.output);
    }

    #[test]
    fn executable_fixture_manifest_matches_builtin_behavior_and_hashes() {
        let manifest: Value = serde_json::from_str(include_str!(
            "../../../fixtures/performance/runner-fixtures-v1.json"
        ))
        .expect("fixture manifest");
        assert_eq!(
            manifest["schema_version"],
            "mutsuki.performance.runner-fixtures/v1"
        );
        let fixtures = manifest["fixtures"].as_array().expect("fixture entries");
        let protocols = fixtures
            .iter()
            .map(|fixture| fixture["protocol_id"].as_str().expect("protocol_id"))
            .collect::<Vec<_>>();
        assert_eq!(protocols, FIXTURE_PROTOCOLS);

        for fixture in fixtures {
            let protocol_id = fixture["protocol_id"].as_str().expect("protocol_id");
            let output = &fixture["output"];
            let output_bytes = serde_json::to_vec(output).expect("canonical fixture output");
            assert_eq!(
                format!("{:x}", Sha256::digest(output_bytes)),
                fixture["output_sha256"].as_str().expect("output_sha256"),
                "fixture hash for {protocol_id}"
            );
            let task = Task::new("manifest", protocol_id, fixture["payload"].clone());
            if protocol_id == "runner.fault" {
                let error = fixture_result(&task).expect_err("fault fixture fails");
                assert_eq!(error.error().code, output["error"]["code"]);
            } else {
                let result = fixture_result(&task).expect("fixture succeeds");
                assert_eq!(
                    result.output.as_ref(),
                    Some(output),
                    "fixture {protocol_id}"
                );
            }
        }
    }
}
