use std::sync::Arc;

use mutsuki_runtime_contracts::resource::experimental::{CommandBatch, SagaPlan};
use mutsuki_runtime_contracts::{
    CommandPlan, ExportPlan, PlanReceipt, PluginArtifact, PluginManifest, ReadPlan, ResourceAccess,
    ResourceId, ResourceLifetime, ResourceRef, ResourceSealState, ResourceSemantic,
    RunnerDescriptor, SnapshotDescriptor, StreamPlan, WritePlan,
};
use mutsuki_runtime_core::{Runner, RunnerContext, RuntimeFailure, RuntimeResult};
use mutsuki_runtime_sdk::{
    AbiHostClient, PluginBuilder, ResourcePlanGateway, ResourceProviderGateway,
    RunnerDescriptorBuilder, map_work_batch_entries,
};
use serde_json::{Value, json};

const PLUGIN_ID: &str = "mutsuki.test.abi-fixture";
const RUNNER_ID: &str = "mutsuki.test.abi-fixture.echo";
const PROVIDER_ID: &str = "mutsuki.test.abi-fixture.resource";

struct EchoRunner {
    descriptor: RunnerDescriptor,
}

impl EchoRunner {
    fn new() -> Self {
        Self {
            descriptor: RunnerDescriptorBuilder::new(RUNNER_ID, PLUGIN_ID)
                .accepted_protocol("mutsuki.test.abi.echo")
                .build(),
        }
    }
}

impl Runner for EchoRunner {
    fn descriptor(&self) -> &RunnerDescriptor {
        &self.descriptor
    }

    fn run_batch(
        &mut self,
        _ctx: RunnerContext,
        batch: mutsuki_runtime_contracts::WorkBatch,
    ) -> RuntimeResult<mutsuki_runtime_contracts::CompletionBatch> {
        map_work_batch_entries(&batch, |task| {
            Ok(mutsuki_runtime_contracts::RunnerResult::completed(
                task.task_id.clone(),
            ))
        })
    }
}

struct FixtureProvider;

impl ResourcePlanGateway for FixtureProvider {
    fn collect_read_plan(&self, _plan: &ReadPlan) -> RuntimeResult<Vec<u8>> {
        Ok(b"fixture-resource".to_vec())
    }

    fn snapshot_read_plan(
        &self,
        _plan: &ReadPlan,
        _kind_id: &str,
        _schema: &str,
    ) -> RuntimeResult<SnapshotDescriptor> {
        Err(unsupported("snapshot"))
    }

    fn open_stream_plan(&self, _plan: &ReadPlan) -> RuntimeResult<StreamPlan> {
        Err(unsupported("stream"))
    }

    fn execute_export_plan(&self, plan: &ExportPlan) -> RuntimeResult<PlanReceipt> {
        Ok(receipt(&plan.plan_id, json!("fixture-resource")))
    }

    fn commit_write_plan(&self, plan: &WritePlan, bytes: Vec<u8>) -> RuntimeResult<PlanReceipt> {
        Ok(receipt(&plan.plan_id, json!({ "bytes": bytes.len() })))
    }

    fn execute_command_plan(&self, plan: &CommandPlan) -> RuntimeResult<PlanReceipt> {
        Ok(receipt(
            &plan.plan_id,
            json!({ "operation": plan.operation }),
        ))
    }

    fn execute_command_batch(&self, _batch: &CommandBatch) -> RuntimeResult<Vec<PlanReceipt>> {
        Err(unsupported("command_batch"))
    }

    fn execute_saga_plan(&self, _saga: &SagaPlan) -> RuntimeResult<Vec<PlanReceipt>> {
        Err(unsupported("saga"))
    }
}

impl ResourceProviderGateway for FixtureProvider {
    fn create_blob_resource(&self, schema: &str, bytes: Vec<u8>) -> RuntimeResult<ResourceRef> {
        Ok(resource_ref("blob", schema, bytes.len() as u64))
    }

    fn create_cow_state_resource(
        &self,
        kind_id: &str,
        schema: &str,
        bytes: Vec<u8>,
    ) -> RuntimeResult<ResourceRef> {
        Ok(resource_ref(kind_id, schema, bytes.len() as u64))
    }

    fn create_capability_resource(
        &self,
        kind_id: &str,
        schema: &str,
    ) -> RuntimeResult<ResourceRef> {
        Ok(resource_ref(kind_id, schema, 0))
    }
}

pub fn fixture_manifest(path: &str, sha256: &str) -> PluginManifest {
    build_plugin(path, sha256).manifest
}

fn create_plugin(_host: AbiHostClient) -> RuntimeResult<mutsuki_runtime_sdk::LoadedPlugin> {
    Ok(build_plugin("fixture", "sha256:fixture"))
}

fn build_plugin(path: &str, sha256: &str) -> mutsuki_runtime_sdk::LoadedPlugin {
    PluginBuilder::new(PLUGIN_ID)
        .runner(Box::new(EchoRunner::new()))
        .resource_provider_gateway(PROVIDER_ID, Arc::new(FixtureProvider))
        .artifact(PluginArtifact {
            artifact_type: mutsuki_runtime_contracts::ArtifactType::Abi,
            path: path.into(),
            sha256: sha256.into(),
        })
        .build()
}

fn resource_ref(kind_id: &str, schema: &str, size: u64) -> ResourceRef {
    ResourceRef {
        resource_id: ResourceId {
            kind_id: kind_id.into(),
            slot_id: "fixture".into(),
            generation: 1,
            version: 1,
        },
        ref_id: format!("{kind_id}:fixture"),
        semantic: ResourceSemantic::FrozenValue,
        provider_id: PROVIDER_ID.into(),
        resource_kind: kind_id.into(),
        schema: schema.into(),
        version: 1,
        generation: 1,
        access: ResourceAccess::Inline,
        size_hint: Some(size),
        content_hash: None,
        lifetime: ResourceLifetime::ExternalManaged,
        lease: None,
        seal_state: ResourceSealState::Sealed,
    }
}

fn receipt(plan_id: &str, output: Value) -> PlanReceipt {
    PlanReceipt {
        plan_id: plan_id.into(),
        status: "completed".into(),
        resource_ref: None,
        snapshot: None,
        descriptor_updates: Vec::new(),
        new_version: None,
        output,
    }
}

fn unsupported(route: &str) -> RuntimeFailure {
    RuntimeFailure::new(mutsuki_runtime_contracts::RuntimeError::new(
        mutsuki_runtime_contracts::ERR_RESOURCE_UNSUPPORTED,
        PLUGIN_ID,
        route,
    ))
}

mutsuki_runtime_sdk::export_mutsuki_plugin_abi_v1!(create_plugin);
