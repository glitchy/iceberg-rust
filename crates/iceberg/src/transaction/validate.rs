// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Commit-time conflict-detection validations for transaction actions.

use std::collections::HashMap;

use crate::expr::visitors::inclusive_metrics_evaluator::InclusiveMetricsEvaluator;
use crate::expr::visitors::inclusive_projection::InclusiveProjection;
use crate::expr::visitors::manifest_evaluator::ManifestEvaluator;
use crate::expr::{Bind, BoundPredicate, Predicate};
use crate::spec::{ManifestContentType, ManifestStatus, Operation, Schema, TableMetadataRef};
use crate::table::Table;
use crate::util::snapshot::ancestors_between;
use crate::{Error, ErrorKind, Result};

const VALIDATE_ADDED_DATA_FILES_OPERATIONS: [Operation; 2] =
    [Operation::Append, Operation::Overwrite];

/// Fails if any snapshot committed after `from_snapshot_id` (up to the current head) added a data
/// file that could contain records matching `conflict_filter`.
///
/// Detects concurrently-added data only; concurrent delete files and concurrently-removed data
/// files are not yet validated. Returns a non-retryable [`ErrorKind::DataInvalid`] on conflict.
pub(crate) async fn validate_no_conflicting_data(
    table: &Table,
    from_snapshot_id: Option<i64>,
    conflict_filter: &Predicate,
) -> Result<()> {
    let metadata = table.metadata_ref();
    // Scoped to the table's current snapshot; branch-targeted overwrites are not yet supported.
    let Some(head_snapshot_id) = metadata.current_snapshot_id() else {
        return Ok(());
    };

    // Empty window: head is still the snapshot we read from (the common first-attempt case).
    if from_snapshot_id == Some(head_snapshot_id) {
        return Ok(());
    }

    let schema = metadata.current_schema();
    let bound_filter = conflict_filter.bind(schema.clone(), true)?;

    // Partition-summary evaluators are built once per partition spec and reused across manifests.
    let mut manifest_evaluator_cache: HashMap<i32, ManifestEvaluator> = HashMap::new();
    let file_io = table.file_io();

    for snapshot in ancestors_between(&metadata, head_snapshot_id, from_snapshot_id) {
        if !VALIDATE_ADDED_DATA_FILES_OPERATIONS.contains(&snapshot.summary().operation) {
            continue;
        }

        let manifest_list = table.manifest_list_reader(&snapshot).load().await?;

        for manifest_file in manifest_list.entries() {
            // Only data manifests authored by this snapshot hold its concurrently-added files.
            // Author + `Added` status is enough to identify them: a snapshot's own manifest marks
            // its new files `Added` and carried-over files `Existing`, so the entries we keep
            // necessarily belong to this snapshot without a separate snapshot-id cross-check.
            if manifest_file.content != ManifestContentType::Data
                || manifest_file.added_snapshot_id != snapshot.snapshot_id()
                || !manifest_file.has_added_files()
            {
                continue;
            }

            // Prune the manifest by its partition summaries before reading entries.
            let evaluator = match manifest_evaluator_cache.get(&manifest_file.partition_spec_id) {
                Some(evaluator) => evaluator,
                None => {
                    let partition_filter = build_partition_filter(
                        &metadata,
                        manifest_file.partition_spec_id,
                        schema,
                        &bound_filter,
                    )?;
                    manifest_evaluator_cache
                        .entry(manifest_file.partition_spec_id)
                        .or_insert(ManifestEvaluator::builder(partition_filter).build())
                }
            };
            if !evaluator.eval(manifest_file)? {
                continue;
            }

            let manifest = manifest_file.load_manifest(file_io).await?;
            for entry in manifest.entries() {
                if entry.status() != ManifestStatus::Added {
                    continue;
                }
                if InclusiveMetricsEvaluator::eval(&bound_filter, entry.data_file(), false)? {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Found conflicting files that can contain records matching {conflict_filter}: {}",
                            entry.file_path()
                        ),
                    ));
                }
            }
        }
    }

    Ok(())
}

/// Projects `bound_filter` onto partition spec `spec_id` and binds it to that spec's schema.
fn build_partition_filter(
    metadata: &TableMetadataRef,
    spec_id: i32,
    schema: &Schema,
    bound_filter: &BoundPredicate,
) -> Result<BoundPredicate> {
    let partition_spec = metadata.partition_spec_by_id(spec_id).ok_or_else(|| {
        Error::new(
            ErrorKind::Unexpected,
            format!("Could not find partition spec for id {spec_id}"),
        )
    })?;

    let partition_type = partition_spec.partition_type(schema)?;
    let partition_schema = Schema::builder()
        .with_schema_id(partition_spec.spec_id())
        .with_fields(partition_type.fields().to_owned())
        .build()?;

    InclusiveProjection::new(partition_spec.clone())
        .project(bound_filter)?
        .rewrite_not()
        .bind(partition_schema.into(), true)
}
