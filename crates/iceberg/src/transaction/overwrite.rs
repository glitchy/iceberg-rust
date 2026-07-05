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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{
    DataFile, FormatVersion, Manifest, ManifestContentType, ManifestEntry, ManifestFile,
    ManifestWriterBuilder, Operation,
};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};

/// OverwriteAction is a transaction action for overwriting data files in the table.
///
/// Creates a snapshot with `Operation::Overwrite` semantics — adds new data files and
/// optionally removes existing data files by rewriting affected manifests with those
/// entries marked as `ManifestStatus::Deleted`.
pub struct OverwriteAction {
    check_duplicate: bool,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    deleted_data_files: Vec<DataFile>,
}

impl OverwriteAction {
    pub(crate) fn new() -> Self {
        Self {
            check_duplicate: true,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            added_data_files: vec![],
            deleted_data_files: vec![],
        }
    }

    /// Set whether to check duplicate files.
    pub fn with_check_duplicate(mut self, v: bool) -> Self {
        self.check_duplicate = v;
        self
    }

    /// Add data files to the snapshot.
    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(data_files);
        self
    }

    /// Specify data files to be removed from the table in this overwrite.
    pub fn delete_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.deleted_data_files.extend(data_files);
        self
    }

    /// Set commit UUID for the snapshot.
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set key metadata for manifest files.
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    /// Set snapshot summary properties.
    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }
}

#[async_trait]
impl TransactionAction for OverwriteAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
            self.deleted_data_files.clone(),
        );

        snapshot_producer.validate_added_data_files()?;

        if self.check_duplicate {
            snapshot_producer.validate_duplicate_files().await?;
        }

        let deleted_file_paths: HashSet<String> = self
            .deleted_data_files
            .iter()
            .map(|f| f.file_path.clone())
            .collect();

        let snapshot_id = snapshot_producer.snapshot_id();
        snapshot_producer
            .commit(
                OverwriteOperation {
                    deleted_file_paths,
                    snapshot_id,
                },
                DefaultManifestProcess,
            )
            .await
    }
}

struct OverwriteOperation {
    deleted_file_paths: HashSet<String>,
    snapshot_id: i64,
}

impl SnapshotProduceOperation for OverwriteOperation {
    fn operation(&self) -> Operation {
        Operation::Overwrite
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot_produce
            .table
            .manifest_list_reader(snapshot)
            .load()
            .await?;

        let mut result = Vec::new();

        for manifest_file in manifest_list.entries() {
            // Match Java's `MergingSnapshotProducer.shouldKeep`: a manifest with no live
            // entries (only Deleted entries) carries no information that is not already
            // captured by the absence of those files in older snapshots, and the scan
            // path skips Deleted entries (`scan::process_data_manifest_entry` via
            // `is_alive`). Drop it.
            if !manifest_file.has_added_files() && !manifest_file.has_existing_files() {
                continue;
            }

            let manifest = manifest_file
                .load_manifest(snapshot_produce.table.file_io())
                .await?;

            let has_deletes = manifest.entries().iter().any(|entry| {
                entry.is_alive() && self.deleted_file_paths.contains(entry.file_path())
            });

            if has_deletes {
                let rewritten = self
                    .rewrite_manifest(snapshot_produce, manifest_file, &manifest)
                    .await?;
                result.push(rewritten);
            } else {
                result.push(manifest_file.clone());
            }
        }

        Ok(result)
    }
}

impl OverwriteOperation {
    /// Rewrite a manifest, marking entries whose file paths are in `deleted_file_paths`
    /// as `ManifestStatus::Deleted`.
    async fn rewrite_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
        manifest_file: &ManifestFile,
        manifest: &Manifest,
    ) -> Result<ManifestFile> {
        let table = snapshot_produce.table;

        let new_manifest_path = format!(
            "{}/metadata/{}-m-overwrite.avro",
            table.metadata().location(),
            Uuid::now_v7(),
        );
        let output_file = table.file_io().new_output(&new_manifest_path)?;
        let builder = ManifestWriterBuilder::new(
            output_file,
            Some(self.snapshot_id),
            manifest_file.key_metadata.clone(),
            manifest.metadata().schema.clone(),
            manifest.metadata().partition_spec.clone(),
        );

        let mut writer = match table.metadata().format_version() {
            FormatVersion::V1 => builder.build_v1(),
            FormatVersion::V2 => match manifest_file.content {
                ManifestContentType::Data => builder.build_v2_data(),
                ManifestContentType::Deletes => builder.build_v2_deletes(),
            },
            FormatVersion::V3 => match manifest_file.content {
                ManifestContentType::Data => builder.build_v3_data(),
                ManifestContentType::Deletes => builder.build_v3_deletes(),
            },
        };

        for entry in manifest.entries() {
            // Match Java's `ManifestFilterManager.filterManifestWithDeletedFiles`, which
            // iterates `reader.liveEntries()` and therefore drops entries already marked
            // Deleted. Re-emitting them would either flip them back to Existing (the
            // original bug) or, if re-emitted as Deleted, overwrite the original
            // deletion's `snapshot_id` and lose provenance. Dropping them preserves the
            // prior snapshot's deletion record unchanged.
            if !entry.is_alive() {
                continue;
            }
            if self.deleted_file_paths.contains(entry.file_path()) {
                let mut deleted: ManifestEntry = (**entry).clone();
                deleted.snapshot_id = Some(self.snapshot_id);
                writer.add_deleted_entry(deleted)?;
            } else {
                let cloned: ManifestEntry = (**entry).clone();
                writer.add_existing_entry(cloned)?;
            }
        }

        writer.write_manifest_file().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use serde_json::json;
    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::io::FileIO;
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, MAIN_BRANCH,
        ManifestEntry, ManifestListWriter, ManifestStatus, ManifestWriterBuilder, Operation,
        SnapshotRef, Struct, TableMetadata,
    };
    use crate::table::Table;
    use crate::test_utils::test_runtime;
    use crate::transaction::tests::{make_v2_minimal_table, make_v3_minimal_table_in_catalog};
    use crate::transaction::{ApplyTransactionAction, Transaction, TransactionAction};
    use crate::{TableIdent, TableRequirement, TableUpdate};

    fn test_data_file(path: &str, partition_spec_id: i32) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(partition_spec_id)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_empty_data_overwrite_action() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.overwrite().add_data_files(vec![]);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_overwrite_snapshot_properties() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let mut snapshot_properties = HashMap::new();
        snapshot_properties.insert("key".to_string(), "val".to_string());

        let data_file = test_data_file(
            "test/1.parquet",
            table.metadata().default_partition_spec_id(),
        );

        let action = tx
            .overwrite()
            .set_snapshot_properties(snapshot_properties)
            .add_data_files(vec![data_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_snapshot = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot
        } else {
            unreachable!()
        };
        assert_eq!(
            new_snapshot
                .summary()
                .additional_properties
                .get("key")
                .unwrap(),
            "val"
        );
    }

    #[tokio::test]
    async fn test_overwrite_incompatible_partition_value() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("test/3.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::string("test"))]))
            .build()
            .unwrap();

        let action = tx.overwrite().add_data_files(vec![data_file]);
        assert!(Arc::new(action).commit(&table).await.is_err());
    }

    #[tokio::test]
    async fn test_overwrite_basic() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);

        let data_file = test_data_file(
            "test/3.parquet",
            table.metadata().default_partition_spec_id(),
        );

        let action = tx.overwrite().add_data_files(vec![data_file.clone()]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        assert!(
            matches!((&updates[0],&updates[1]), (TableUpdate::AddSnapshot { snapshot },TableUpdate::SetSnapshotRef { reference,ref_name }) if snapshot.snapshot_id() == reference.snapshot_id && ref_name == MAIN_BRANCH)
        );

        assert_eq!(
            vec![
                TableRequirement::UuidMatch {
                    uuid: table.metadata().uuid()
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: MAIN_BRANCH.to_string(),
                    snapshot_id: table.metadata().current_snapshot_id
                }
            ],
            requirements
        );

        let new_snapshot: SnapshotRef = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            SnapshotRef::new(snapshot.clone())
        } else {
            unreachable!()
        };
        assert_eq!(new_snapshot.summary().operation, Operation::Overwrite);

        let manifest_list = table
            .manifest_list_reader(&new_snapshot)
            .load()
            .await
            .unwrap();
        assert_eq!(1, manifest_list.entries().len());
        assert_eq!(
            manifest_list.entries()[0].sequence_number,
            new_snapshot.sequence_number()
        );

        let manifest = manifest_list.entries()[0]
            .load_manifest(table.file_io())
            .await
            .unwrap();
        assert_eq!(1, manifest.entries().len());
        assert_eq!(
            new_snapshot.sequence_number(),
            manifest.entries()[0]
                .sequence_number()
                .expect("Inherit sequence number by load manifest")
        );
        assert_eq!(
            new_snapshot.snapshot_id(),
            manifest.entries()[0].snapshot_id().unwrap()
        );
        assert_eq!(data_file, *manifest.entries()[0].data_file());
    }

    #[tokio::test]
    async fn test_overwrite_with_deleted_files() {
        use crate::memory::tests::new_memory_catalog;
        use crate::transaction::ApplyTransactionAction;
        use crate::transaction::tests::make_v3_minimal_table_in_catalog;

        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        let original_file1 = test_data_file("test/original1.parquet", spec_id);
        let original_file2 = test_data_file("test/original2.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .fast_append()
            .add_data_files(vec![original_file1.clone(), original_file2.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
        assert_eq!(1, manifest_list.entries().len());

        let replacement_file = test_data_file("test/replacement.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite()
            .add_data_files(vec![replacement_file.clone()])
            .delete_data_files(vec![original_file1.clone(), original_file2.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.summary().operation, Operation::Overwrite);

        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();

        assert_eq!(2, manifest_list.entries().len());

        let mut all_entries = vec![];
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                all_entries.push((entry.status(), entry.file_path().to_string()));
            }
        }

        for original_file in ["test/original1.parquet", "test/original2.parquet"] {
            assert!(
                all_entries
                    .iter()
                    .any(|(status, path)| *status == ManifestStatus::Deleted
                        && path == original_file),
                "Original file {original_file} should be marked as Deleted, entries: {all_entries:?}",
            );
        }

        assert!(
            all_entries
                .iter()
                .any(|(status, path)| *status == ManifestStatus::Added
                    && path == "test/replacement.parquet"),
            "Replacement file should be marked as Added, entries: {all_entries:?}",
        );

        // Verify snapshot summary reports the deleted file.
        assert_eq!(
            snapshot
                .summary()
                .additional_properties
                .get("deleted-data-files")
                .map(|s| s.as_str()),
            Some("2")
        );
        assert_eq!(
            snapshot
                .summary()
                .additional_properties
                .get("deleted-records")
                .map(|s| s.as_str()),
            Some("2")
        );

        // Step 3: Fast append after overwrite — delete-only manifest must survive.
        let appended_file = test_data_file("test/appended.parquet", spec_id);
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![appended_file.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();

        // 3 manifests: rewritten (deleted entry), overwrite added, fast_append added.
        assert_eq!(3, manifest_list.entries().len());

        let mut all_entries = vec![];
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                all_entries.push((entry.status(), entry.file_path().to_string()));
            }
        }

        // The deleted entry must still be present after fast_append.
        for original_file in ["test/original1.parquet", "test/original2.parquet"] {
            assert!(
                all_entries
                    .iter()
                    .any(|(status, path)| *status == ManifestStatus::Deleted
                        && path == original_file),
                "Deleted entry should survive fast_append, entries: {all_entries:?}",
            );
        }
    }

    /// Regression test for finding #5: when a manifest that already contains a Deleted
    /// entry (from a prior snapshot) is rewritten for a new deletion, the previously
    /// Deleted entry must NOT come back as Existing. Matching Java's
    /// `ManifestFilterManager.filterManifestWithDeletedFiles`, which iterates
    /// `reader.liveEntries()`, already-Deleted entries are dropped on rewrite rather
    /// than re-emitted.
    ///
    /// This variant uses a partial overwrite (delete + add) for the first step so that
    /// the unfixed summary underflow from finding #6 is not triggered.
    #[tokio::test]
    async fn test_overwrite_preserves_previously_deleted_entries() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let spec_id = table.metadata().default_partition_spec_id();

        let file_a = test_data_file("test/a.parquet", spec_id);
        let file_b = test_data_file("test/b.parquet", spec_id);
        let file_x = test_data_file("test/x.parquet", spec_id);

        // Append two files into a single manifest.
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(vec![file_a.clone(), file_b.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Partial overwrite: delete a and add x. The rewritten manifest now contains a
        // Deleted entry for a, an Existing entry for b, and an Added entry for x.
        let tx = Transaction::new(&table);
        let action = tx
            .overwrite()
            .delete_data_files(vec![file_a.clone()])
            .add_data_files(vec![file_x.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Sanity: after step 1, a is Deleted in the manifest (so the next step actually
        // exercises the "already-Deleted entry in a rewritten manifest" path).
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
        let mut pre_entries = vec![];
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                pre_entries.push((entry.status(), entry.file_path().to_string()));
            }
        }
        assert!(
            pre_entries
                .iter()
                .any(|(status, path)| *status == ManifestStatus::Deleted && path == "test/a.parquet"),
            "a should be Deleted after step 1, entries: {pre_entries:?}"
        );

        // Delete file_b from the same manifest. The manifest is rewritten. Under the
        // Java-aligned behavior, the previously-Deleted entry for a is DROPPED (not
        // re-emitted), b becomes Deleted, and x stays alive.
        let tx = Transaction::new(&table);
        let action = tx.overwrite().delete_data_files(vec![file_b.clone()]);
        let tx = action.apply(tx).unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();

        let mut all_entries = vec![];
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
            for entry in manifest.entries() {
                all_entries.push((entry.status(), entry.file_path().to_string()));
            }
        }

        // a must NOT come back as Existing/Added (the original #5 bug). It is dropped.
        assert!(
            !all_entries
                .iter()
                .any(|(_status, path)| path == "test/a.parquet"),
            "previously deleted entry a must not reappear in the rewritten manifest \
             (drop semantics), entries: {all_entries:?}"
        );
        assert!(
            all_entries
                .iter()
                .any(|(status, path)| *status == ManifestStatus::Deleted && path == "test/b.parquet"),
            "entry b must be Deleted, entries: {all_entries:?}"
        );
        assert!(
            all_entries
                .iter()
                .any(|(status, path)| *status != ManifestStatus::Deleted && path == "test/x.parquet"),
            "entry x must remain alive, entries: {all_entries:?}"
        );
    }

    /// Builds a table whose current snapshot contains one data manifest written with
    /// partition spec 0, while the table's default partition spec id is 1.
    async fn make_table_with_non_default_partition_spec_manifest() -> (Table, TempDir, DataFile) {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path().join("table1");
        let manifest_list_location = table_location.join("metadata/manifests_list_1.avro");
        let table_metadata_location = table_location.join("metadata/v1.json");

        let file_io = FileIO::new_with_fs();

        let metadata_json = std::fs::read_to_string(format!(
            "{}/testdata/table_metadata/TableMetadataV2Valid.json",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap();
        let mut metadata_value: serde_json::Value = serde_json::from_str(&metadata_json).unwrap();

        // Point the table at the temp location and install two different partition specs.
        // The default spec is 1, but the existing manifest will be written with spec 0.
        *metadata_value.get_mut("location").unwrap() = json!(table_location.to_str().unwrap());
        metadata_value["default-spec-id"] = json!(1);
        metadata_value["partition-specs"] = json!([
            {
                "spec-id": 0,
                "fields": [
                    {"name": "x", "transform": "identity", "source-id": 1, "field-id": 1000}
                ]
            },
            {
                "spec-id": 1,
                "fields": [
                    {"name": "y", "transform": "identity", "source-id": 2, "field-id": 1001}
                ]
            }
        ]);
        metadata_value["last-partition-id"] = json!(1001);

        let current_snapshot_id = metadata_value["current-snapshot-id"].as_i64().unwrap();
        metadata_value["snapshots"] = json!([
            {
                "snapshot-id": current_snapshot_id,
                "timestamp-ms": 1555100955770i64,
                "sequence-number": 1,
                "summary": {"operation": "append"},
                "manifest-list": manifest_list_location.to_str().unwrap()
            }
        ]);
        metadata_value["snapshot-log"] = json!([
            {"snapshot-id": current_snapshot_id, "timestamp-ms": 1555100955770i64}
        ]);
        metadata_value["metadata-log"] = json!([]);

        let table_metadata = serde_json::from_value::<TableMetadata>(metadata_value).unwrap();

        let table = Table::builder()
            .metadata(table_metadata)
            .identifier(TableIdent::from_strs(["db", "table1"]).unwrap())
            .file_io(file_io)
            .metadata_location(table_metadata_location.to_str().unwrap().to_string())
            .runtime(test_runtime())
            .build()
            .unwrap();

        let current_snapshot = table.metadata().current_snapshot().unwrap();
        let schema = current_snapshot.schema(table.metadata()).unwrap();
        let partition_spec = table.metadata().partition_spec_by_id(0).unwrap();

        let manifest_path = format!(
            "{}/metadata/manifest_{}.avro",
            table_location.to_str().unwrap(),
            Uuid::new_v4()
        );
        let output_file = table.file_io().new_output(manifest_path).unwrap();

        let data_file = DataFileBuilder::default()
            .partition_spec_id(0)
            .content(DataContentType::Data)
            .file_path("s3://bucket/test/location/data/old_spec.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .build()
            .unwrap();

        let mut writer = ManifestWriterBuilder::new(
            output_file,
            Some(current_snapshot.snapshot_id()),
            None,
            schema.clone(),
            partition_spec.as_ref().clone(),
        )
        .build_v2_data();

        writer
            .add_entry(
                ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(data_file.clone())
                    .build(),
            )
            .unwrap();

        let manifest = writer.write_manifest_file().await.unwrap();
        assert_eq!(manifest.partition_spec_id, 0);

        let mut manifest_list_write = ManifestListWriter::v2(
            table
                .file_io()
                .new_output(current_snapshot.manifest_list())
                .unwrap()
                .writer()
                .await
                .unwrap(),
            current_snapshot.snapshot_id(),
            current_snapshot.parent_snapshot_id(),
            current_snapshot.sequence_number(),
        );
        manifest_list_write
            .add_manifests(vec![manifest].into_iter())
            .unwrap();
        manifest_list_write.close().await.unwrap();

        (table, tmp_dir, data_file)
    }

    /// Regression test for finding #3: when an overwrite rewrites a manifest, it must
    /// preserve the original manifest's partition spec id rather than using the table's
    /// default partition spec id.
    #[tokio::test]
    async fn test_overwrite_rewritten_manifest_preserves_original_partition_spec() {
        let (table, _tmp_dir, data_file) =
            make_table_with_non_default_partition_spec_manifest().await;

        // Overwrite that deletes the existing file forces a manifest rewrite.
        let tx = Transaction::new(&table);
        let action = tx.overwrite().delete_data_files(vec![data_file]);
        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = action_commit.take_updates();

        let new_snapshot: SnapshotRef = if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            SnapshotRef::new(snapshot.clone())
        } else {
            unreachable!("first update of an overwrite should be AddSnapshot")
        };

        let manifest_list = table
            .manifest_list_reader(&new_snapshot)
            .load()
            .await
            .unwrap();

        assert_eq!(
            manifest_list.entries().len(),
            1,
            "overwrite should produce a single rewritten manifest"
        );

        let rewritten_manifest = &manifest_list.entries()[0];
        assert_eq!(
            rewritten_manifest.partition_spec_id, 0,
            "rewritten manifest must preserve original partition spec id 0, \
             not the table default spec id 1"
        );

        // Verify the entry is marked as deleted.
        let manifest = rewritten_manifest
            .load_manifest(table.file_io())
            .await
            .unwrap();
        assert_eq!(manifest.entries().len(), 1);
        assert_eq!(manifest.entries()[0].status(), ManifestStatus::Deleted);
    }
}
