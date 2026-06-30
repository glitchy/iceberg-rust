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
use crate::expr::Predicate;
use crate::spec::{
    DataFile, FormatVersion, Manifest, ManifestContentType, ManifestEntry, ManifestFile,
    ManifestWriterBuilder, Operation,
};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::validate::validate_no_conflicting_data;
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
    validate_added_files: bool,
    conflict_detection_filter: Option<Predicate>,
    from_snapshot_id: Option<i64>,
}

impl OverwriteAction {
    pub(crate) fn new(from_snapshot_id: Option<i64>) -> Self {
        Self {
            check_duplicate: true,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            added_data_files: vec![],
            deleted_data_files: vec![],
            validate_added_files: false,
            conflict_detection_filter: None,
            from_snapshot_id,
        }
    }

    /// Set whether to check duplicate files.
    pub fn with_check_duplicate(mut self, v: bool) -> Self {
        self.check_duplicate = v;
        self
    }

    /// Enable validation that no concurrently-added data file conflicts with this overwrite.
    ///
    /// When enabled, the commit is rejected if any snapshot committed after this action's starting
    /// snapshot added a data file that could contain records matching the
    /// [conflict detection filter](Self::conflict_detection_filter). Validation is re-run against
    /// the latest table state on every commit retry.
    ///
    /// This guards against concurrent *appends* only. It does not yet detect concurrent delete
    /// files or concurrent removal of the files being replaced, so it is not sufficient for full
    /// serializable isolation on tables that use delete files.
    pub fn validate_no_conflicting_data(mut self) -> Self {
        self.validate_added_files = true;
        self
    }

    /// Set the filter used to scope [conflict detection](Self::validate_no_conflicting_data).
    ///
    /// Only concurrently-added data files that could contain records matching `predicate` are
    /// treated as conflicts. When unset, conflict detection scans every concurrently-added file
    /// (equivalent to `TRUE`).
    ///
    /// The filter only scopes validation; it is not tied to the files added or deleted by this
    /// action. Callers must set it to match the overwrite's effective predicate — a filter that
    /// does not cover what was overwritten will silently weaken the guarantee.
    pub fn conflict_detection_filter(mut self, predicate: Predicate) -> Self {
        self.conflict_detection_filter = Some(predicate);
        self
    }

    /// Override the snapshot that [conflict detection](Self::validate_no_conflicting_data) reads
    /// from. Conflicts are detected among snapshots committed after this one. Defaults to the
    /// table's current snapshot when the action was created.
    pub fn validate_from_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.from_snapshot_id = Some(snapshot_id);
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

        if self.validate_added_files {
            let conflict_filter = self
                .conflict_detection_filter
                .clone()
                .unwrap_or(Predicate::AlwaysTrue);
            validate_no_conflicting_data(table, self.from_snapshot_id, &conflict_filter).await?;
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

        if self.deleted_file_paths.is_empty() {
            return Ok(manifest_list
                .entries()
                .iter()
                .filter(|entry| entry.has_added_files() || entry.has_existing_files())
                .cloned()
                .collect());
        }

        let mut result = Vec::new();

        for manifest_file in manifest_list.entries() {
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
            table.metadata().current_schema().clone(),
            table.metadata().default_partition_spec().as_ref().clone(),
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
            if entry.is_alive() && self.deleted_file_paths.contains(entry.file_path()) {
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

    use crate::catalog::Catalog;
    use crate::expr::{Predicate, Reference};
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, Literal, MAIN_BRANCH,
        ManifestStatus, Operation, SnapshotRef, Struct,
    };
    use crate::table::Table;
    use crate::transaction::tests::{make_v2_minimal_table, make_v3_minimal_table_in_catalog};
    use crate::transaction::{ApplyTransactionAction, Transaction, TransactionAction};
    use crate::{ErrorKind, TableRequirement, TableUpdate};

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

    // Like `test_data_file` but with `y` (field id 2, a non-partition column) bounds set to [y, y].
    fn test_data_file_with_y_bounds(path: &str, partition_spec_id: i32, y: i64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition_spec_id(partition_spec_id)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .lower_bounds(HashMap::from([(2, Datum::long(y))]))
            .upper_bounds(HashMap::from([(2, Datum::long(y))]))
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

    async fn append(catalog: &impl Catalog, table: &Table, path: &str) -> Table {
        let spec_id = table.metadata().default_partition_spec_id();
        let tx = Transaction::new(table);
        let action = tx
            .fast_append()
            .add_data_files(vec![test_data_file(path, spec_id)]);
        action.apply(tx).unwrap().commit(catalog).await.unwrap()
    }

    // Builds a writer overwrite reading from `base`, then lands a concurrent append on top.
    async fn writer_and_concurrent_append(
        catalog: &impl Catalog,
        base: &Table,
        filter: Option<Predicate>,
    ) -> Transaction {
        let spec_id = base.metadata().default_partition_spec_id();
        let tx = Transaction::new(base);
        let mut action = tx
            .overwrite()
            .add_data_files(vec![test_data_file("test/replacement.parquet", spec_id)])
            .validate_no_conflicting_data();
        if let Some(filter) = filter {
            action = action.conflict_detection_filter(filter);
        }
        let writer_tx = action.apply(tx).unwrap();

        append(catalog, base, "test/concurrent.parquet").await;
        writer_tx
    }

    #[tokio::test]
    async fn test_overwrite_validate_detects_concurrent_append() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let base = append(&catalog, &table, "test/a.parquet").await;

        let writer = writer_and_concurrent_append(&catalog, &base, None).await;
        let err = writer.commit(&catalog).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }

    #[tokio::test]
    async fn test_overwrite_validate_filter_excludes_concurrent_append() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let base = append(&catalog, &table, "test/a.parquet").await;

        // Concurrent files live in partition x=300; a filter on x=999 prunes them.
        let filter = Reference::new("x").equal_to(Datum::long(999));
        let writer = writer_and_concurrent_append(&catalog, &base, Some(filter)).await;
        assert!(writer.commit(&catalog).await.is_ok());
    }

    #[tokio::test]
    async fn test_overwrite_validate_metrics_excludes_concurrent_append() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let base = append(&catalog, &table, "test/a.parquet").await;
        let spec_id = base.metadata().default_partition_spec_id();

        // Filter on `y`, a non-partition column, so the manifest passes partition-summary pruning
        // and the per-file metrics evaluator is what must exclude the concurrent file.
        let tx = Transaction::new(&base);
        let action = tx
            .overwrite()
            .add_data_files(vec![test_data_file("test/replacement.parquet", spec_id)])
            .validate_no_conflicting_data()
            .conflict_detection_filter(Reference::new("y").equal_to(Datum::long(999)));
        let writer_tx = action.apply(tx).unwrap();

        // Concurrent file's y bounds are [5, 5] — outside y = 999, so metrics must exclude it.
        let tx2 = Transaction::new(&base);
        let action = tx2
            .fast_append()
            .add_data_files(vec![test_data_file_with_y_bounds(
                "test/concurrent.parquet",
                spec_id,
                5,
            )]);
        action.apply(tx2).unwrap().commit(&catalog).await.unwrap();

        assert!(writer_tx.commit(&catalog).await.is_ok());
    }

    #[tokio::test]
    async fn test_overwrite_validate_from_snapshot_id_widens_window() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let snap_a = append(&catalog, &table, "test/a.parquet").await;
        let a_id = snap_a.metadata().current_snapshot_id().unwrap();
        let snap_b = append(&catalog, &snap_a, "test/b.parquet").await;
        let spec_id = snap_b.metadata().default_partition_spec_id();

        // Reading from B the default window is empty; overriding the start back to A pulls B's
        // added file into the window. If the override were ignored, the commit would succeed.
        let tx = Transaction::new(&snap_b);
        let action = tx
            .overwrite()
            .add_data_files(vec![test_data_file("test/replacement.parquet", spec_id)])
            .validate_no_conflicting_data()
            .validate_from_snapshot_id(a_id);
        let err = action
            .apply(tx)
            .unwrap()
            .commit(&catalog)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
    }
}
