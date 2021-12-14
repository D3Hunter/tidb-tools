// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	dmutils "github.com/pingcap/ticdc/dm/pkg/utils"
	tidbconfig "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/model"
	"github.com/siddontang/go/ioutil2"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/checkpoints"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/chunk"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/config"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/continuous"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/progress"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/report"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/source/common"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/splitter"
	"github.com/pingcap/tidb-tools/sync_diff_inspector/utils"
)

const (
	// checkpointFile represents the checkpoints' file name which used for save and loads chunks
	checkpointFile     = "sync_diff_checkpoints.pb"
	defaultDelay       = 5 * time.Second
	retryInterval      = 5 * time.Second
	batchRowCount      = 200
	validationInterval = time.Second // when there's not enough data to validate, we check it every validationInterval
)

// ChunkDML SQL struct for each chunk
type ChunkDML struct {
	node      *checkpoints.Node
	sqls      []string
	rowAdd    int
	rowDelete int
}

// Diff contains two sql DB, used for comparing.
type Diff struct {
	// we may have multiple sources in dm sharding sync.
	upstream   source.Source
	downstream source.Source

	// workSource is one of upstream/downstream by some policy in #pickSource.
	workSource source.Source

	sample           int
	checkThreadCount int
	exportFixSQL     bool
	useCheckpoint    bool
	ignoreDataCheck  bool
	sqlWg            sync.WaitGroup
	checkpointWg     sync.WaitGroup

	FixSQLDir     string
	CheckpointDir string

	sqlCh      chan *ChunkDML
	cp         *checkpoints.Checkpoint
	startRange *splitter.RangeInfo
	report     *report.Report

	continuousWg sync.WaitGroup
	cfg          *config.Config
	sync.RWMutex
	failedChanges      map[string]*tableChange
	failedRowCnt       atomic.Int64
	accumulatedChanges map[string]*tableChange
	pendingRowCnt      atomic.Int64
	rowsEventChan      chan *replication.BinlogEvent // unbuffered is enough
	pendingChangeCh    chan map[string]*tableChange
	changeEventCount   []int
	validationTimer    *time.Timer
}

// NewDiff returns a Diff instance.
func NewDiff(ctx context.Context, cfg *config.Config) (diff *Diff, err error) {
	diff = &Diff{
		checkThreadCount:   cfg.CheckThreadCount,
		exportFixSQL:       cfg.ExportFixSQL,
		ignoreDataCheck:    cfg.CheckStructOnly,
		sqlCh:              make(chan *ChunkDML, splitter.DefaultChannelBuffer),
		cp:                 new(checkpoints.Checkpoint),
		report:             report.NewReport(&cfg.Task),
		cfg:                cfg,
		failedChanges:      make(map[string]*tableChange),
		accumulatedChanges: make(map[string]*tableChange),
		rowsEventChan:      make(chan *replication.BinlogEvent),
		pendingChangeCh:    make(chan map[string]*tableChange),
		changeEventCount:   make([]int, rowUpdated+1),
		validationTimer:    time.NewTimer(validationInterval),
	}
	if err = diff.init(ctx, cfg); err != nil {
		diff.Close()
		return nil, errors.Trace(err)
	}

	return diff, nil
}

func (df *Diff) PrintSummary(ctx context.Context) bool {
	// Stop updating progress bar so that summary won't be flushed.
	progress.Close()
	df.report.CalculateTotalSize(ctx, df.downstream.GetDB())
	err := df.report.CommitSummary()
	if err != nil {
		log.Fatal("failed to commit report", zap.Error(err))
	}
	df.report.Print(os.Stdout)
	return df.report.Result == report.Pass
}

func (df *Diff) Close() {
	if df.upstream != nil {
		df.upstream.Close()
	}
	if df.downstream != nil {
		df.downstream.Close()
	}

	failpoint.Inject("wait-for-checkpoint", func() {
		log.Info("failpoint wait-for-checkpoint injected, skip delete checkpoint file.")
		failpoint.Return()
	})

	if err := os.Remove(filepath.Join(df.CheckpointDir, checkpointFile)); err != nil && !os.IsNotExist(err) {
		log.Fatal("fail to remove the checkpoint file", zap.String("error", err.Error()))
	}
}

func (df *Diff) init(ctx context.Context, cfg *config.Config) (err error) {
	// TODO adjust config
	setTiDBCfg()

	df.downstream, df.upstream, err = source.NewSources(ctx, cfg)
	if err != nil {
		return errors.Trace(err)
	}

	df.workSource = df.pickSource(ctx)
	df.FixSQLDir = cfg.Task.FixDir
	df.CheckpointDir = cfg.Task.CheckpointDir

	sourceConfigs, targetConfig, err := getConfigsForReport(cfg)
	if err != nil {
		return errors.Trace(err)
	}
	df.report.Init(df.downstream.GetTables(), sourceConfigs, targetConfig)
	if err := df.initCheckpoint(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (df *Diff) initCheckpoint() error {
	df.cp.Init()

	finishTableNums := 0
	path := filepath.Join(df.CheckpointDir, checkpointFile)
	if ioutil2.FileExists(path) {
		node, reportInfo, err := df.cp.LoadChunk(path)
		if err != nil {
			return errors.Annotate(err, "the checkpoint load process failed")
		} else {
			// this need not be synchronized, because at the moment, the is only one thread access the section
			log.Info("load checkpoint",
				zap.Any("chunk index", node.GetID()),
				zap.Reflect("chunk", node),
				zap.String("state", node.GetState()))
			df.cp.InitCurrentSavedID(node)
		}

		if node != nil {
			// remove the sql file that ID bigger than node.
			// cause we will generate these sql again.
			err = df.removeSQLFiles(node.GetID())
			if err != nil {
				return errors.Trace(err)
			}
			df.startRange = splitter.FromNode(node)
			df.report.LoadReport(reportInfo)
			finishTableNums = df.startRange.GetTableIndex()
			if df.startRange.ChunkRange.Type == chunk.Empty {
				// chunk_iter will skip this table directly
				finishTableNums++
			}
		}
	} else {
		log.Info("not found checkpoint file, start from beginning")
		id := &chunk.ChunkID{TableIndex: -1, BucketIndexLeft: -1, BucketIndexRight: -1, ChunkIndex: -1, ChunkCnt: 0}
		err := df.removeSQLFiles(id)
		if err != nil {
			return errors.Trace(err)
		}
	}
	if !df.cfg.Incremental {
		progress.Init(len(df.workSource.GetTables()), finishTableNums)
	}
	return nil
}

func encodeReportConfig(config *report.ReportConfig) ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := toml.NewEncoder(buf).Encode(config); err != nil {
		return nil, errors.Trace(err)
	}
	return buf.Bytes(), nil
}

func getConfigsForReport(cfg *config.Config) ([][]byte, []byte, error) {
	sourceConfigs := make([]*report.ReportConfig, len(cfg.Task.SourceInstances))
	for i := 0; i < len(cfg.Task.SourceInstances); i++ {
		instance := cfg.Task.SourceInstances[i]

		sourceConfigs[i] = &report.ReportConfig{
			Host:     instance.Host,
			Port:     instance.Port,
			User:     instance.User,
			Snapshot: instance.Snapshot,
			SqlMode:  instance.SqlMode,
		}
	}
	instance := cfg.Task.TargetInstance
	targetConfig := &report.ReportConfig{
		Host:     instance.Host,
		Port:     instance.Port,
		User:     instance.User,
		Snapshot: instance.Snapshot,
		SqlMode:  instance.SqlMode,
	}
	sourceBytes := make([][]byte, len(sourceConfigs))
	var err error
	for i := range sourceBytes {
		sourceBytes[i], err = encodeReportConfig(sourceConfigs[i])
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
	}
	targetBytes, err := encodeReportConfig(targetConfig)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return sourceBytes, targetBytes, nil
}

// Equal tests whether two database have same data and schema.
func (df *Diff) Equal(ctx context.Context) error {
	chunksIter, err := df.generateChunksIterator(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	defer chunksIter.Close()
	pool := utils.NewWorkerPool(uint(df.checkThreadCount), "consumer")
	stopCh := make(chan struct{})

	df.checkpointWg.Add(1)
	go df.handleCheckpoints(ctx, stopCh)
	df.sqlWg.Add(1)
	go df.writeSQLs(ctx)

	defer func() {
		pool.WaitFinished()
		log.Debug("all consume tasks finished")
		// close the sql channel
		close(df.sqlCh)
		df.sqlWg.Wait()
		stopCh <- struct{}{}
		df.checkpointWg.Wait()
	}()

	for {
		c, err := chunksIter.Next(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if c == nil {
			// finish read the tables
			break
		}
		log.Info("global consume chunk info", zap.Any("chunk index", c.ChunkRange.Index), zap.Any("chunk bound", c.ChunkRange.Bounds))
		pool.Apply(func() {
			isEqual := df.consume(ctx, c)
			if !isEqual {
				progress.FailTable(c.ProgressID)
			}
			progress.Inc(c.ProgressID)
		})
	}

	return nil
}

type rowChangeType int

const (
	rowInvalidChange rowChangeType = iota
	rowInsert
	rowDeleted
	rowUpdated
)

type tableChange struct {
	table *common.TableDiff
	rows  map[string]*rowChange
	minTs int64
}

type rowChange struct {
	pk         []string
	data       []interface{}
	theType    rowChangeType
	lastMeetTs int64 // the last meet timestamp(in seconds)
}

func (df *Diff) getContinueValidationSummary() (int, int64) {
	df.RLock()
	defer df.RUnlock()
	var count int
	var minTs int64 = math.MaxInt64
	for _, v := range df.failedChanges {
		count += len(v.rows)
		for _, r := range v.rows {
			if r.lastMeetTs < minTs {
				minTs = r.lastMeetTs
			}
		}
	}
	return count, minTs
}

// IncrementalValidate right now we assume there is only one upstream
func (df *Diff) IncrementalValidate(ctx context.Context) error {
	randomServerID, err := dmutils.GetRandomServerID(ctx, df.upstream.GetDB())
	if err != nil {
		return err
	}
	sources := df.upstream.(*source.MySQLSources)

	syncerCfg := replication.BinlogSyncerConfig{
		ServerID:       randomServerID,
		Flavor:         "mysql",
		Host:           sources.Ds[0].Host,
		Port:           uint16(sources.Ds[0].Port),
		User:           sources.Ds[0].User,
		Password:       sources.Ds[0].Password,
		UseDecimal:     false,
		VerifyChecksum: true,
	}

	//if !EnableGTID {
	//	syncerCfg.RawModeEnabled = true
	//}
	binlogSyncer := replication.NewBinlogSyncer(syncerCfg)
	gtidSet, _ := mysql.ParseMysqlGTIDSet("")
	binlogStreamer, err := binlogSyncer.StartSyncGTID(gtidSet)
	if err != nil {
		return err
	}
	log.Info("start incremental validation")

	df.continuousWg.Add(3)
	go df.retryFailedRows(ctx)
	go df.rowsEventProcessRoutine(ctx)
	go df.validateGoRoutine(ctx)

	// TODO context done
	var latestPos mysql.Position
	for {
		e, err := binlogStreamer.GetEvent(ctx)
		if err != nil {
			log.Error("get event failed", zap.Reflect("error", err))
			if myErr, ok := err.(*mysql.MyError); ok && myErr.Code == mysql.ER_MASTER_FATAL_ERROR_READING_BINLOG {
				binlogSyncer.Close()
				for {
					binlogSyncer = replication.NewBinlogSyncer(syncerCfg)
					binlogStreamer, err = binlogSyncer.StartSync(latestPos)
					if err != nil {
						binlogSyncer.Close()
						log.Error("failed to restart sync", zap.Reflect("error", err))
						time.Sleep(time.Second)
						continue
					}
					break
				}
			}
			continue
		}
		eventTime := time.Unix(int64(e.Header.Timestamp), 0)
		lag := time.Now().Sub(eventTime)
		// TODO delay should be configurable
		if lag < defaultDelay {
			time.Sleep(defaultDelay - lag)
		}

		switch ev := e.Event.(type) {
		case *replication.RotateEvent:
			latestPos.Name = string(ev.NextLogName)
		case *replication.QueryEvent:
			// TODO not processed now
		case *replication.RowsEvent:
			select {
			case df.rowsEventChan <- e:
			case <-ctx.Done():
				return nil
			}
		}
		latestPos.Pos = e.Header.LogPos
	}
	return nil
}

func getRowChangeType(t replication.EventType) rowChangeType {
	switch t {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		return rowInsert
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		return rowUpdated
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		return rowDeleted
	default:
		return rowInvalidChange
	}
}

func (df *Diff) rowsEventProcessRoutine(ctx context.Context) {
	df.continuousWg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-df.rowsEventChan:
			if err := df.processEventRows(e.Header, e.Event.(*replication.RowsEvent)); err != nil {
				log.Warn("failed to process event: ", zap.Reflect("error", err))
			}
		case <-df.validationTimer.C:
			rowCount := df.getRowCount(df.accumulatedChanges)
			if rowCount > 0 {
				df.pendingChangeCh <- df.accumulatedChanges
				df.accumulatedChanges = make(map[string]*tableChange)
			}
			df.validationTimer.Reset(validationInterval)
		}
	}
}

func (df *Diff) getRowCount(c map[string]*tableChange) int {
	res := 0
	for _, v := range c {
		res += len(v.rows)
	}
	return res
}

func (df *Diff) processEventRows(header *replication.EventHeader, ev *replication.RowsEvent) error {
	schemaName, tableName := string(ev.Table.Schema), string(ev.Table.Table)
	sources := df.upstream.(*source.MySQLSources)
	table := sources.GetTable(schemaName, tableName)
	if table == nil {
		return nil
	}
	if table.PrimaryKey == nil {
		panic("no primary index")
	}
	// TODO incomplete row event
	for _, cols := range ev.SkippedColumns {
		if len(cols) > 0 {
			return errors.New("")
		}
	}
	changeType := getRowChangeType(header.EventType)
	if changeType == rowInvalidChange {
		log.Info("ignoring unrecognized event", zap.Reflect("event header", header))
		return nil
	}

	df.changeEventCount[changeType]++

	init, step := 0, 1
	if changeType == rowUpdated {
		init, step = 1, 2
	}
	pk := table.PrimaryKey
	pkIndices := make([]int, len(pk.Columns))
	for i, col := range pk.Columns {
		pkIndices[i] = table.ColumnMap[col.Name.O].Offset
	}

	// TODO for every table merge events into batch
	// TODO for every table validate the batch
	rowCount := df.getRowCount(df.accumulatedChanges)
	fullTableName := fmt.Sprintf("%s.%s", table.Schema, table.Table)
	change := df.accumulatedChanges[fullTableName]
	for i := init; i < len(ev.Rows); i += step {
		row := ev.Rows[i]
		pkValue := make([]string, len(pk.Columns))
		for _, idx := range pkIndices {
			pkValue[idx] = fmt.Sprintf("%v", row[idx])
		}

		if change == nil {
			change = &tableChange{
				table: table,
				rows:  make(map[string]*rowChange),
			}
			df.accumulatedChanges[fullTableName] = change
		}
		key := strings.Join(pkValue, "-")
		val, ok := change.rows[key]
		if !ok {
			val = &rowChange{pk: pkValue}
			change.rows[key] = val
			rowCount++
			df.pendingRowCnt.Inc()
		}
		val.data = row
		val.theType = changeType
		val.lastMeetTs = int64(header.Timestamp)

		if rowCount >= batchRowCount {
			df.pendingChangeCh <- df.accumulatedChanges
			df.accumulatedChanges = make(map[string]*tableChange)

			if !df.validationTimer.Stop() {
				<-df.validationTimer.C
			}
			df.validationTimer.Reset(validationInterval)

			rowCount = 0
			change = nil
		}
	}

	// TODO make rows in small events into a batch, and group by table
	return nil
}

func (df *Diff) validateGoRoutine(ctx context.Context) {
	df.continuousWg.Done()
	for {
		select {
		case change := <-df.pendingChangeCh:
			df.Lock()
			failed := df.validateTableChange(ctx, change)
			df.updateFailedChanges(change, failed)
			df.failedRowCnt.Store(int64(df.getRowCount(df.failedChanges)))
			df.pendingRowCnt.Sub(int64(df.getRowCount(change)))
			df.Unlock()
		case <-ctx.Done():
			return
		}
	}
}

func (df *Diff) validateChanges(ctx context.Context, table *common.TableDiff, rows []*rowChange, deleteChange bool) [][]string {
	pkValues := make([][]string, 0, len(rows))
	for _, r := range rows {
		pkValues = append(pkValues, r.pk)
	}
	cond := &continuous.Cond{Table: table, PkValues: pkValues}
	var failedRows [][]string
	var err error
	if deleteChange {
		failedRows, err = df.validateDeletedRows(ctx, cond)
	} else {
		failedRows, err = df.validateInsertAndUpdateRows(ctx, rows, cond)
	}
	if err != nil {
		panic(err)
	}
	return failedRows
}

func (df *Diff) updateFailedChanges(all, failed map[string]*tableChange) {
	// remove previous failed rows related to current batch of rows
	for k, v := range all {
		prevFailed := df.failedChanges[k]
		if prevFailed == nil {
			continue
		}
		for _, r := range v.rows {
			key := strings.Join(r.pk, "-")
			delete(prevFailed.rows, key)
		}
	}
	for k, v := range failed {
		prevFailed := df.failedChanges[k]
		if prevFailed == nil {
			prevFailed = &tableChange{
				table: v.table,
				rows:  make(map[string]*rowChange),
			}
			df.failedChanges[k] = prevFailed
		}

		for _, r := range v.rows {
			key := strings.Join(r.pk, "-")
			prevFailed.rows[key] = r
		}
	}
}

func (df *Diff) validateTableChange(ctx context.Context, tableChanges map[string]*tableChange) map[string]*tableChange {
	failedChanges := make(map[string]*tableChange)
	for k, v := range tableChanges {
		var insertUpdateChanges, deleteChanges []*rowChange
		for _, r := range v.rows {
			if r.theType == rowDeleted {
				deleteChanges = append(deleteChanges, r)
			} else {
				insertUpdateChanges = append(insertUpdateChanges, r)
			}
		}
		rows := make(map[string]*rowChange, 0)
		if len(insertUpdateChanges) > 0 {
			failedRows := df.validateChanges(ctx, v.table, insertUpdateChanges, false)
			for _, pk := range failedRows {
				key := strings.Join(pk, "-")
				rows[key] = v.rows[key]
			}
		}
		if len(deleteChanges) > 0 {
			failedRows := df.validateChanges(ctx, v.table, deleteChanges, true)
			for _, pk := range failedRows {
				key := strings.Join(pk, "-")
				rows[key] = v.rows[key]
			}
		}
		if len(rows) > 0 {
			failedChanges[k] = &tableChange{
				table: v.table,
				rows:  rows,
			}
		}
	}
	return failedChanges
}

func (df *Diff) retryFailedRows(ctx context.Context) {
	df.continuousWg.Done()
	for {
		// TODO fine-grain lock
		// TODO limit number of failed rows
		// TODO limit number of retry, if number of retry > max_retry_count or after some time, move rows to error-rows
		// TODO if error-rows > max_error_rows, pause validation
		df.Lock()
		df.failedChanges = df.validateTableChange(ctx, df.failedChanges)
		df.failedRowCnt.Store(int64(df.getRowCount(df.failedChanges)))
		if df.failedRowCnt.Load() < 5 {
			for tableName, t := range df.failedChanges {
				for _, r := range t.rows {
					log.Info("failed row after retry: ",
						zap.String("table", tableName), zap.Reflect("key", r.pk),
						zap.Reflect("type", r.theType), zap.Int64("ts", r.lastMeetTs))
				}
			}
		}
		df.Unlock()
		cnt, ts := df.getContinueValidationSummary()
		if cnt > 0 {
			fmt.Printf("events: %3d/%3d/%3d, pending: %d, failed: %d, min ts: %v\n",
				df.changeEventCount[rowInsert], df.changeEventCount[rowUpdated], df.changeEventCount[rowDeleted],
				df.pendingRowCnt.Load(), df.failedRowCnt.Load(), time.Unix(ts, 0))
		} else {
			fmt.Printf("events: %3d/%3d/%3d, pending: %d, failed: %d\n",
				df.changeEventCount[rowInsert], df.changeEventCount[rowUpdated], df.changeEventCount[rowDeleted],
				df.pendingRowCnt.Load(), df.failedRowCnt.Load())
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(retryInterval): // TODO wait time be configurable?
		}
	}
}

func (df *Diff) StructEqual(ctx context.Context) error {
	tables := df.downstream.GetTables()
	tableIndex := 0
	if df.startRange != nil {
		tableIndex = df.startRange.ChunkRange.Index.TableIndex
	}
	for ; tableIndex < len(tables); tableIndex++ {
		isEqual, isSkip, err := df.compareStruct(ctx, tableIndex)
		if err != nil {
			return errors.Trace(err)
		}
		progress.RegisterTable(dbutil.TableName(tables[tableIndex].Schema, tables[tableIndex].Table), !isEqual, isSkip)
		df.report.SetTableStructCheckResult(tables[tableIndex].Schema, tables[tableIndex].Table, isEqual, isSkip)
	}
	return nil
}

func (df *Diff) compareStruct(ctx context.Context, tableIndex int) (isEqual bool, isSkip bool, err error) {
	sourceTableInfos, err := df.upstream.GetSourceStructInfo(ctx, tableIndex)
	if err != nil {
		return false, true, errors.Trace(err)
	}
	table := df.downstream.GetTables()[tableIndex]
	isEqual, isSkip = utils.CompareStruct(sourceTableInfos, table.Info)
	table.IgnoreDataCheck = isSkip
	return isEqual, isSkip, nil
}

func (df *Diff) startGCKeeperForTiDB(ctx context.Context, db *sql.DB, snap string) {
	pdCli, _ := utils.GetPDClientForGC(ctx, db)
	if pdCli != nil {
		// Get latest snapshot
		latestSnap, err := utils.GetSnapshot(ctx, db)
		if err != nil {
			log.Info("failed to get snapshot, user should guarantee the GC stopped during diff progress.")
			return
		}

		if len(latestSnap) == 1 {
			if len(snap) == 0 {
				snap = latestSnap[0]
			}
			// compare the snapshot and choose the small one to lock
			if strings.Compare(latestSnap[0], snap) < 0 {
				snap = latestSnap[0]
			}
		}

		err = utils.StartGCSavepointUpdateService(ctx, pdCli, db, snap)
		if err != nil {
			log.Info("failed to keep snapshot, user should guarantee the GC stopped during diff progress.")
		} else {
			log.Info("start update service to keep GC stopped automatically")
		}
	}
}

// pickSource pick one proper source to do some work. e.g. generate chunks
func (df *Diff) pickSource(ctx context.Context) source.Source {
	workSource := df.downstream
	if ok, _ := dbutil.IsTiDB(ctx, df.upstream.GetDB()); ok {
		log.Info("The upstream is TiDB. pick it as work source candidate")
		df.startGCKeeperForTiDB(ctx, df.upstream.GetDB(), df.upstream.GetSnapshot())
		workSource = df.upstream
	}
	if ok, _ := dbutil.IsTiDB(ctx, df.downstream.GetDB()); ok {
		log.Info("The downstream is TiDB. pick it as work source first")
		df.startGCKeeperForTiDB(ctx, df.downstream.GetDB(), df.downstream.GetSnapshot())
		workSource = df.downstream
	}
	return workSource
}

func (df *Diff) generateChunksIterator(ctx context.Context) (source.RangeIterator, error) {
	return df.workSource.GetRangeIterator(ctx, df.startRange, df.workSource.GetTableAnalyzer())
}

func (df *Diff) handleCheckpoints(ctx context.Context, stopCh chan struct{}) {
	// a background goroutine which will insert the verified chunk,
	// and periodically save checkpoint
	log.Info("start handleCheckpoint goroutine")
	defer func() {
		log.Info("close handleCheckpoint goroutine")
		df.checkpointWg.Done()
	}()
	flush := func() {
		chunk := df.cp.GetChunkSnapshot()
		if chunk != nil {
			tableDiff := df.downstream.GetTables()[chunk.GetTableIndex()]
			schema, table := tableDiff.Schema, tableDiff.Table
			r, err := df.report.GetSnapshot(chunk.GetID(), schema, table)
			if err != nil {
				log.Warn("fail to save the report", zap.Error(err))
			}
			_, err = df.cp.SaveChunk(ctx, filepath.Join(df.CheckpointDir, checkpointFile), chunk, r)
			if err != nil {
				log.Warn("fail to save the chunk", zap.Error(err))
				// maybe we should panic, because SaveChunk method should not failed.
			}
		}
	}
	defer flush()
	for {
		select {
		case <-ctx.Done():
			log.Info("Stop do checkpoint by context done")
			return
		case <-stopCh:
			log.Info("Stop do checkpoint")
			return
		case <-time.After(10 * time.Second):
			flush()
		}
	}
}

func (df *Diff) consume(ctx context.Context, rangeInfo *splitter.RangeInfo) bool {
	dml := &ChunkDML{
		node: rangeInfo.ToNode(),
	}
	defer func() { df.sqlCh <- dml }()
	if rangeInfo.ChunkRange.Type == chunk.Empty {
		dml.node.State = checkpoints.IgnoreState
		return true
	}
	tableDiff := df.downstream.GetTables()[rangeInfo.GetTableIndex()]
	schema, table := tableDiff.Schema, tableDiff.Table
	var state string = checkpoints.SuccessState

	isEqual, count, err := df.compareChecksumAndGetCount(ctx, rangeInfo)
	if err != nil {
		// If an error occurs during the checksum phase, skip the data compare phase.
		state = checkpoints.FailedState
		df.report.SetTableMeetError(schema, table, err)
	} else if !isEqual && df.exportFixSQL {
		log.Debug("checksum failed", zap.Any("chunk id", rangeInfo.ChunkRange.Index), zap.Int64("chunk size", count), zap.String("table", df.workSource.GetTables()[rangeInfo.GetTableIndex()].Table))
		state = checkpoints.FailedState
		// if the chunk's checksum differ, try to do binary check
		info := rangeInfo
		if count > splitter.SplitThreshold {
			log.Debug("count greater than threshold, start do bingenerate", zap.Any("chunk id", rangeInfo.ChunkRange.Index), zap.Int64("chunk size", count))
			info, err = df.BinGenerate(ctx, df.workSource, rangeInfo, count)
			if err != nil {
				log.Error("fail to do binary search.", zap.Error(err))
				df.report.SetTableMeetError(schema, table, err)
				// reuse rangeInfo to compare data
				info = rangeInfo
			} else {
				log.Debug("bin generate finished", zap.Reflect("chunk", info.ChunkRange), zap.Any("chunk id", info.ChunkRange.Index))
			}
		}
		isDataEqual, err := df.compareRows(ctx, info, dml)
		if err != nil {
			df.report.SetTableMeetError(schema, table, err)
		}
		isEqual = isEqual && isDataEqual
	}
	dml.node.State = state
	id := rangeInfo.ChunkRange.Index
	df.report.SetTableDataCheckResult(schema, table, isEqual, dml.rowAdd, dml.rowDelete, id)
	return isEqual
}

func (df *Diff) BinGenerate(ctx context.Context, targetSource source.Source, tableRange *splitter.RangeInfo, count int64) (*splitter.RangeInfo, error) {
	if count <= splitter.SplitThreshold {
		return tableRange, nil
	}
	tableDiff := targetSource.GetTables()[tableRange.GetTableIndex()]
	indices := dbutil.FindAllIndex(tableDiff.Info)
	// if no index, do not split
	if len(indices) == 0 {
		log.Warn("cannot found an index to split and disable the BinGenerate",
			zap.String("table", dbutil.TableName(tableDiff.Schema, tableDiff.Table)))
		return tableRange, nil
	}
	var index *model.IndexInfo
	// using the index
	for _, i := range indices {
		if tableRange.IndexID == i.ID {
			index = i
			break
		}
	}
	if index == nil {
		log.Warn("have indices but cannot found a proper index to split and disable the BinGenerate",
			zap.String("table", dbutil.TableName(tableDiff.Schema, tableDiff.Table)))
		return tableRange, nil
	}
	// TODO use selectivity from utils.GetBetterIndex
	// only support PK/UK
	if !(index.Primary || index.Unique) {
		log.Warn("BinGenerate only support PK/UK")
		return tableRange, nil
	}

	log.Debug("index for BinGenerate", zap.String("index", index.Name.O))
	indexColumns := utils.GetColumnsFromIndex(index, tableDiff.Info)
	if len(indexColumns) == 0 {
		log.Warn("fail to get columns of the selected index, directly return the origin chunk")
		return tableRange, nil
	}

	return df.binSearch(ctx, targetSource, tableRange, count, tableDiff, indexColumns)
}

func (df *Diff) binSearch(ctx context.Context, targetSource source.Source, tableRange *splitter.RangeInfo, count int64, tableDiff *common.TableDiff, indexColumns []*model.ColumnInfo) (*splitter.RangeInfo, error) {
	if count <= splitter.SplitThreshold {
		return tableRange, nil
	}
	var (
		isEqual1, isEqual2 bool
		count1, count2     int64
	)
	tableRange1 := tableRange.Copy()
	tableRange2 := tableRange.Copy()

	chunkLimits, args := tableRange.ChunkRange.ToString(tableDiff.Collation)
	limitRange := fmt.Sprintf("(%s) AND (%s)", chunkLimits, tableDiff.Range)
	midValues, err := utils.GetApproximateMidBySize(ctx, targetSource.GetDB(), tableDiff.Schema, tableDiff.Table, indexColumns, limitRange, args, count)
	log.Debug("mid values", zap.Reflect("mid values", midValues), zap.Reflect("indices", indexColumns), zap.Reflect("bounds", tableRange.ChunkRange.Bounds))
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Debug("table ranges", zap.Reflect("original range", tableRange))
	for i := range indexColumns {
		log.Debug("update tableRange", zap.String("field", indexColumns[i].Name.O), zap.String("value", midValues[indexColumns[i].Name.O]))
		tableRange1.Update(indexColumns[i].Name.O, "", midValues[indexColumns[i].Name.O], false, true, tableDiff.Collation, tableDiff.Range)
		tableRange2.Update(indexColumns[i].Name.O, midValues[indexColumns[i].Name.O], "", true, false, tableDiff.Collation, tableDiff.Range)
	}
	log.Debug("table ranges", zap.Reflect("tableRange 1", tableRange1), zap.Reflect("tableRange 2", tableRange2))
	isEqual1, count1, err = df.compareChecksumAndGetCount(ctx, tableRange1)
	if err != nil {
		return nil, errors.Trace(err)
	}
	isEqual2, count2, err = df.compareChecksumAndGetCount(ctx, tableRange2)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if count1+count2 != count {
		log.Fatal("the count is not correct",
			zap.Int64("count1", count1),
			zap.Int64("count2", count2),
			zap.Int64("count", count))
	}
	log.Info("chunk split successfully",
		zap.Any("chunk id", tableRange.ChunkRange.Index),
		zap.Int64("count1", count1),
		zap.Int64("count2", count2))

	if !isEqual1 && !isEqual2 {
		return tableRange, nil
	} else if !isEqual1 {
		c, err := df.binSearch(ctx, targetSource, tableRange1, count1, tableDiff, indexColumns)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return c, nil
	} else if !isEqual2 {
		c, err := df.binSearch(ctx, targetSource, tableRange2, count2, tableDiff, indexColumns)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return c, nil
	} else {
		// TODO: handle the error to foreground
		log.Fatal("the isEqual1 and isEqual2 cannot be both true")
		return nil, nil
	}
}

func (df *Diff) compareChecksumAndGetCount(ctx context.Context, tableRange *splitter.RangeInfo) (bool, int64, error) {
	var wg sync.WaitGroup
	var upstreamInfo, downstreamInfo *source.ChecksumInfo
	wg.Add(1)
	go func() {
		defer wg.Done()
		upstreamInfo = df.upstream.GetCountAndCrc32(ctx, tableRange)
	}()
	downstreamInfo = df.downstream.GetCountAndCrc32(ctx, tableRange)
	wg.Wait()

	if upstreamInfo.Err != nil {
		log.Warn("failed to compare upstream checksum")
		return false, -1, errors.Trace(upstreamInfo.Err)
	}
	if downstreamInfo.Err != nil {
		log.Warn("failed to compare downstream checksum")
		return false, -1, errors.Trace(downstreamInfo.Err)

	}
	// TODO two counts are not necessary equal
	if upstreamInfo.Count == downstreamInfo.Count && upstreamInfo.Checksum == downstreamInfo.Checksum {
		return true, upstreamInfo.Count, nil
	}
	return false, upstreamInfo.Count, nil
}

func (df *Diff) validateDeletedRows(ctx context.Context, cond *continuous.Cond) ([][]string, error) {
	downstreamRowsIterator, err := df.downstream.GetRows(ctx, cond)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer downstreamRowsIterator.Close()

	var failedRows [][]string
	for {
		data, err := downstreamRowsIterator.Next()
		if err != nil {
			return nil, err
		}
		if data == nil {
			break
		}
		failedRows = append(failedRows, getPkValues(data, cond))
	}
	return failedRows, nil
}

func getPkValues(data map[string]*dbutil.ColumnData, cond *continuous.Cond) []string {
	var pkValues []string
	for _, pkColumn := range cond.Table.PrimaryKey.Columns {
		// TODO primary key cannot be null, if we uses unique key should make sure all columns are not null
		pkValues = append(pkValues, string(data[pkColumn.Name.O].Data))
	}
	return pkValues
}

func (df *Diff) getRowChangeIterator(table *common.TableDiff, rows []*rowChange) (source.RowDataIterator, error) {
	sort.Slice(rows, func(i, j int) bool {
		left, right := rows[i], rows[j]
		for idx := range left.pk {
			if left.pk[idx] != right.pk[idx] {
				return left.pk[idx] < right.pk[idx]
			}
		}
		return false
	})

	// TODO columns in table.Info.Columns may diff with binlog row columns
	// TODO for datetime/timestamp type, should make sure timezone is the same between upstream and downstream
	it := &continuous.SimpleRowsIterator{}
	for _, r := range rows {
		colMap := make(map[string]*dbutil.ColumnData)
		for _, c := range table.Info.Columns {
			var colData []byte
			if r.data[c.Offset] != nil {
				colData = []byte(fmt.Sprintf("%v", r.data[c.Offset]))
			}
			colMap[c.Name.O] = &dbutil.ColumnData{
				Data:   colData,
				IsNull: r.data[c.Offset] == nil,
			}
		}
		it.Rows = append(it.Rows, colMap)
	}
	return it, nil
}

func (df *Diff) validateInsertAndUpdateRows(ctx context.Context, rows []*rowChange, cond *continuous.Cond) ([][]string, error) {
	var failedRows [][]string
	// TODO support both ways in case binlog doesn't contain complete rows
	var upstreamRowsIterator source.RowDataIterator
	var err error
	if df.cfg.UseBinlogForCompare {
		upstreamRowsIterator, err = df.getRowChangeIterator(cond.Table, rows)
	} else {
		upstreamRowsIterator, err = df.upstream.GetRows(ctx, cond)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer upstreamRowsIterator.Close()
	downstreamRowsIterator, err := df.downstream.GetRows(ctx, cond)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer downstreamRowsIterator.Close()

	var lastUpstreamData, lastDownstreamData map[string]*dbutil.ColumnData

	tableInfo := cond.Table.Info
	_, orderKeyCols := dbutil.SelectUniqueOrderKey(tableInfo)
	for {
		if lastUpstreamData == nil {
			lastUpstreamData, err = upstreamRowsIterator.Next()
			if err != nil {
				return nil, err
			}
		}

		if lastDownstreamData == nil {
			lastDownstreamData, err = downstreamRowsIterator.Next()
			if err != nil {
				return nil, err
			}
		}

		// may have deleted on upstream and haven't synced to downstream,
		// we mark this as success as we'll check the delete-event later
		// or downstream removed the pk and added more data by other clients, skip it.
		if lastUpstreamData == nil && lastDownstreamData != nil {
			log.Debug("more data on downstream, may come from other client, skip it")
			break
		}

		if lastDownstreamData == nil {
			// target lack some data, should insert the last source datas
			for lastUpstreamData != nil {
				failedRows = append(failedRows, getPkValues(lastUpstreamData, cond))

				lastUpstreamData, err = upstreamRowsIterator.Next()
				if err != nil {
					return nil, err
				}
			}
			break
		}

		eq, cmp, err := utils.CompareData(lastUpstreamData, lastDownstreamData, orderKeyCols, tableInfo.Columns)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if eq {
			lastDownstreamData = nil
			lastUpstreamData = nil
			continue
		}

		switch cmp {
		case 1:
			// may have deleted on upstream and haven't synced to downstream,
			// we mark this as success as we'll check the delete-event later
			// or downstream removed the pk and added more data by other clients, skip it.
			log.Debug("more data on downstream, may come from other client, skip it", zap.Reflect("data", lastDownstreamData))
			lastDownstreamData = nil
		case -1:
			failedRows = append(failedRows, getPkValues(lastUpstreamData, cond))
			lastUpstreamData = nil
		case 0:
			failedRows = append(failedRows, getPkValues(lastUpstreamData, cond))
			lastUpstreamData = nil
			lastDownstreamData = nil
		}
	}
	return failedRows, nil
}

func (df *Diff) compareRows(ctx context.Context, rangeInfo *splitter.RangeInfo, dml *ChunkDML) (bool, error) {
	rowsAdd, rowsDelete := 0, 0
	upstreamRowsIterator, err := df.upstream.GetRowsIterator(ctx, rangeInfo)
	if err != nil {
		return false, errors.Trace(err)
	}
	defer upstreamRowsIterator.Close()
	downstreamRowsIterator, err := df.downstream.GetRowsIterator(ctx, rangeInfo)
	if err != nil {
		return false, errors.Trace(err)
	}
	defer downstreamRowsIterator.Close()

	var lastUpstreamData, lastDownstreamData map[string]*dbutil.ColumnData
	equal := true

	tableInfo := df.workSource.GetTables()[rangeInfo.GetTableIndex()].Info
	_, orderKeyCols := dbutil.SelectUniqueOrderKey(tableInfo)
	for {
		if lastUpstreamData == nil {
			lastUpstreamData, err = upstreamRowsIterator.Next()
			if err != nil {
				return false, err
			}
		}

		if lastDownstreamData == nil {
			lastDownstreamData, err = downstreamRowsIterator.Next()
			if err != nil {
				return false, err
			}
		}

		if lastUpstreamData == nil {
			// don't have source data, so all the targetRows's data is redundant, should be deleted
			for lastDownstreamData != nil {
				sql := df.downstream.GenerateFixSQL(source.Delete, lastUpstreamData, lastDownstreamData, rangeInfo.GetTableIndex())
				rowsDelete++
				log.Debug("[delete]", zap.String("sql", sql))

				dml.sqls = append(dml.sqls, sql)
				equal = false
				lastDownstreamData, err = downstreamRowsIterator.Next()
				if err != nil {
					return false, err
				}
			}
			break
		}

		if lastDownstreamData == nil {
			// target lack some data, should insert the last source datas
			for lastUpstreamData != nil {
				sql := df.downstream.GenerateFixSQL(source.Insert, lastUpstreamData, lastDownstreamData, rangeInfo.GetTableIndex())
				rowsAdd++
				log.Debug("[insert]", zap.String("sql", sql))

				dml.sqls = append(dml.sqls, sql)
				equal = false

				lastUpstreamData, err = upstreamRowsIterator.Next()
				if err != nil {
					return false, err
				}
			}
			break
		}

		eq, cmp, err := utils.CompareData(lastUpstreamData, lastDownstreamData, orderKeyCols, tableInfo.Columns)
		if err != nil {
			return false, errors.Trace(err)
		}
		if eq {
			lastDownstreamData = nil
			lastUpstreamData = nil
			continue
		}

		equal = false
		sql := ""

		switch cmp {
		case 1:
			// delete
			sql = df.downstream.GenerateFixSQL(source.Delete, lastUpstreamData, lastDownstreamData, rangeInfo.GetTableIndex())
			rowsDelete++
			log.Debug("[delete]", zap.String("sql", sql))
			lastDownstreamData = nil
		case -1:
			// insert
			sql = df.downstream.GenerateFixSQL(source.Insert, lastUpstreamData, lastDownstreamData, rangeInfo.GetTableIndex())
			rowsAdd++
			log.Debug("[insert]", zap.String("sql", sql))
			lastUpstreamData = nil
		case 0:
			// update
			sql = df.downstream.GenerateFixSQL(source.Replace, lastUpstreamData, lastDownstreamData, rangeInfo.GetTableIndex())
			rowsAdd++
			rowsDelete++
			log.Debug("[update]", zap.String("sql", sql))
			lastUpstreamData = nil
			lastDownstreamData = nil
		}

		dml.sqls = append(dml.sqls, sql)
	}
	dml.rowAdd = rowsAdd
	dml.rowDelete = rowsDelete
	return equal, nil
}

// WriteSQLs write sqls to file
func (df *Diff) writeSQLs(ctx context.Context) {
	log.Info("start writeSQLs goroutine")
	defer func() {
		log.Info("close writeSQLs goroutine")
		df.sqlWg.Done()
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case dml, ok := <-df.sqlCh:
			if !ok && dml == nil {
				log.Info("write sql channel closed")
				return
			}
			if len(dml.sqls) > 0 {
				tableDiff := df.downstream.GetTables()[dml.node.GetTableIndex()]
				fileName := fmt.Sprintf("%s:%s:%s.sql", tableDiff.Schema, tableDiff.Table, utils.GetSQLFileName(dml.node.GetID()))
				fixSQLPath := filepath.Join(df.FixSQLDir, fileName)
				if ok := ioutil2.FileExists(fixSQLPath); ok {
					// unreachable
					log.Fatal("write sql failed: repeat sql happen", zap.Strings("sql", dml.sqls))
				}
				fixSQLFile, err := os.Create(fixSQLPath)
				if err != nil {
					log.Fatal("write sql failed: cannot create file", zap.Strings("sql", dml.sqls), zap.Error(err))
					continue
				}
				// write chunk meta
				chunkRange := dml.node.ChunkRange
				fixSQLFile.WriteString(fmt.Sprintf("-- table: %s.%s\n-- %s\n", tableDiff.Schema, tableDiff.Table, chunkRange.ToMeta()))
				if tableDiff.NeedUnifiedTimeZone {
					fixSQLFile.WriteString(fmt.Sprintf("set @@session.time_zone = \"%s\";\n", source.UnifiedTimeZone))
				}
				for _, sql := range dml.sqls {
					_, err = fixSQLFile.WriteString(fmt.Sprintf("%s\n", sql))
					if err != nil {
						log.Fatal("write sql failed", zap.String("sql", sql), zap.Error(err))
					}
				}
				fixSQLFile.Close()
			}
			log.Debug("insert node", zap.Any("chunk index", dml.node.GetID()))
			df.cp.Insert(dml.node)
		}
	}
}

func (df *Diff) removeSQLFiles(checkPointId *chunk.ChunkID) error {
	ts := time.Now().Format("2006-01-02T15:04:05Z07:00")
	dirName := fmt.Sprintf(".trash-%s", ts)
	folderPath := filepath.Join(df.FixSQLDir, dirName)

	if _, err := os.Stat(folderPath); os.IsNotExist(err) {
		err = os.MkdirAll(folderPath, os.ModePerm)
		if err != nil {
			return errors.Trace(err)
		}
	}

	err := filepath.Walk(df.FixSQLDir, func(path string, f fs.FileInfo, err error) error {
		if os.IsNotExist(err) {
			// if path not exists, we should return nil to continue.
			return nil
		}
		if err != nil {
			return errors.Trace(err)
		}

		if f == nil || f.IsDir() {
			return nil
		}

		name := f.Name()
		// in mac osx, the path parameter is absolute path; in linux, the path is relative path to execution base dir,
		// so use Rel to convert to relative path to l.base
		relPath, _ := filepath.Rel(df.FixSQLDir, path)
		oldPath := filepath.Join(df.FixSQLDir, relPath)
		newPath := filepath.Join(folderPath, relPath)
		if strings.Contains(oldPath, ".trash") {
			return nil
		}

		if strings.HasSuffix(name, ".sql") {
			fileIDStr := strings.TrimRight(name, ".sql")
			fileIDSubstrs := strings.SplitN(fileIDStr, ":", 3)
			if len(fileIDSubstrs) != 3 {
				return nil
			}
			tableIndex, bucketIndexLeft, bucketIndexRight, chunkIndex, err := utils.GetChunkIDFromSQLFileName(fileIDSubstrs[2])
			if err != nil {
				return errors.Trace(err)
			}
			fileID := &chunk.ChunkID{
				TableIndex: tableIndex, BucketIndexLeft: bucketIndexLeft, BucketIndexRight: bucketIndexRight, ChunkIndex: chunkIndex, ChunkCnt: 0,
			}
			if err != nil {
				return errors.Trace(err)
			}
			if fileID.Compare(checkPointId) > 0 {
				// move to trash
				err = os.Rename(oldPath, newPath)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func setTiDBCfg() {
	// to support long index key in TiDB
	tidbCfg := tidbconfig.GetGlobalConfig()
	// 3027 * 4 is the max value the MaxIndexLength can be set
	tidbCfg.MaxIndexLength = 3027 * 4
	tidbconfig.StoreGlobalConfig(tidbCfg)

	log.Info("set tidb cfg")
}
