package clickhouse

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/AlexAkulov/clickhouse-backup/pkg/config"
	"github.com/AlexAkulov/clickhouse-backup/pkg/metadata"
	"github.com/ClickHouse/clickhouse-go/v2"
	apexLog "github.com/apex/log"
)

// ClickHouse - provide
type ClickHouse struct {
	Config  *config.ClickHouseConfig
	Log     *apexLog.Entry
	conn    driver.Conn
	disks   []Disk
	version int
	IsOpen  bool
}

// Connect - establish connection to ClickHouse
func (ch *ClickHouse) Connect() error {
	if ch.IsOpen {
		if err := ch.conn.Close(); err != nil {
			ch.Log.Errorf("close previous connection error: %v", err)
		}
	}
	ch.IsOpen = false
	timeout, err := time.ParseDuration(ch.Config.Timeout)
	if err != nil {
		return err
	}

	timeoutSeconds := fmt.Sprintf("%d", int(timeout.Seconds()))

	opt := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", ch.Config.Host, ch.Config.Port)},
		Auth: clickhouse.Auth{
			Database: "system",
			Username: ch.Config.Username,
			Password: ch.Config.Password,
		},
		Settings: clickhouse.Settings{
			"connect_timeout": timeoutSeconds,
			"receive_timeout": timeoutSeconds,
			"send_timeout":    timeoutSeconds,
		},
		MaxOpenConns:    1,
		ConnMaxLifetime: 0,
		MaxIdleConns:    0,
		DialTimeout:     timeout,
	}

	if ch.Config.Debug {
		opt.Debug = true
	}

	if ch.Config.Secure {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: ch.Config.SkipVerify,
		}
		if ch.Config.TLSKey != "" || ch.Config.TLSCert != "" || ch.Config.TLSCa != "" {
			if ch.Config.TLSCert != "" || ch.Config.TLSKey != "" {
				cert, err := tls.LoadX509KeyPair(ch.Config.TLSCert, ch.Config.TLSKey)
				if err != nil {
					ch.Log.Errorf("tls.LoadX509KeyPair error: %v", err)
					return err
				}
				tlsConfig.Certificates = []tls.Certificate{cert}
			}
			if ch.Config.TLSCa != "" {
				caCert, err := os.ReadFile(ch.Config.TLSCa)
				if err != nil {
					ch.Log.Errorf("read `tls_ca` file %s return error: %v ", ch.Config.TLSCa, err)
					return err
				}
				caCertPool := x509.NewCertPool()
				if caCertPool.AppendCertsFromPEM(caCert) != true {
					ch.Log.Errorf("AppendCertsFromPEM %s return false", ch.Config.TLSCa)
					return fmt.Errorf("AppendCertsFromPEM %s return false", ch.Config.TLSCa)
				}
				tlsConfig.RootCAs = caCertPool
			}
		}
		opt.TLS = tlsConfig
	}
	if !ch.Config.LogSQLQueries {
		opt.Settings["log_queries"] = 0
	}

	if ch.conn, err = clickhouse.Open(opt); err != nil {
		ch.Log.Errorf("clickhouse connection: %s, sql.Open return error: %v", fmt.Sprintf("tcp://%v:%v", ch.Config.Host, ch.Config.Port), err)
		return err
	}

	logFunc := ch.Log.Infof
	if !ch.Config.LogSQLQueries {
		logFunc = ch.Log.Debugf
	}
	logFunc("clickhouse connection prepared: %s run ping", fmt.Sprintf("tcp://%v:%v", ch.Config.Host, ch.Config.Port))
	err = ch.conn.Ping(context.Background())
	if err != nil {
		ch.Log.Errorf("clickhouse connection ping: %s return error: %v", fmt.Sprintf("tcp://%v:%v", ch.Config.Host, ch.Config.Port), err)
		return err
	} else {
		ch.IsOpen = true
	}
	logFunc("clickhouse connection open: %s", fmt.Sprintf("tcp://%v:%v", ch.Config.Host, ch.Config.Port))
	return err
}

// GetDisks - return data from system.disks table
func (ch *ClickHouse) GetDisks(ctx context.Context) ([]Disk, error) {
	version, err := ch.GetVersion(ctx)
	if err != nil {
		return nil, err
	}
	var disks []Disk
	if version < 19015000 {
		disks, err = ch.getDisksFromSystemSettings(ctx)
	} else {
		disks, err = ch.getDisksFromSystemDisks(ctx)
	}
	if err != nil {
		return nil, err
	}
	for i := range disks {
		if disks[i].Name == ch.Config.EmbeddedBackupDisk {
			disks[i].IsBackup = true
		}
	}
	if len(ch.Config.DiskMapping) == 0 {
		return disks, nil
	}
	dm := map[string]string{}
	for k, v := range ch.Config.DiskMapping {
		dm[k] = v
	}
	for i := range disks {
		if p, ok := dm[disks[i].Name]; ok {
			disks[i].Path = p
			delete(dm, disks[i].Name)
		}
	}
	for k, v := range dm {
		disks = append(disks, Disk{
			Name: k,
			Path: v,
			Type: "local",
		})
	}
	return disks, nil
}

func (ch *ClickHouse) GetEmbeddedBackupPath(disks []Disk) (string, error) {
	if !ch.Config.UseEmbeddedBackupRestore {
		return "", nil
	}
	if ch.Config.EmbeddedBackupDisk == "" {
		return "", fmt.Errorf("please setup `clickhouse->embedded_backup_disk` in config or CLICKHOUSE_EMBEDDED_BACKUP_DISK environment variable")
	}
	for _, d := range disks {
		if d.Name == ch.Config.EmbeddedBackupDisk {
			return d.Path, nil
		}
	}
	return "", fmt.Errorf("%s not found in system.disks %v", ch.Config.EmbeddedBackupDisk, disks)
}

func (ch *ClickHouse) GetDefaultPath(disks []Disk) (string, error) {
	defaultPath := "/var/lib/clickhouse"
	for _, d := range disks {
		if d.Name == "default" {
			defaultPath = d.Path
			break
		}
	}
	return defaultPath, nil
}

func (ch *ClickHouse) getDisksFromSystemSettings(ctx context.Context) ([]Disk, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		var result []struct {
			MetadataPath string `ch:"metadata_path"`
		}
		query := "SELECT metadata_path FROM system.tables WHERE database = 'system' AND metadata_path!='' LIMIT 1;"
		if err := ch.SelectContext(ctx, &result, query); err != nil {
			return nil, err
		}
		if len(result) == 0 {
			return nil, fmt.Errorf("can't get metadata_path from system.tables")
		}
		metadataPath := result[0].MetadataPath
		dataPathArray := strings.Split(metadataPath, "/")
		clickhouseData := path.Join(dataPathArray[:len(dataPathArray)-3]...)
		return []Disk{{
			Name: "default",
			Path: path.Join("/", clickhouseData),
			Type: "local",
		}}, nil
	}
}

func (ch *ClickHouse) getDisksFromSystemDisks(ctx context.Context) ([]Disk, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		var result []Disk
		query := "SELECT name, path, type FROM system.disks;"
		err := ch.Select(&result, query)
		return result, err
	}
}

// Close - closing connection to ClickHouse
func (ch *ClickHouse) Close() {
	if ch.IsOpen {
		if err := ch.conn.Close(); err != nil {
			ch.Log.Warnf("can't close clickhouse connection: %v", err)
		}
	}
	if ch.Config.LogSQLQueries {
		ch.Log.Info("clickhouse connection closed")
	} else {
		ch.Log.Debug("clickhouse connection closed")
	}
	ch.IsOpen = false
}

// GetTables - return slice of all tables suitable for backup, MySQL and PostgresSQL database engine shall be skipped
func (ch *ClickHouse) GetTables(ctx context.Context, tablePattern string) ([]Table, error) {
	var err error
	tables := make([]Table, 0)
	var isUUIDPresent uint64
	if err = ch.SelectSingleRow(ctx, &isUUIDPresent, "SELECT count() FROM system.settings WHERE name = 'show_table_uuid_in_table_create_query_if_not_nil'"); err != nil {
		return nil, err
	}
	skipDatabases := make([]Database, 0)
	if err = ch.SelectContext(ctx, &skipDatabases, "SELECT name FROM system.databases WHERE engine IN ('MySQL','PostgreSQL')"); err != nil {
		return nil, err
	}
	allTablesSQL, err := ch.prepareAllTablesSQL(ctx, tablePattern, err, skipDatabases, isUUIDPresent)
	if err != nil {
		return nil, err
	}
	if err = ch.SelectContext(ctx, &tables, allTablesSQL); err != nil {
		return nil, err
	}
	for i, t := range tables {
		for _, filter := range ch.Config.SkipTables {
			if matched, _ := filepath.Match(strings.Trim(filter, " \t\r\n"), fmt.Sprintf("%s.%s", t.Database, t.Name)); matched {
				t.Skip = true
				break
			}
		}
		if ch.Config.UseEmbeddedBackupRestore && (strings.HasPrefix(t.Name, ".inner_id.") || strings.HasPrefix(t.Name, ".inner.")) {
			t.Skip = true
		}
		if t.Skip {
			tables[i] = t
			continue
		}
		tables[i] = ch.fixVariousVersions(t)
	}
	if len(tables) == 0 {
		return tables, nil
	}
	for i, table := range tables {
		if table.TotalBytes == 0 && !table.Skip && strings.HasSuffix(table.Engine, "Tree") {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				tables[i].TotalBytes = ch.getTableSizeFromParts(ctx, tables[i])
			}
		}
	}
	return tables, nil
}

func (ch *ClickHouse) prepareAllTablesSQL(ctx context.Context, tablePattern string, err error, skipDatabases []Database, isUUIDPresent uint64) (string, error) {
	isSystemTablesFieldPresent := make([]IsSystemTablesFieldPresent, 0)
	isFieldPresentSQL := `
		SELECT 
			countIf(name='data_path') is_data_path_present, 
			countIf(name='data_paths') is_data_paths_present, 
			countIf(name='uuid') is_uuid_present, 
			countIf(name='create_table_query') is_create_table_query_present, 
			countIf(name='total_bytes') is_total_bytes_present 
		FROM system.columns WHERE database='system' AND table='tables'
	`
	if err = ch.SelectContext(ctx, &isSystemTablesFieldPresent, isFieldPresentSQL); err != nil {
		return "", err
	}

	allTablesSQL := "SELECT database, name, engine "
	if len(isSystemTablesFieldPresent) > 0 && isSystemTablesFieldPresent[0].IsDataPathPresent > 0 {
		allTablesSQL += ", data_path "
	}
	if len(isSystemTablesFieldPresent) > 0 && isSystemTablesFieldPresent[0].IsDataPathsPresent > 0 {
		allTablesSQL += ", data_paths "
	}
	if len(isSystemTablesFieldPresent) > 0 && isSystemTablesFieldPresent[0].IsUUIDPresent > 0 {
		allTablesSQL += ", uuid "
	}
	if len(isSystemTablesFieldPresent) > 0 && isSystemTablesFieldPresent[0].IsCreateTableQueryPresent > 0 {
		allTablesSQL += ", create_table_query "
	}
	if len(isSystemTablesFieldPresent) > 0 && isSystemTablesFieldPresent[0].IsTotalBytesPresent > 0 {
		allTablesSQL += ", coalesce(total_bytes, 0) AS total_bytes "
	}

	allTablesSQL += "  FROM system.tables WHERE is_temporary = 0"
	if tablePattern != "" {
		replacer := strings.NewReplacer(".", "\\.", ",", "|", "*", ".*", "?", ".", " ", "", "'", "")
		allTablesSQL += fmt.Sprintf(" AND match(concat(database,'.',name),'%s') ", replacer.Replace(tablePattern))
	}
	if len(skipDatabases) > 0 {
		allTablesSQL += fmt.Sprintf(" AND database NOT IN ('%s')", strings.Join(ConvertToSlice(skipDatabases), "','"))
	}
	if isUUIDPresent > 0 {
		allTablesSQL += " SETTINGS show_table_uuid_in_table_create_query_if_not_nil=1"
	}
	return allTablesSQL, nil
}

// GetDatabases - return slice of all non system databases for backup
func (ch *ClickHouse) GetDatabases(ctx context.Context) ([]Database, error) {
	allDatabases := make([]Database, 0)
	allDatabasesSQL := "SELECT name, engine FROM system.databases WHERE name NOT IN" +
		" ('system', 'INFORMATION_SCHEMA', 'information_schema', '_temporary_and_external_tables')"
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		if err := ch.StructSelect(&allDatabases, allDatabasesSQL); err != nil {
			return nil, err
		}
	}
	for i, db := range allDatabases {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			showDatabaseSQL := fmt.Sprintf("SHOW CREATE DATABASE `%s`", db.Name)
			var result string
			// 19.4 doesn't have /var/lib/clickhouse/metadata/default.sql
			if err := ch.SelectSingleRow(ctx, &result, showDatabaseSQL); err != nil {
				ch.Log.Warnf("can't get create database query: %v", err)
				allDatabases[i].Query = fmt.Sprintf("CREATE DATABASE `%s` ENGINE = %s", db.Name, db.Engine)
			} else {
				allDatabases[i].Query = result
			}
		}
	}
	return allDatabases, nil
}

func (ch *ClickHouse) getTableSizeFromParts(ctx context.Context, table Table) uint64 {
	var tablesSize []struct {
		Size uint64 `ch:"size"`
	}
	query := fmt.Sprintf("SELECT sum(bytes_on_disk) as size FROM system.parts WHERE active AND database='%s' AND table='%s' GROUP BY database, table", table.Database, table.Name)
	if err := ch.SelectContext(ctx, &tablesSize, query); err != nil {
		ch.Log.Warnf("error parsing tablesSize: %v", err)
	}
	if len(tablesSize) > 0 {
		return tablesSize[0].Size
	}
	return 0
}

func (ch *ClickHouse) fixVariousVersions(t Table) Table {
	// versions before 19.15 contain data_path in a different column
	if t.DataPath != "" {
		t.DataPaths = []string{t.DataPath}
	}
	// version 20.6.3.28 has zero UUID
	if t.UUID == "00000000-0000-0000-0000-000000000000" {
		t.UUID = ""
	}
	// version 1.1.54390 no has query column
	if strings.TrimSpace(t.CreateTableQuery) == "" {
		t.CreateTableQuery = ch.ShowCreateTable(t.Database, t.Name)
	}
	// materialized views should properly restore via attach
	if t.Engine == "MaterializedView" {
		t.CreateTableQuery = strings.Replace(
			t.CreateTableQuery, "CREATE MATERIALIZED VIEW", "ATTACH MATERIALIZED VIEW", 1,
		)
	}
	return t
}

// GetVersion - returned ClickHouse version in number format
// Example value: 19001005
func (ch *ClickHouse) GetVersion(ctx context.Context) (int, error) {
	if ch.version != 0 {
		return ch.version, nil
	}
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
		var result string
		var err error
		query := "SELECT value FROM `system`.`build_options` where name='VERSION_INTEGER'"
		if err = ch.SelectSingleRow(ctx, &result, query); err != nil {
			return 0, fmt.Errorf("can't get ClickHouse version: %w", err)
		}
		ch.version, err = strconv.Atoi(result)
		return ch.version, err
	}
}

func (ch *ClickHouse) GetVersionDescribe(ctx context.Context) string {
	var result string
	query := "SELECT value FROM `system`.`build_options` where name='VERSION_DESCRIBE'"
	if err := ch.SelectSingleRow(ctx, &result, query); err != nil {
		return ""
	}
	return result
}

// FreezeTableOldWay - freeze all partitions in table one by one
// This way using for ClickHouse below v19.1
func (ch *ClickHouse) FreezeTableOldWay(ctx context.Context, table *Table, name string) error {
	var partitions []struct {
		PartitionID string `ch:"partition_id"`
	}
	q := fmt.Sprintf("SELECT DISTINCT partition_id FROM `system`.`parts` WHERE database='%s' AND table='%s' %s", table.Database, table.Name, ch.Config.FreezeByPartWhere)
	if err := ch.conn.Select(ctx, &partitions, q); err != nil {
		return fmt.Errorf("can't get partitions for '%s.%s': %w", table.Database, table.Name, err)
	}
	withNameQuery := ""
	if name != "" {
		withNameQuery = fmt.Sprintf("WITH NAME '%s'", name)
	}
	for _, item := range partitions {
		ch.Log.Debugf("  partition '%v'", item.PartitionID)
		query := fmt.Sprintf(
			"ALTER TABLE `%v`.`%v` FREEZE PARTITION ID '%v' %s;",
			table.Database,
			table.Name,
			item.PartitionID,
			withNameQuery,
		)
		if item.PartitionID == "all" {
			query = fmt.Sprintf(
				"ALTER TABLE `%v`.`%v` FREEZE PARTITION tuple() %s;",
				table.Database,
				table.Name,
				withNameQuery,
			)
		}
		if err := ch.QueryContext(ctx, query); err != nil {
			if (strings.Contains(err.Error(), "code: 60") || strings.Contains(err.Error(), "code: 81")) && ch.Config.IgnoreNotExistsErrorDuringFreeze {
				ch.Log.Warnf("can't freeze partition: %v", err)
			} else {
				return fmt.Errorf("can't freeze partition '%s': %w", item.PartitionID, err)
			}
		}
	}
	return nil
}

// FreezeTable - freeze all partitions for table
// This way available for ClickHouse since v19.1
func (ch *ClickHouse) FreezeTable(ctx context.Context, table *Table, name string) error {
	version, err := ch.GetVersion(ctx)
	if err != nil {
		return err
	}
	if strings.HasPrefix(table.Engine, "Replicated") && ch.Config.SyncReplicatedTables {
		query := fmt.Sprintf("SYSTEM SYNC REPLICA `%s`.`%s`;", table.Database, table.Name)
		if err := ch.QueryContext(ctx, query); err != nil {
			ch.Log.Warnf("can't sync replica: %v", err)
		} else {
			ch.Log.WithField("table", fmt.Sprintf("%s.%s", table.Database, table.Name)).Debugf("replica synced")
		}
	}
	if version < 19001005 || ch.Config.FreezeByPart {
		return ch.FreezeTableOldWay(ctx, table, name)
	}
	withNameQuery := ""
	if name != "" {
		withNameQuery = fmt.Sprintf("WITH NAME '%s'", name)
	}
	query := fmt.Sprintf("ALTER TABLE `%s`.`%s` FREEZE %s;", table.Database, table.Name, withNameQuery)
	if err := ch.Query(query); err != nil {
		if (strings.Contains(err.Error(), "code: 60") || strings.Contains(err.Error(), "code: 81")) && ch.Config.IgnoreNotExistsErrorDuringFreeze {
			ch.Log.Warnf("can't freeze table: %v", err)
			return nil
		}
		return fmt.Errorf("can't freeze table: %v", err)
	}
	return nil
}

// AttachPartitions - execute ATTACH command for specific table
func (ch *ClickHouse) AttachPartitions(table metadata.TableMetadata, disks []Disk) error {
	// https://github.com/AlexAkulov/clickhouse-backup/issues/474
	if ch.Config.CheckReplicasBeforeAttach && strings.Contains(table.Query, "Replicated") {
		var existsReplicas uint64
		if err := ch.SelectSingleRowNoCtx(
			&existsReplicas,
			fmt.Sprintf("SELECT sum(log_pointer + log_max_index + absolute_delay + queue_size) AS replication_in_progress FROM system.replicas WHERE database = '%s' and table = '%s' SETTINGS empty_result_for_aggregation_by_empty_set=0", table.Database, table.Table),
		); err != nil {
			return err
		}
		if existsReplicas > 0 {
			ch.Log.Warnf("%s.%s skipped cause system.replicas entry already exists and replication in progress from another replica", table.Database, table.Table)
			return nil
		} else {
			ch.Log.Infof("replication_in_progress status = %+v", existsReplicas)
		}
	}
	for _, disk := range disks {
		for _, partition := range table.Parts[disk.Name] {
			if !strings.HasSuffix(partition.Name, ".proj") {
				query := fmt.Sprintf("ALTER TABLE `%s`.`%s` ATTACH PART '%s'", table.Database, table.Table, partition.Name)
				if err := ch.Query(query); err != nil {
					return err
				}
				ch.Log.WithField("table", fmt.Sprintf("%s.%s", table.Database, table.Table)).WithField("disk", disk.Name).WithField("part", partition.Name).Debug("attached")
			}
		}
	}
	return nil
}

func (ch *ClickHouse) ShowCreateTable(database, name string) string {
	var result []struct {
		Statement string `ch:"statement"`
	}
	query := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`;", database, name)

	if err := ch.conn.Select(context.Background(), &result, query); err != nil {
		return ""
	}
	return result[0].Statement
}

// CreateDatabase - create ClickHouse database
func (ch *ClickHouse) CreateDatabase(database string, cluster string) error {
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", database)
	if cluster != "" {
		query += fmt.Sprintf(" ON CLUSTER '%s'", cluster)
	}
	return ch.Query(query)
}

func (ch *ClickHouse) CreateDatabaseWithEngine(database, engine, cluster string) error {
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` ENGINE=%s", database, engine)
	query = ch.addOnClusterToCreateDatabase(cluster, query)
	return ch.Query(query)
}

func (ch *ClickHouse) CreateDatabaseFromQuery(ctx context.Context, query, cluster string, args ...interface{}) error {
	if !strings.HasPrefix(query, "CREATE DATABASE IF NOT EXISTS") {
		query = strings.Replace(query, "CREATE DATABASE", "CREATE DATABASE IF NOT EXISTS", 1)
	}
	query = ch.addOnClusterToCreateDatabase(cluster, query)
	//_, err :=
	return ch.QueryContext(ctx, query, args)
}

func (ch *ClickHouse) addOnClusterToCreateDatabase(cluster string, query string) string {
	if cluster != "" && !strings.Contains(query, " ON CLUSTER ") {
		if !strings.Contains(query, "ENGINE") {
			query += fmt.Sprintf(" ON CLUSTER '%s'", cluster)
		} else {
			query = strings.Replace(query, "ENGINE", fmt.Sprintf(" ON CLUSTER '%s' ENGINE", cluster), 1)
		}
	}
	return query
}

// DropTable - drop ClickHouse table
func (ch *ClickHouse) DropTable(table Table, query string, onCluster string, ignoreDependencies bool, version int) error {
	var isAtomic bool
	var err error
	if isAtomic, err = ch.IsAtomic(table.Database); err != nil {
		return err
	}
	kind := "TABLE"
	if strings.HasPrefix(query, "CREATE DICTIONARY") {
		kind = "DICTIONARY"
	}
	dropQuery := fmt.Sprintf("DROP %s IF EXISTS `%s`.`%s`", kind, table.Database, table.Name)
	if version > 19000000 && onCluster != "" {
		dropQuery += " ON CLUSTER '" + onCluster + "' "
	}
	if isAtomic {
		dropQuery += " NO DELAY"
	}
	if ignoreDependencies {
		dropQuery += " SETTINGS check_table_dependencies=0"
	}
	if err = ch.Query(dropQuery); err != nil {
		return err
	}
	return nil
}

var createViewToClauseRe = regexp.MustCompile(`(?im)^(CREATE[\s\w]+VIEW[^(]+)(\s+TO\s+.+)`)
var createViewSelectRe = regexp.MustCompile(`(?im)^(CREATE[\s\w]+VIEW[^(]+)(\s+AS\s+SELECT.+)`)
var attachViewToClauseRe = regexp.MustCompile(`(?im)^(ATTACH[\s\w]+VIEW[^(]+)(\s+TO\s+.+)`)
var attachViewSelectRe = regexp.MustCompile(`(?im)^(ATTACH[\s\w]+VIEW[^(]+)(\s+AS\s+SELECT.+)`)
var createObjRe = regexp.MustCompile(`(?im)^(CREATE [^(]+)(\(.+)`)
var onClusterRe = regexp.MustCompile(`(?im)\S+ON\S+CLUSTER\S+`)

// CreateTable - create ClickHouse table
func (ch *ClickHouse) CreateTable(table Table, query string, dropTable, ignoreDependencies bool, onCluster string, version int) error {
	var err error
	if dropTable {
		if err = ch.DropTable(table, query, onCluster, ignoreDependencies, version); err != nil {
			return err
		}
	}

	if version > 19000000 && onCluster != "" && !onClusterRe.MatchString(query) {
		tryMatchReList := []*regexp.Regexp{attachViewToClauseRe, attachViewSelectRe, createViewToClauseRe, createViewSelectRe, createObjRe}
		for _, tryMatchRe := range tryMatchReList {
			if tryMatchRe.MatchString(query) {
				query = tryMatchRe.ReplaceAllString(query, "$1 ON CLUSTER '"+onCluster+"' $2")
				break
			}
		}
	}

	if !strings.Contains(query, table.Name) {
		return errors.New(fmt.Sprintf("schema query ```%s``` doesn't contains table name `%s`", query, table.Name))
	}

	// fix restore schema for legacy backup
	// see https://github.com/AlexAkulov/clickhouse-backup/issues/268
	// https://github.com/AlexAkulov/clickhouse-backup/issues/297
	// https://github.com/AlexAkulov/clickhouse-backup/issues/331
	isOnlyTableWithQuotesPresent, err := regexp.Match(fmt.Sprintf("^CREATE [^(\\.]+ `%s`", table.Name), []byte(query))
	if err != nil {
		return err
	}
	isOnlyTableWithQuotesPresent = isOnlyTableWithQuotesPresent && !strings.Contains(query, fmt.Sprintf("`%s`.`%s`", table.Database, table.Name))

	isOnlyTablePresent, err := regexp.Match(fmt.Sprintf("^CREATE [^(\\.]+ %s", table.Name), []byte(query))
	if err != nil {
		return err
	}
	isOnlyTablePresent = isOnlyTablePresent && !strings.Contains(query, fmt.Sprintf("%s.%s", table.Database, table.Name))
	if isOnlyTableWithQuotesPresent && table.Database != "" {
		query = strings.Replace(query, fmt.Sprintf("`%s`", table.Name), fmt.Sprintf("`%s`.`%s`", table.Database, table.Name), 1)
	} else if isOnlyTablePresent && table.Database != "" {
		query = strings.Replace(query, fmt.Sprintf("%s", table.Name), fmt.Sprintf("%s.%s", table.Database, table.Name), 1)
	}

	if err = ch.Query(query); err != nil {
		return err
	}
	return nil
}

// GetConn - return current connection
func (ch *ClickHouse) GetConn() driver.Conn {
	return ch.conn
}

func (ch *ClickHouse) IsClickhouseShadow(path string) bool {
	d, err := os.Open(path)
	if err != nil {
		return false
	}
	defer func() {
		if err := d.Close(); err != nil {
			ch.Log.Warnf("can't close directory %v", err)
		}
	}()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return false
	}
	for _, name := range names {
		if name == "increment.txt" {
			continue
		}
		if _, err := strconv.Atoi(name); err != nil {
			return false
		}
	}
	return true
}

func (ch *ClickHouse) StructSelect(dest interface{}, query string, args ...interface{}) error {
	return ch.SelectContext(context.Background(), dest, ch.LogQuery(query, args...), args)
}

func (ch *ClickHouse) QueryContext(ctx context.Context, query string, args ...interface{}) error {
	return ch.conn.Exec(ctx, ch.LogQuery(query, args...), args...)
}

func (ch *ClickHouse) Query(query string, args ...interface{}) error {
	return ch.conn.Exec(context.Background(), ch.LogQuery(query, args...), args...)
}

func (ch *ClickHouse) QueryxContext(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
	return ch.conn.Query(ctx, ch.LogQuery(query, args...), args...)
}

func (ch *ClickHouse) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return ch.conn.Select(ctx, dest, ch.LogQuery(query, args...), args...)
	}
}

func (ch *ClickHouse) Select(dest interface{}, query string, args ...interface{}) error {
	return ch.conn.Select(context.Background(), dest, ch.LogQuery(query, args...), args...)
}

func (ch *ClickHouse) SelectSingleRow(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	return ch.conn.QueryRow(ctx, ch.LogQuery(query, args...), args).Scan(dest)
}

func (ch *ClickHouse) SelectSingleRowNoCtx(dest interface{}, query string, args ...interface{}) error {
	err := ch.conn.QueryRow(context.Background(), ch.LogQuery(query, args...), args).Scan(dest)
	if err != nil && err == sql.ErrNoRows {
		return nil
	}
	return err
}

func (ch *ClickHouse) LogQuery(query string, args ...interface{}) string {
	var logF func(msg string)
	if !ch.Config.LogSQLQueries {
		logF = ch.Log.Debug
	} else {
		logF = ch.Log.Info
	}
	if len(args) > 0 {
		logF(strings.NewReplacer("\n", " ", "\r", " ", "\t", " ").Replace(fmt.Sprintf("%s with args %v", query, args)))
	} else {
		logF(strings.NewReplacer("\n", " ", "\r", " ", "\t", " ").Replace(query))
	}
	return query
}

func (ch *ClickHouse) IsAtomic(database string) (bool, error) {
	var isDatabaseAtomic string
	if err := ch.SelectSingleRowNoCtx(&isDatabaseAtomic, fmt.Sprintf("SELECT engine FROM system.databases WHERE name = '%s'", database)); err != nil {
		return false, err
	}
	return isDatabaseAtomic == "Atomic", nil
}

// GetAccessManagementPath @todo think about how to properly extract access_management_path from /etc/clickhouse-server/
func (ch *ClickHouse) GetAccessManagementPath(ctx context.Context, disks []Disk) (string, error) {
	accessPath := "/var/lib/clickhouse/access"
	var rows []string
	if err := ch.SelectContext(ctx, &rows, "SELECT JSONExtractString(params,'path') AS access_path FROM system.user_directories WHERE type='local directory'"); err != nil || len(rows) == 0 {
		if disks == nil {
			disks, err = ch.GetDisks(ctx)
			if err != nil {
				return "", err
			}
		}
		for _, disk := range disks {
			if _, err := os.Stat(path.Join(disk.Path, "access")); !os.IsNotExist(err) {
				accessPath = path.Join(disk.Path, "access")
				break
			}
		}
	} else {
		accessPath = rows[0]
	}
	return accessPath, nil
}

func (ch *ClickHouse) GetUserDefinedFunctions(ctx context.Context) ([]Function, error) {
	allFunctions := make([]Function, 0)
	allFunctionsSQL := "SELECT name, create_query FROM system.functions WHERE create_query!=''"
	var detectUDF uint64
	detectUDFSQL := "SELECT count() as cnt FROM system.columns WHERE database='system' AND table='functions' AND name='create_query'"
	if err := ch.SelectSingleRow(ctx, &detectUDF, detectUDFSQL); err != nil {
		return nil, err
	}
	if detectUDF == 0 {
		return allFunctions, nil
	}

	if err := ch.SelectContext(ctx, &allFunctions, allFunctionsSQL); err != nil {
		return nil, err
	}
	return allFunctions, nil
}

func (ch *ClickHouse) CreateUserDefinedFunction(name string, query string, cluster string) error {
	dropQuery := fmt.Sprintf("DROP FUNCTION IF EXISTS `%s`", name)
	if cluster != "" {
		dropQuery += fmt.Sprintf(" ON CLUSTER '%s'", cluster)
		query = strings.Replace(query, " AS ", fmt.Sprintf(" ON CLUSTER '%s' AS ", cluster), 1)
	}
	err := ch.Query(dropQuery)
	if err != nil {
		return err
	}
	return ch.Query(query)
}

func (ch *ClickHouse) CalculateMaxFileSize(ctx context.Context, cfg *config.Config) (int64, error) {
	var rows int64
	maxSizeQuery := "SELECT max(toInt64(bytes_on_disk * 1.02)) AS max_file_size FROM system.parts"
	if !cfg.General.UploadByPart {
		maxSizeQuery = "SELECT toInt64(max(data_by_disk) * 1.02) AS max_file_size FROM (SELECT disk_name, max(toInt64(bytes_on_disk)) data_by_disk FROM system.parts GROUP BY disk_name)"
	}
	if err := ch.SelectSingleRow(ctx, &rows, maxSizeQuery); err != nil {
		return 0, fmt.Errorf("can't calculate max(bytes_on_disk): %v", err)
	}
	return rows, nil
}

func (ch *ClickHouse) ApplyMacros(ctx context.Context, s string) (string, error) {
	var macrosExists uint64
	err := ch.SelectSingleRow(ctx, &macrosExists, "SELECT count() AS is_macros_exists FROM system.tables WHERE database='system' AND name='macros'")
	if err != nil || macrosExists == 0 {
		return s, err
	}

	macros := make([]Macro, 0)
	err = ch.SelectContext(ctx, &macros, "SELECT macro, substitution FROM system.macros")
	if err != nil || len(macros) == 0 {
		return s, err
	}

	replaces := make([]string, len(macros)*2)
	for i, macro := range macros {
		replaces[i*2] = fmt.Sprintf("{%s}", macro.Macro)
		replaces[i*2+1] = fmt.Sprintf("%s", macro.Substitution)
	}
	s = strings.NewReplacer(replaces...).Replace(s)
	return s, nil
}
