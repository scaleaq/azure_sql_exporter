package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	yaml "gopkg.in/yaml.v2"

	_ "github.com/microsoft/go-mssqldb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// Version of azure_sql_exporter. Set at build time.
	Version = "0.0.0.dev"

	logLevel      = flag.String("log.level", "info", "Log level: debug, info, warn, error")
	listenAddress = flag.String("web.listen-address", ":9139", "Address to listen on for web interface and telemetry.")
	metricsPath   = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
	configFile    = flag.String("config.file", "./config.yaml", "Specify the config file with the database credentials.")
)

const namespace = "azure_sql"

// Exporter implements prometheus.Collector.
type Exporter struct {
	sourceDB       Database
	discoveredDBs  []Database
	mutex          sync.RWMutex
	up             prometheus.Gauge
	cpuPercent     *prometheus.GaugeVec
	dataIO         *prometheus.GaugeVec
	logIO          *prometheus.GaugeVec
	memoryPercent  *prometheus.GaugeVec
	instanceCpu    *prometheus.GaugeVec
	instanceMemory *prometheus.GaugeVec
	storageUsed    *prometheus.GaugeVec
	storageAlloc   *prometheus.GaugeVec
	workPercent    *prometheus.GaugeVec
	sessionPercent *prometheus.GaugeVec
	dbUp           *prometheus.GaugeVec
}

// NewExporter returns an initialized MS SQL Exporter.
func NewExporter(database Database) *Exporter {
	e := &Exporter{
		sourceDB:       database,
		up:             newGuage("up", "Was the last scrape of Azure SQL successful."),
		cpuPercent:     newGuageVec("cpu_percent", "Average compute utilization in percentage of the limit of the service tier."),
		dataIO:         newGuageVec("data_io", "Average I/O utilization in percentage based on the limit of the service tier."),
		logIO:          newGuageVec("log_io", "Average write resource utilization in percentage of the limit of the service tier."),
		memoryPercent:  newGuageVec("memory_percent", "Average Memory Usage In Percent"),
		instanceCpu:    newGuageVec("instance_cpu_percent", "Average CPU Percent for the entire instance."),
		instanceMemory: newGuageVec("instance_memory_percent", "Average Memory Percent for the entire instance."),
		storageUsed:    newGuageVec("storage_used_mb", "Storage used in MB."),
		storageAlloc:   newGuageVec("storage_allocated_mb", "Storage allocated in MB."),
		workPercent:    newGuageVec("worker_percent", "Maximum concurrent workers (requests) in percentage based on the limit of the database’s service tier."),
		sessionPercent: newGuageVec("session_percent", "Maximum concurrent sessions in percentage based on the limit of the database’s service tier."),
		dbUp:           newGuageVec("db_up", "Is the database is accessible."),
	}
	go e.runDiscovery(time.Hour)
	return e
}

// Describe describes all the metrics exported by the MS SQL exporter.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	e.cpuPercent.Describe(ch)
	e.dataIO.Describe(ch)
	e.logIO.Describe(ch)
	e.memoryPercent.Describe(ch)
	e.instanceCpu.Describe(ch)
	e.instanceMemory.Describe(ch)
	e.storageUsed.Describe(ch)
	e.storageAlloc.Describe(ch)
	e.workPercent.Describe(ch)
	e.sessionPercent.Describe(ch)
	e.dbUp.Describe(ch)
	e.up.Describe(ch)
}

// Collect fetches the stats from MS SQL and delivers them as Prometheus metrics. It implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	e.mutex.RLock()
	dbs := make([]Database, len(e.discoveredDBs))
	copy(dbs, e.discoveredDBs)
	e.mutex.RUnlock()

	if len(dbs) == 0 {
		slog.Warn("No databases discovered yet, skipping scrape.")
		e.up.Set(0)
		ch <- e.up
		return
	}

	var wg sync.WaitGroup
	for _, db := range dbs {
		wg.Add(1)
		go func(d Database) {
			slog.Debug("Scraping", "connection_string", db.String())
			defer wg.Done()
			e.scrapeDatabase(d)
		}(db)
	}
	wg.Wait()

	e.mutex.Lock()
	defer e.mutex.Unlock()
	e.cpuPercent.Collect(ch)
	e.dataIO.Collect(ch)
	e.logIO.Collect(ch)
	e.memoryPercent.Collect(ch)
	e.instanceCpu.Collect(ch)
	e.instanceMemory.Collect(ch)
	e.storageUsed.Collect(ch)
	e.storageAlloc.Collect(ch)
	e.workPercent.Collect(ch)
	e.sessionPercent.Collect(ch)
	e.dbUp.Collect(ch)
	e.up.Set(1)
	ch <- e.up
}

func (e *Exporter) runDiscovery(interval time.Duration) {
	discover := func() {
		slog.Info("Running database discovery.")
		dbs, err := e.discoverDatabases()
		if err != nil {
			slog.Error("Failed to discover databases.", "err", err)
			return
		}

		e.mutex.Lock()
		e.discoveredDBs = dbs
		e.mutex.Unlock()
		slog.Info("Discovery complete.", "db_count", len(dbs)/2)
	}

	// Initial discovery
	discover()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		discover()
	}
}

// Connect to discovery db and look up all databases
func (e *Exporter) discoverDatabases() ([]Database, error) {
	conn, err := sql.Open("mssql", e.sourceDB.DSN())
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to discovery database %s: %w", e.sourceDB, err)
	}
	defer conn.Close()

	// Find all databases including elastic pool and edition information
	query := "SELECT d.name, dso.elastic_pool_name, dso.edition FROM sys.databases d INNER JOIN sys.database_service_objectives dso ON d.database_id = dso.database_id WHERE d.Name <> 'master' ORDER BY d.name;"
	rows, err := conn.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to read databases from %s: %w", e.sourceDB, err)
	}
	defer rows.Close()

	var discoveredDBs []Database
	for rows.Next() {
		var dbName, dbPool, dbEdition string
		err = rows.Scan(&dbName, &dbPool, &dbEdition)
		if err != nil {
			slog.Error("Failed to scan database.", "err", err)
			continue
		}

		// Create ReadOnly and ReadWrite variants for each discovered database
		readonlyVariant := Database{
			Name:     dbName,
			Pool:     dbPool,
			Edition:  dbEdition,
			Server:   e.sourceDB.Server,
			User:     e.sourceDB.User,
			Password: e.sourceDB.Password,
			Port:     e.sourceDB.Port,
			Intent:   "ReadOnly",
		}
		readwriteVariant := readonlyVariant
		readwriteVariant.Intent = "ReadWrite"

		discoveredDBs = append(discoveredDBs, readonlyVariant, readwriteVariant)
	}
	return discoveredDBs, nil
}

func (e *Exporter) scrapeDatabase(d Database) {
	conn, err := sql.Open("mssql", d.DSN())
	if err != nil {
		e.mutex.Lock()
		defer e.mutex.Unlock()
		slog.Error("Failed to access database %s: %s", d, err)
		e.dbUp.WithLabelValues(d.Server, d.Name, d.Intent, d.Pool, d.Edition).Set(0)
		return
	}
	defer conn.Close()
	query := "SELECT TOP 1 avg_cpu_percent, avg_data_io_percent, avg_log_write_percent, avg_memory_usage_percent, avg_instance_cpu_percent, avg_instance_memory_percent, used_storage_mb, allocated_storage_mb, max_session_percent, max_worker_percent FROM sys.dm_db_resource_stats ORDER BY end_time DESC"
	var cpu, data, logio, memory, instanceCpu, instanceMemory, storageUsed, storageAllocated, session, worker float64
	err = conn.QueryRow(query).Scan(&cpu, &data, &logio, &memory, &instanceCpu, &instanceMemory, &storageUsed, &storageAllocated, &session, &worker)
	if err != nil {
		e.mutex.Lock()
		defer e.mutex.Unlock()
		slog.Error("Failed to query database %s: %s", d, err)
		e.dbUp.WithLabelValues(d.Server, d.Name, d.Intent, d.Pool, d.Edition).Set(0)
		return
	}
	queryupdability := "SELECT DATABASEPROPERTYEX(DB_NAME(), 'Updateability') AS Updateability;"
	var updateability string
	err = conn.QueryRow(queryupdability).Scan(&updateability)
	if err != nil {
		e.mutex.Lock()
		defer e.mutex.Unlock()
		slog.Error("Failed to query database %s: %s", d, err)
		e.dbUp.WithLabelValues(d.Server, d.Name, d.Intent, d.Pool, d.Edition).Set(0)
		return
	}
	if d.Intent == "ReadOnly" && updateability != "READ_ONLY" {
		e.mutex.Lock()
		defer e.mutex.Unlock()
		slog.Info("Database is not accessible read-only as expected, skipping metrics collection.", "db_name", d.Name)
		e.dbUp.WithLabelValues(d.Server, d.Name, d.Intent, d.Pool, d.Edition).Set(0)
		return
	}
	slog.Debug("Database updateability info.", "db_name", d.Name, "updatability", updateability)
	e.mutex.Lock()
	defer e.mutex.Unlock()
	e.cpuPercent.WithLabelValues(d.Server, d.Name, d.Intent, d.Pool, d.Edition).Set(cpu)
	e.dataIO.WithLabelValues(d.Server, d.Name, d.Intent, d.Pool, d.Edition).Set(data)
	e.logIO.WithLabelValues(d.Server, d.Name, d.Intent, d.Pool, d.Edition).Set(logio)
	e.memoryPercent.WithLabelValues(d.Server, d.Name, d.Intent, d.Pool, d.Edition).Set(memory)
	e.instanceCpu.WithLabelValues(d.Server, d.Name, d.Intent, d.Pool, d.Edition).Set(instanceCpu)
	e.instanceMemory.WithLabelValues(d.Server, d.Name, d.Intent, d.Pool, d.Edition).Set(instanceMemory)
	e.storageUsed.WithLabelValues(d.Server, d.Name, d.Intent, d.Pool, d.Edition).Set(storageUsed)
	e.storageAlloc.WithLabelValues(d.Server, d.Name, d.Intent, d.Pool, d.Edition).Set(storageAllocated)
	e.workPercent.WithLabelValues(d.Server, d.Name, d.Intent, d.Pool, d.Edition).Set(worker)
	e.sessionPercent.WithLabelValues(d.Server, d.Name, d.Intent, d.Pool, d.Edition).Set(session)
	e.dbUp.WithLabelValues(d.Server, d.Name, d.Intent, d.Pool, d.Edition).Set(1)
}

// Database represents a MS SQL database connection.
type Database struct {
	Name     string
	Server   string
	Pool     string
	Edition  string
	User     string
	Password string
	Intent   string
	Port     uint
}

// DSN returns the data source name as a string for the DB connection.
func (d Database) DSN() string {
	return fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;ApplicationIntent=%s", d.Server, d.User, d.Password, d.Port, d.Name, d.Intent)
}

// DSN returns the data source name as a string for the DB connection with the password hidden for safe log output.
func (d Database) String() string {
	return fmt.Sprintf("server=%s;user id=%s;password=******;port=%d;database=%s;ApplicationIntent=%s", d.Server, d.User, d.Port, d.Name, d.Intent)
}

// Config contains all the required information for connecting to the databases.
type Config struct {
	DBServer Database `yaml:"dbserver"`
}

// NewConfig creates an instance of Config from a local YAML file.
func NewConfig(path string) (Config, error) {
	fh, err := ioutil.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("unable to read file %s: %s", path, err)
	}
	var config Config
	err = yaml.Unmarshal(fh, &config)
	if err != nil {
		return Config{}, fmt.Errorf("unable to unmarshal file %s: %s", path, err)
	}
	// Set default port if not specified
	if config.DBServer.Port == 0 {
		config.DBServer.Port = 1433
	}
	return config, nil
}

func newGuageVec(metricsName, docString string) *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      metricsName,
			Help:      docString,
		},
		[]string{"server", "database", "intent", "elastic_pool", "edition"},
	)
}

func newGuage(metricsName, docString string) prometheus.Gauge {
	return prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      metricsName,
			Help:      docString,
		},
	)
}

func main() {
	flag.Parse()

	var theLogLevel slog.Level
	switch *logLevel {
	case "debug":
		theLogLevel = slog.LevelDebug
	case "info":
		theLogLevel = slog.LevelInfo
	case "warn":
		theLogLevel = slog.LevelWarn
	case "error":
		theLogLevel = slog.LevelError
	default:
		theLogLevel = slog.LevelInfo
		slog.Warn("Unknown log level specified, defaulting to info", "level", *logLevel)
	}
	logHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: theLogLevel,
	})
	logger := slog.New(logHandler)
	slog.SetDefault(logger)
	config, err := NewConfig(*configFile)
	if err != nil {
		slog.Error("Cannot open config file", "path", *configFile, "error", err)
		os.Exit(1)
	}
	exporter := NewExporter(config.DBServer)
	prometheus.MustRegister(exporter)
	http.Handle(*metricsPath, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
                <head><title>Azure SQL Exporter</title></head>
                <body>
                   <h1>Azure SQL Exporter</h1>
                   <p><a href='` + *metricsPath + `'>Metrics</a></p>
                   </body>
                </html>
              `))
	})
	slog.Info("Starting Server", "version", Version, "addr", *listenAddress)
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}
