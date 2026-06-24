package pg

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
)

type Store struct {
	DB *sql.DB
}

func Connect(host string, port int, user, password, dbname string) (*Store, error) {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable connect_timeout=5",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return &Store{DB: db}, nil
}

// Worker registration
func (s *Store) Heartbeat(workerID string, currentJob string) {
	s.DB.Exec(`INSERT INTO heartbeat (worker_id, last_seen, current_job) 
		VALUES ($1, NOW(), $2) 
		ON CONFLICT (worker_id) DO UPDATE SET last_seen=NOW(), current_job=$2`, workerID, currentJob)
}

func (s *Store) SetActivity(workerID, activity string) {
	s.DB.Exec(`UPDATE heartbeat SET activity=$2 WHERE worker_id=$1`, workerID, activity)
}

func (s *Store) RegisterWorker(id, name string, cpu, mem, maxConc, masscanRate int) {
	s.DB.Exec(`INSERT INTO workers (id, name, cpu_count, memory_mb, max_concurrent, masscan_rate, last_seen, status)
		VALUES ($1, $2, $3, $4, $5, $6, NOW(), 'online')
		ON CONFLICT (id) DO UPDATE SET name=$2, cpu_count=$3, memory_mb=$4, max_concurrent=$5, masscan_rate=$6, last_seen=NOW(), status='online'`,
		id, name, cpu, mem, maxConc, masscanRate)
}

func (s *Store) DeregisterWorker(id string) {
	s.DB.Exec(`UPDATE workers SET status='offline' WHERE id=$1`, id)
}

// Phase 1: insert masscan results
func (s *Store) InsertMasscanResults(jobID string, ips []string, port int) error {
	if len(ips) == 0 {
		return nil
	}
	batchSize := 500
	for i := 0; i < len(ips); i += batchSize {
		end := i + batchSize
		if end > len(ips) {
			end = len(ips)
		}
		batch := ips[i:end]

		values := make([]string, len(batch))
		args := make([]interface{}, 0, len(batch)*3)
		for j, ip := range batch {
			values[j] = fmt.Sprintf("($%d,$%d,$%d)", j*3+1, j*3+2, j*3+3)
			args = append(args, jobID, ip, port)
		}
		query := fmt.Sprintf("INSERT INTO masscan_results (job_id, ip, port) VALUES %s ON CONFLICT DO NOTHING",
			strings.Join(values, ","))

		// Retry on deadlock (40P01)
		var lastErr error
		for attempt := 0; attempt < 3; attempt++ {
			if attempt > 0 {
				time.Sleep(time.Duration(50<<attempt) * time.Millisecond)
			}
			_, err := s.DB.Exec(query, args...)
			if err == nil {
				lastErr = nil
				break
			}
			if strings.Contains(err.Error(), "deadlock") || strings.Contains(err.Error(), "40P01") {
				lastErr = err
				continue
			}
			return err
		}
		if lastErr != nil {
			return lastErr
		}
	}
	return nil
}

// InsertMasscanResultsBulk uses pq.CopyIn for fast bulk insert (~100K rows/sec).
func (s *Store) InsertMasscanResultsBulk(jobID string, ips []string, port int) error {
	tx, err := s.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(pq.CopyIn("masscan_results", "job_id", "ip", "port"))
	if err != nil {
		return err
	}

	for _, ip := range ips {
		if _, err = stmt.Exec(jobID, ip, port); err != nil {
			stmt.Close()
			return err
		}
	}

	if _, err = stmt.Exec(); err != nil {
		stmt.Close()
		return err
	}
	if err = stmt.Close(); err != nil {
		return err
	}
	return tx.Commit()
}

// MarkWorkDone marks a work_queue item as done without inserting a result (for non-CF / errors).
func (s *Store) MarkWorkDone(jobID, ip string, port int, status string) {
	s.DB.Exec(`UPDATE work_queue SET status='done' WHERE job_id=$1 AND ip=$2 AND port=$3`, jobID, ip, port)
}

// MarkWorkScannedBatch marks multiple items as scanned in a single transaction.
func (s *Store) MarkWorkScannedBatch(jobID string, items []ScannedItem) {
	if len(items) == 0 {
		return
	}
	tx, err := s.DB.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()
	stmt, _ := tx.Prepare(`UPDATE work_queue SET status='done' WHERE job_id=$1 AND ip=$2 AND port=$3`)
	if stmt == nil {
		return
	}
	defer stmt.Close()
	for _, item := range items {
		stmt.Exec(jobID, item.IP, item.Port)
	}
	tx.Commit()
}

type ScannedItem struct {
	IP   string
	Port int
}

func (s *Store) MarkPhase1Done(jobID string) {
	s.DB.Exec(`UPDATE jobs SET done_workers = done_workers + 1 WHERE id = $1`, jobID)
}

// Phase 2: fetch pending items from work queue
func (s *Store) FetchWorkBatch(jobID, workerID string, limit int) ([]WorkItem, error) {
	rows, err := s.DB.Query(`
		UPDATE work_queue SET status='processing', worker=$3
		WHERE id IN (
			SELECT id FROM work_queue
			WHERE job_id=$1 AND status='pending'
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, ip, port
	`, jobID, limit, workerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []WorkItem
	for rows.Next() {
		var item WorkItem
		if err := rows.Scan(&item.ID, &item.IP, &item.Port); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

type WorkItem struct {
	ID   int64
	IP   string
	Port int
}

func (s *Store) InsertResult(jobID string, ip string, port int, status, colo, country, asn string, delay int, city, org string) {
	s.DB.Exec(`INSERT INTO results (job_id, ip, port, status, colo, country, asn, delay, city, org)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT (job_id,ip,port) DO UPDATE SET
		status=$4, colo=$5, country=$6, asn=$7, delay=$8, city=$9, org=$10`,
		jobID, ip, port, status, colo, country, asn, delay, city, org)

	s.DB.Exec(`UPDATE work_queue SET status='done' WHERE job_id=$1 AND ip=$2 AND port=$3`, jobID, ip, port)
	s.DB.Exec(`SELECT pg_notify('progress', $1)`, fmt.Sprintf(`{"job":"%s","done":1}`, jobID))
}

func (s *Store) CreateJob(id, name, mode, asns, ports string, totalWorkers int) error {
	_, err := s.DB.Exec(`INSERT INTO jobs (id, name, mode, asns, ports, total_workers)
		VALUES ($1, $2, $3, $4, $5, $6)`, id, name, mode, asns, ports, totalWorkers)
	return err
}

func (s *Store) UpdateJobPhase(id, phase string, totalItems int) {
	s.DB.Exec(`UPDATE jobs SET phase=$2, total_items=$3 WHERE id=$1`, id, phase, totalItems)
	if phase == "done" {
		s.DB.Exec(`UPDATE jobs SET status='done' WHERE id=$1`, id)
	}
}

func (s *Store) CancelJob(id string) {
	s.DB.Exec(`UPDATE jobs SET status='cancelled' WHERE id=$1`, id)
	s.DB.Exec(`UPDATE work_queue SET status='pending', worker=NULL WHERE job_id=$1 AND status='processing'`, id)
}

func (s *Store) DeleteJob(id string) {
	s.DB.Exec(`DELETE FROM results WHERE job_id=$1`, id)
	s.DB.Exec(`DELETE FROM work_queue WHERE job_id=$1`, id)
	s.DB.Exec(`DELETE FROM masscan_results WHERE job_id=$1`, id)
	s.DB.Exec(`DELETE FROM jobs WHERE id=$1`, id)
}

func (s *Store) IsJobCancelled(id string) bool {
	var status string
	s.DB.QueryRow(`SELECT status FROM jobs WHERE id=$1`, id).Scan(&status)
	return status == "cancelled"
}

// Populate work_queue from masscan_results (Master triggers this for Phase 2)
func (s *Store) PopulateWorkQueue(jobID string) error {
	_, err := s.DB.Exec(`
		INSERT INTO work_queue (job_id, ip, port)
		SELECT job_id, ip, port FROM masscan_results
		WHERE job_id = $1
		ON CONFLICT DO NOTHING
	`, jobID)
	return err
}

func (s *Store) GetJobStats(jobID string) (done, total int, err error) {
	var phase string
	s.DB.QueryRow(`SELECT phase FROM jobs WHERE id=$1`, jobID).Scan(&phase)

	switch phase {
	case "phase1":
		// masscan still running — count from masscan_results (streaming writes)
		s.DB.QueryRow(`SELECT COUNT(*) FROM masscan_results WHERE job_id=$1`, jobID).Scan(&done)
		total = 0 // unknown total during masscan
	case "phase2b":
		err = s.DB.QueryRow(`SELECT COUNT(*) FILTER (WHERE status='verified'), COUNT(*) FROM results WHERE job_id=$1`, jobID).Scan(&done, &total)
	default:
		err = s.DB.QueryRow(`SELECT COUNT(*) FILTER (WHERE status != 'pending'), COUNT(*) FROM work_queue WHERE job_id=$1`, jobID).Scan(&done, &total)
	}
	return
}

func (s *Store) GetResults(jobID string) (*sql.Rows, error) {
	return s.DB.Query(`SELECT ip, port, colo, country, asn, delay FROM results WHERE job_id=$1 AND colo != '' ORDER BY delay`, jobID)
}
