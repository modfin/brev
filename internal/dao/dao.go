package dao

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/modfin/brev"
	"github.com/modfin/brev/internal/signals"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

type DAO interface {
	EnsureApiKey(string) error
	UpsertApiKey(ApiKey) (ApiKey, error)
	GetApiKey(key string) (ApiKey, error)

	EnqueueEmails(emails []SpoolEmail, content []byte) ([]int64, error)
	DequeueEmails(count int) (emails []SpoolEmail, err error)
	RequeueEmail(emails SpoolEmail) error

	SetEmailStatus(transactionId int64, statusBrev string) error
	GetEmail(transactionId int64) (SpoolEmail, error)
	GetEmailContent(messageId string) ([]byte, error)

	AddSpecificLogEntry(messageId string, transactionId int64, log string) error
	AddLogEntry(messageId string, log string) error

	EnqueuePosthook(email SpoolEmail, event brev.PosthookEvent, message string) error
	DequeuePosthook(count int) ([]Posthook, error)
}

func NewSQLite(path string) (DAO, error) {
	lite := &sqlite{path: path}
	err := lite.ensureSchema()
	return lite, err
}

type sqlite struct {
	db   *sqlx.DB
	path string
}

func (s *sqlite) DequeuePosthook(count int) (hooks []Posthook, err error) {
	q := `
	DELETE FROM posthook_queue
	WHERE posthook_id IN (SELECT posthook_id FROM posthook_queue
						  ORDER BY posthook_id ASC
						  LIMIT 1)
	RETURNING *
;`
	var tx *sqlx.Tx
	tx, err = s.getTX()
	if err != nil {
		err = fmt.Errorf("failed to get transaction, err %v", err)
		return nil, err
	}

	defer func() {
		if err == nil {
			err = tx.Commit()
			return
		}
		fmt.Printf("[DAO]: failed to get transactions, rolling back,%v\n", err)
		_ = tx.Rollback()
	}()

	err = tx.Select(&hooks, q, count)
	return hooks, err
}

func (s *sqlite) EnqueuePosthook(email SpoolEmail, event brev.PosthookEvent, message string) error {

	key, err := s.GetApiKey(email.ApiKey)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(key.PosthookURL, "http") {
		return nil
	}

	payload := brev.Posthook{
		MessageId:     email.MessageId,
		TransactionId: email.TransactionId,
		CreatedAt:     time.Now().In(time.UTC),
		Emails:        email.Recipients,
		Event:         event,
		Info:          message,
	}

	content, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	q := `INSERT INTO posthook_queue (api_key, posthook_url, content) 
		  VALUES (?, ?, ?)`
	db, err := s.getDB()
	if err != nil {
		return err
	}

	_, err = db.Exec(q, key.Key, key.PosthookURL, string(content))
	if err == nil {
		signals.Broadcast(signals.NewPosthook)
	}
	return err
}

func (s *sqlite) EnqueueEmails(transfers []SpoolEmail, content []byte) (transactionIds []int64, err error) {

	if len(transfers) == 0 {
		return nil, errors.New("transfer list must be greater than 0")
	}

	var tx *sqlx.Tx
	tx, err = s.getTX()
	if err != nil {
		err = fmt.Errorf("failed to get transaction, err %v", err)
		return
	}

	defer func() {
		if err == nil {
			err = tx.Commit()
			return
		}
		fmt.Printf("[DAO]: failed adding to spool, rolling back,%v\n", err)
		_ = tx.Rollback()
	}()

	messageId := transfers[0].MessageId

	q0 := `INSERT INTO spool_content(message_id, content) VALUES (?, ?)`

	_, err = tx.Exec(q0, messageId, string(content))
	if err != nil {
		err = fmt.Errorf("faild to insert into spool_content, %v", err)
		return
	}
	err = s.AddSpoolLogEntryTx(tx, messageId, "email content has been added into spool_content")
	if err != nil {
		return nil, err
	}

	q1 := `
	INSERT INTO spool(message_id, api_key, from_, recipients, mx_servers, status, send_at)
	VALUES (:message_id, :api_key, :from_, :recipients, :mx_servers, :status, :send_at) 
	RETURNING transaction_id
	`
	stmt, err := tx.PrepareNamed(q1)
	if err != nil {
		err = fmt.Errorf("failed to prepare statement, err %v", err)
		return
	}
	defer stmt.Close()
	for _, transfer := range transfers {

		var transactionId int64
		err = stmt.Get(&transactionId, map[string]interface{}{
			"message_id": transfer.MessageId,
			"api_key":    transfer.ApiKey,
			"from_":      transfer.From,
			"recipients": strings.Join(transfer.Recipients, " "),
			"mx_servers": strings.Join(transfer.MXServers, " "),
			"status":     BrevStatusQueued,
			"send_at":    time.Now().In(time.UTC),
		})
		if err != nil {
			err = fmt.Errorf("failed to insert into spool table, err %v", err)
			return
		}

		// Since we need to know what specific email that bounced, we need to keep trac of transaction id in bounce email.
		transfer.From = strings.Replace(transfer.From, "@", "="+strconv.FormatInt(transactionId, 10)+"@", 1)
		_, err = tx.Exec(`UPDATE spool SET from_ = ? WHERE transaction_id = ?`, transfer.From, transactionId)
		if err != nil {
			err = fmt.Errorf("failed to update from after initial insert, err %v", err)
			return
		}

		transactionIds = append(transactionIds, transactionId)
		err = s.AddSpoolSpecificLogEntryTx(tx, messageId, transactionId, "transfer list has been added to spool")
		if err != nil {
			err = fmt.Errorf("failed to insert log entry, err %v", err)
			return
		}
	}

	return
}

func (s *sqlite) DequeueEmails(limit int) (emails []SpoolEmail, err error) {
	q1 := `
		WITH mails AS (
		    SELECT transaction_id
			FROM spool
			WHERE send_count < 3
			  AND send_at <= ?
			  AND status = ?
			ORDER BY send_at
			LIMIT ?
		)
		UPDATE spool
		SET status = ?,
		    send_count = send_count + 1,
		    updated_at = (strftime('%Y-%m-%d %H:%M:%f', 'now'))
		WHERE transaction_id IN (SELECT m.transaction_id FROM mails m )
		RETURNING *
	`
	var tx *sqlx.Tx
	tx, err = s.getTX()
	if err != nil {
		return
	}

	defer func() {
		if err == nil {
			err = tx.Commit()
			return
		}
		_ = tx.Rollback()
	}()

	var intr []struct {
		SpoolEmail
		Recipients string `db:"recipients"`
		MXServers  string `db:"mx_servers"`
	}

	err = tx.Select(&intr, q1, time.Now().In(time.UTC), BrevStatusQueued, limit, BrevStatusProcessing)
	if err != nil {
		return
	}

	for _, mail := range intr {
		mail.SpoolEmail.Recipients = strings.Split(mail.Recipients, " ")
		mail.SpoolEmail.MXServers = strings.Split(mail.MXServers, " ")
		emails = append(emails, mail.SpoolEmail)
		err = s.AddSpoolSpecificLogEntryTx(tx, mail.MessageId, mail.TransactionId, "retrieved from queue")
		if err != nil {
			return
		}
	}

	return emails, err
}

func (s *sqlite) RequeueEmail(email SpoolEmail) error {
	q := `
		UPDATE spool
		SET send_at = ?,
		    status = ?,
		    updated_at = (strftime('%Y-%m-%d %H:%M:%f', 'now'))
		WHERE transaction_id = ?
	`
	db, err := s.getDB()
	if err != nil {
		return err
	}

	var delay = time.Hour
	switch email.SendCount {
	case 1:
		delay = time.Minute + time.Second*time.Duration(rand.Intn(60)) // between 1-2 min
	case 2:
		delay = 15*time.Minute + time.Second*time.Duration(rand.Intn(180)) // between 15-17 min
	case 3:
		delay = 30*time.Minute + time.Second*time.Duration(rand.Intn(300)) // between 30-35 min
	}
	_, err = db.Exec(q, time.Now().Add(delay), BrevStatusQueued, email.TransactionId)
	return err
}

func (s *sqlite) SetEmailStatus(transactionId int64, statusBrev string) error {
	q := `
		UPDATE spool
		SET status = ?,
		    updated_at = (strftime('%Y-%m-%d %H:%M:%f', 'now'))
		WHERE transaction_id = ?
		RETURNING message_id
	`
	var err error
	var tx *sqlx.Tx
	tx, err = s.getTX()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = tx.Commit()
			return
		}
		_ = tx.Rollback()
	}()

	var messageId string
	err = tx.Get(&messageId, q, statusBrev, transactionId)
	if err != nil {
		return err
	}

	err = s.AddSpoolSpecificLogEntryTx(tx, messageId, transactionId, "spool brev status set to "+statusBrev)

	return nil
}

func (s *sqlite) AddLogEntry(messageId, log string) error {
	return s.AddSpecificLogEntry(messageId, 0, log)
}

func (s *sqlite) AddSpoolLogEntryTx(tx *sqlx.Tx, messageId, log string) error {
	return s.AddSpoolSpecificLogEntryTx(tx, messageId, 0, log)
}

func (s *sqlite) AddSpecificLogEntry(messageId string, transactionId int64, log string) error {
	tx, err := s.getTX()
	if err != nil {
		return err
	}
	err = s.AddSpoolSpecificLogEntryTx(tx, messageId, transactionId, log)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (s *sqlite) AddSpoolSpecificLogEntryTx(tx *sqlx.Tx, messageId string, transactionId int64, log string) error {

	q := `
	INSERT INTO spool_log (message_id, transaction_id, created_at, log)
	VALUES (?, ?, ?, ?)
	`

	var tid *int64
	if transactionId > 0 {
		tid = &transactionId
	}
	_, err := tx.Exec(q, messageId, tid, time.Now().In(time.UTC), log)
	if err != nil {
		return fmt.Errorf("failed to insert log entry, %v", err)
	}
	return err

}

func (s *sqlite) GetEmail(transactionId int64) (email SpoolEmail, err error) {
	q := `SELECT * FROM spool WHERE transaction_id = ?`
	db, err := s.getDB()
	if err != nil {
		return email, err
	}
	err = db.Get(&email, q, transactionId)
	return email, err
}

func (s *sqlite) GetEmailContent(messageId string) ([]byte, error) {
	q := `SELECT content FROM spool_content WHERE message_id = ?`
	db, err := s.getDB()
	if err != nil {
		return nil, err
	}
	var content string
	err = db.Get(&content, q, messageId)
	return []byte(content), err
}
func (s *sqlite) EnsureApiKey(apiKey string) error {
	q := `INSERT INTO api_key (api_key, domain)
	      VALUES (?, '')
	      ON CONFLICT (api_key) DO NOTHING
	`
	db, err := s.getDB()
	if err != nil {
		return err
	}
	_, err = db.Exec(q, apiKey)
	return err
}

func (s *sqlite) UpsertApiKey(key ApiKey) (ApiKey, error) {
	q := `INSERT INTO api_key (api_key, domain, mx_cname, posthook_url) 
	      VALUES (?, ?, ?, ?)
	      ON CONFLICT (api_key)
	      DO UPDATE SET (domain, mx_cname, posthook_url) = (EXCLUDED.domain, EXCLUDED.mx_cname, EXCLUDED.posthook_url)
	      RETURNING *
	`
	db, err := s.getDB()
	if err != nil {
		return ApiKey{}, err
	}
	var out ApiKey
	err = db.Get(&out, q, key.Key, key.Domain, key.MxCNAME, key.PosthookURL)
	return out, err
}

func (s *sqlite) GetApiKey(key string) (ApiKey, error) {
	// TODO add cache for keys...

	q := `SELECT * FROM api_key WHERE api_key = ?`
	db, err := s.getDB()
	if err != nil {
		return ApiKey{}, err
	}
	var apiKey ApiKey
	err = db.Get(&apiKey, q, key)
	return apiKey, err
}

func (s *sqlite) tuneDatabase() error {
	q := `pragma journal_mode = WAL;
			pragma synchronous = normal;
			pragma temp_store = memory;
			pragma mmap_size = 30000000000;`

	if s.db == nil {
		return errors.New("db must be instantiated")
	}
	_, err := s.db.Exec(q)
	return err
}

func (s *sqlite) getDB() (*sqlx.DB, error) {

	var err error
	// this seems like it could be optimized in general... some other process checking health and reconnecting.
	// Along with some hourly job running `pragma optimize;`
	for s.db == nil || s.db.Ping() != nil {

		if s.db != nil {
			_ = s.db.Close()
			s.db = nil
		}

		fmt.Println("Connecting to db", s.path)
		s.db, err = sqlx.Connect("sqlite3", s.path)
		if err != nil {
			return nil, fmt.Errorf("error while connecting, %w", err)
		}
		err := s.tuneDatabase()
		if err != nil {
			return nil, fmt.Errorf("error while tuning db instence, %w", err)
		}
	}

	return s.db, nil
}
func (s *sqlite) getTX() (*sqlx.Tx, error) {
	db, err := s.getDB()
	if err != nil {
		return nil, err
	}
	return db.Beginx()
}

func (s *sqlite) ensureSchema() error {

	db, err := s.getDB()
	if err != nil {
		return fmt.Errorf("could not get db, %w", err)
	}

	_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS api_key (
	    api_key TEXT PRIMARY KEY,
		domain TEXT NOT NULL,
	    mx_cname TEXT DEFAULT '' NOT NULL ,  
	    posthook_url TEXT DEFAULT '' NOT NULL
	);

	CREATE TABLE IF NOT EXISTS spool (
	    transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
	    api_key TEXT,
	    
	    message_id TEXT,

	    from_ 	   TEXT NOT NULL,
	    mx_servers TEXT NOT NULL,
	    recipients TEXT NOT NULL,
	    
	    status     TEXT, -- queued, processing, sent, failed
	    
	    send_at DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')),
	    send_count INT DEFAULT 0,
	    
		created_at DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')),
		updated_at DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
	);
	CREATE INDEX IF NOT EXISTS idx_spool_send_at ON spool(send_at) WHERE status = 'queued' AND send_count < 3;


	CREATE TABLE IF NOT EXISTS spool_content (
		message_id TEXT PRIMARY KEY NOT NULL,
		content TEXT NOT NULL,
		created_at DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
	);

	CREATE TABLE IF NOT EXISTS spool_log (
	    created_at DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')),
	    message_id TEXT NOT NULL,
	    transaction_id TEXT INTEGER,
	    log TEXT NOT NULL,
	    PRIMARY KEY (message_id, created_at)
	);


	CREATE TABLE IF NOT EXISTS posthook_queue (
	    posthook_id INTEGER PRIMARY KEY AUTOINCREMENT,
	    api_key TEXT NOT NULL,
		posthook_url TEXT NOT NULL,
		content text,
		created_at DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
	);

	
`)
	if err != nil {
		return fmt.Errorf("could upsert schema, %w", err)
	}

	return err
}
