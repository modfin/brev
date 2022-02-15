package dao

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"time"
)

type DAO interface {
	GetApiKey(key string) (*ApiKey, error)
	AddEmailToSpool(email SpoolEmail) error
	GetQueuedEmails(count int) (emails []SpoolEmail, err error)
	ClaimEmail(id string) error
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

func (s *sqlite) ClaimEmail(messageId string) (err error) {
	q := `
		UPDATE spool
		SET status_brev = 'processing'
		WHERE message_id = ?
          AND status_brev = 'queued'
	`

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

	res, err := tx.Exec(q, messageId)
	if err != nil {
		return err
	}

	affected, err := res.RowsAffected()

	if affected != 1 {
		err = fmt.Errorf("could not claim email %s, %d was affected by claim atempt", messageId, affected)
		return
	}

	err = s.AddSpoolLogEntryTx(tx, messageId, "claimed by internal spool")
	return

}

func (s *sqlite) AddSpoolLogEntry(messageId, log string) error {

	tx, err := s.getTX()
	if err != nil {
		return err
	}
	err = s.AddSpoolLogEntryTx(tx, messageId, log)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (s *sqlite) AddSpoolLogEntryTx(tx *sqlx.Tx, messageId, log string) error {
	q := `
	INSERT INTO spool_log (message_id, log)
	VALUES (?,?)
	`
	_, err := tx.Exec(q, messageId, log)
	return err
}

func (s *sqlite) AddEmailToSpool(email SpoolEmail) (err error) {
	q := `
	INSERT INTO spool(message_id, api_key, status_brev, content, send_at)
	VALUES (:message_id, :api_key, :status_brev, :content, :send_at) 
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

	stmt, err := tx.PrepareNamed(q)
	if err != nil {
		return
	}
	defer stmt.Close()

	_, err = stmt.Exec(map[string]interface{}{
		"message_id":  email.MessageId,
		"api_key":     email.ApiKey,
		"status_brev": email.StatusBrev,
		"content":     string(email.Content),
		"send_at":     email.SendAt.In(time.UTC),
	})

	if err != nil {
		return
	}

	err = s.AddSpoolLogEntryTx(tx, email.MessageId, "spool entry has been added")

	return
}

func (s *sqlite) GetQueuedEmails(count int) (emails []SpoolEmail, err error) {
	q1 := `
	    SELECT *
		FROM spool
		WHERE send_at < CURRENT_TIMESTAMP
		  AND status_brev = 'queued'
		ORDER BY send_at
		LIMIT ?
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

	err = tx.Select(&emails, q1, count)
	if err != nil {
		return
	}

	for _, mail := range emails {
		err = s.AddSpoolLogEntryTx(tx, mail.MessageId, "retrieved from queue")
		if err != nil {
			return
		}
	}

	return emails, err
}

func (s *sqlite) GetApiKey(key string) (*ApiKey, error) {
	q := `SELECT * FROM api_key WHERE api_key = ?`
	db, err := s.getDB()
	if err != nil {
		return nil, err
	}
	var apiKey ApiKey
	err = db.Get(&apiKey, q, key)
	return &apiKey, err
}

func (s *sqlite) getDB() (*sqlx.DB, error) {

	var err error
	if s.db == nil {
		fmt.Println("connecting to db", s.path)
		s.db, err = sqlx.Connect("sqlite3", s.path)
	}
	if err != nil {
		return nil, fmt.Errorf("error while connecting, %w", err)
	}
	// TODO do something to verify connection and reconnect... s.db.Ping / reconnect...
	return s.db, err
}
func (s *sqlite) getTX() (*sqlx.Tx, error) {

	var err error
	if s.db == nil {
		s.db, err = sqlx.Connect("sqlite3", s.path)
	}
	if err != nil {
		return nil, fmt.Errorf("error while connecting, %w", err)
	}
	// TODO do something to verify connection and reconnect... s.db.Ping / reconnect...
	return s.db.Beginx()
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
	    mx_cname TEXT DEFAULT ''
	);

	CREATE TABLE IF NOT EXISTS spool (
	    message_id TEXT PRIMARY KEY,
	    api_key TEXT,
	    
	    status_brev TEXT, -- queued, processing, sent
	    status_smtp TEXT DEFAULT '', -- 250, 4xx, 5xx

	    content TEXT,
	    
	    send_at DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')),
	    send_count INT DEFAULT 0,
	    
		created_at DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')),
		updated_at DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
	);
	CREATE INDEX IF NOT EXISTS idx_spool_send_at ON spool(send_at) WHERE status_brev = 'queued' AND send_count < 3;

	CREATE TABLE IF NOT EXISTS spool_log (
	    message_id TEXT NOT NULL,
	    created_at DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')),
	    log TEXT NOT NULL,
	    PRIMARY KEY (message_id, created_at)
	);

	
`)
	if err != nil {
		return fmt.Errorf("could upsert schema, %w", err)
	}

	return err
}
