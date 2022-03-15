package dao

import (
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"strings"
	"time"
)

type DAO interface {
	UpsertApiKey(*ApiKey) error
	GetApiKey(key string) (*ApiKey, error)
	GetQueuedEmails(count int) (emails []SpoolEmail, err error)
	ClaimEmail(transactionId int64) error
	UpdateEmailBrevStatus(transactionId int64, statusBrev string) error
	UpdateSendCount(email SpoolEmail) error

	GetEmailContent(messageId string) ([]byte, error)
	AddEmailToSpool(spoolmails []SpoolEmail, content []byte) ([]int64, error)
	RescheduleEmailToSpool(spoolmail SpoolEmail) error

	AddSpoolSpecificLogEntry(messageId string, transactionId int64, log string) error
	AddSpoolLogEntry(messageId string, log string) error
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

func (s *sqlite) RescheduleEmailToSpool(email SpoolEmail) error {
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
		delay = time.Minute
	case 2:
		delay = 15 * time.Minute
	case 3:
		delay = 30 * time.Minute
	}
	_, err = db.Exec(q, time.Now().Add(delay), BrevStatusQueued, email.TransactionId)
	return err
}

func (s *sqlite) UpdateSendCount(email SpoolEmail) error {
	q := `
		UPDATE spool
		SET send_count = ?,
		    updated_at = (strftime('%Y-%m-%d %H:%M:%f', 'now'))
		WHERE transaction_id = ?
	`
	db, err := s.getDB()
	if err != nil {
		return err
	}
	_, err = db.Exec(q, email.SendCount, email.TransactionId)
	return err

}

func (s *sqlite) AddEmailToSpool(transfers []SpoolEmail, content []byte) (transactionIds []int64, err error) {

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

		transactionIds = append(transactionIds, transactionId)
		err = s.AddSpoolSpecificLogEntryTx(tx, messageId, transactionId, "transfer list has been added to spool")
		if err != nil {
			err = fmt.Errorf("failed to insert log entry, err %v", err)
			return
		}
	}

	return
}

func (s *sqlite) UpdateEmailBrevStatus(transactionId int64, statusBrev string) error {
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

func (s *sqlite) ClaimEmail(transactionId int64) (err error) {
	q := `
		UPDATE spool
		SET status = ?,
		    updated_at = (strftime('%Y-%m-%d %H:%M:%f', 'now'))
		WHERE transaction_id = ?
          AND status = ?
		RETURNING message_id 
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

	var messageIds []string
	err = tx.Select(&messageIds, q, BrevStatusProcessing, transactionId, BrevStatusQueued)
	if err != nil {
		return err
	}

	if len(messageIds) != 1 {
		err = fmt.Errorf("could not claim email transaction id %s, %d was affected by claim atempt", transactionId, len(messageIds))
		return
	}

	messageId := messageIds[0]

	err = s.AddSpoolSpecificLogEntryTx(tx, messageId, transactionId, "claimed by internal spool")
	return

}

func (s *sqlite) AddSpoolLogEntry(messageId, log string) error {
	return s.AddSpoolSpecificLogEntry(messageId, 0, log)
}

func (s *sqlite) AddSpoolLogEntryTx(tx *sqlx.Tx, messageId, log string) error {
	return s.AddSpoolSpecificLogEntryTx(tx, messageId, 0, log)
}

func (s *sqlite) AddSpoolSpecificLogEntry(messageId string, transactionId int64, log string) error {
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

func (s *sqlite) GetQueuedEmails(count int) (emails []SpoolEmail, err error) {
	q1 := `
	    SELECT *
		FROM spool
		WHERE send_at <= ?
		  AND status = ?
	      AND send_count < 3
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

	var intr []struct {
		SpoolEmail
		Recipients string `db:"recipients"`
		MXServers  string `db:"mx_servers"`
	}

	err = tx.Select(&intr, q1, time.Now().In(time.UTC), BrevStatusQueued, count)
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

func (s *sqlite) UpsertApiKey(key *ApiKey) error {
	q := `INSERT INTO api_key (api_key, domain, mx_cname) 
		  VALUES (?, ?, ?)`
	db, err := s.getDB()
	if err != nil {
		return err
	}
	_, err = db.Exec(q, key.Key, key.Domain, key.MxCNAME)
	return err
}

func (s *sqlite) GetApiKey(key string) (*ApiKey, error) {
	// TODO add cache for keys...

	q := `SELECT * FROM api_key WHERE api_key = ?`
	db, err := s.getDB()
	if err != nil {
		return nil, err
	}
	var apiKey ApiKey
	err = db.Get(&apiKey, q, key)
	return &apiKey, err
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
	    mx_cname TEXT DEFAULT '',
	    
	    posthook_url TEXT
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

	
`)
	if err != nil {
		return fmt.Errorf("could upsert schema, %w", err)
	}

	return err
}
