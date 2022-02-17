package dao

import "time"

type BrevStatus string

const BrevStatusQueued = "queued"
const BrevStatusProcessing = "processing"
const BrevStatusSent = "sent"

type SpoolEmail struct {
	MessageId  string `db:"message_id"`
	ApiKey     string `db:"api_key"`
	StatusBrev string `db:"status_brev"`
	StatusSmtp string `db:"status_smtp"`
	From       string `db:"from_"`
	Recipients []string
	SendAt     time.Time `db:"send_at"`
	SendCount  int       `db:"send_count"`
	CreatedAt  time.Time `db:"created_at"`
	UpdatedAt  time.Time `db:"updated_at"`
}

type ApiKey struct {
	Key     string `db:"api_key"`
	Domain  string `db:"domain"`
	MxCNAME string `db:"mx_cname"`
}
