package dao

import "time"

type BrevStatus string

const BrevStatusQueued = "queued"
const BrevStatusProcessing = "processing"
const BrevStatusSent = "sent"
const BrevStatusFailed = "failed"

type SpoolEmail struct {
	TransactionId int64  `db:"transaction_id"`
	MessageId     string `db:"message_id"`
	ApiKey        string `db:"api_key"`
	Status        string `db:"status"`
	From          string `db:"from_"`
	MXServers     []string
	Recipients    []string
	SendAt        time.Time `db:"send_at"`
	SendCount     int       `db:"send_count"`
	CreatedAt     time.Time `db:"created_at"`
	UpdatedAt     time.Time `db:"updated_at"`
}

type ApiKey struct {
	Key     string `db:"api_key"`
	Domain  string `db:"domain"`
	MxCNAME string `db:"mx_cname"`
}
